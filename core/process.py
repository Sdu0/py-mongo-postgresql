import os
import re
import sys
import datetime
import math
import json
sys.path.append(os.getcwd())

from configparser import (ConfigParser, RawConfigParser, )

from utils.mongo import Mongo
from utils.format import (date_to_str, )


# 读取 config.ini 配置项
def read_config():
    cfg = ConfigParser()
    cfg.read('config/config.ini')

    is_null = lambda x: None if not x else x
    is_list = lambda x: None if not x else eval(x)

    mongo_conf = {
        'host': cfg.get('mongo', 'host'),
        'port': cfg.getint('mongo', 'port'),
        # 'user': cfg.get('mongo', 'user'),
        # 'passwd': cfg.get('mongo', 'passwd'),
        # 'authSource': cfg.get('mongo', 'authSource'),
        # 'authMechanism': cfg.get('mongo', 'authMechanism'),
        'db': cfg.get('mongo', 'db'),
        'tables': is_list(cfg.get('mongo', 'tables')),
    }
    oplog_conf = {
        'ts': is_null(cfg.get('oplog', 'ts')),
    }
    pg_conf = {
        'user': cfg.get('postgresql', 'user'),
        'password': cfg.get('postgresql', 'password'),
        'host': cfg.get('postgresql', 'host'),
        'port': cfg.get('postgresql', 'port'),
        'database': cfg.get('postgresql', 'database'),
        'encode': cfg.get('postgresql', 'encode'),
    }

    return {'mongo': mongo_conf, 'oplog': oplog_conf, 'postgresql': pg_conf}


# 写入 config.ini 配置项
def write_config(section, key, value):
    cfg = RawConfigParser()
    cfg.read('config/config.ini')
    if section not in cfg.sections():
        cfg.add_section(section)

    cfg.set(section, key, value)

    with open('config/config.ini', 'w') as f:
        cfg.write(f)


# 读取 mapping 文件
def read_mapping(name):
    try:
        with open('mapping/{}.json'.format(name), 'r') as file:
            return eval(file.read())
    except:
        return {"mappings":{name:{"properties": {}}}}


# 写入 mapping 文件
def write_mapping(name, data):
    with open('mapping/{}.json'.format(name), 'w') as file:
        json.dump(data, file)


# 业务相关操作：聚合 
def format_mapping_for_aggs(key, value, mapping):
    # pos
    if key == 'terminal_open_time' and not 'terminal_open_time_dict' in mapping:
        mapping['terminal_open_time_dict'] = {
            "type": "nested",
            "properties": {"month": {"type": "keyword"},
            "day": {"type": "keyword"},
            "hour": {"type": "keyword"}}
        }
    if key == 'store_branch' and not 'store_branch_dict' in mapping and len(value):
        mapping['store_branch_dict'] = {
            "type": "nested",
            "properties": { i+1: {"type": "keyword"} for i in range(len(value))}
        }
    if key == 'store_geo' and not 'store_geo_dict' in mapping and len(value):
        mapping['store_geo_dict'] = {
            "type": "nested",
            "properties": { i+1: {"type": "keyword"} for i in range(len(value))}
        }

    # pos_store_sales_detail
    if key == 'sales_amount' and not 'sales_amount_list' in mapping:
        mapping['sales_amount_list'] = {
            "type": "keyword"
        }
    if key == 'sales_count' and not 'sales_count_list' in mapping:
        mapping['sales_count_list'] = {
            "type": "keyword"
        }
    if key == 'branch_ids' and not 'branch_dict' in mapping and len(value):
        mapping['branch_dict'] = {
            "type": "nested",
            "properties": { i+1: {"type": "keyword"} for i in range(len(value))}
        }
    if key == 'geo_ids' and not 'geo_dict' in mapping and len(value):
        mapping['geo_dict'] = {
            "type": "nested",
            "properties": { i+1: {"type": "keyword"} for i in range(len(value))}
        }

    # pos_store_sales
    if key == 'branch' and not 'branch' in mapping and len(value):
        mapping['branch_dict'] = {
            "type": "nested",
            "properties": { i: {"type": "keyword"} for i in range(len(value))}
        }

# 生成mapping文件，根据mongo data
def format_mapping(old_mapping, new_data):
    '''
    name: mapping name
    old_mapping: 
    new_data: 
    '''
    if new_data:
        for k, v in new_data.items():
            if not k in old_mapping:
                old_mapping[k] = {"type": "text"}

            # 业务相关操作：聚合 
            format_mapping_for_aggs(k, v, old_mapping)

            if k.endswith(('no', 'code', 'id', 'type')):
                old_mapping[k] = {"type": "keyword"}
            elif k.endswith(('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) and not k in ['1', '2', '3']:
                old_mapping[k] = {"type": "double", "null_value": 0}
            elif type(v) is int and old_mapping[k]['type'] in ['text', 'keyword']:
                old_mapping[k] = {"type": "long"}
            elif type(v) is bool and old_mapping[k]['type'] in ['text', 'keyword']:
                old_mapping[k] = {"type": "boolean"}
            elif type(v) is float and old_mapping[k]['type'] in ['text', 'keyword', 'long']:
                old_mapping[k] = {"type": "double"}
            elif type(v) is datetime.datetime and not old_mapping[k]['type'] is 'nested':
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is str and (re.compile(r'....-..-.. ..:..:..').match(v) or re.compile(r'....-..-..').match(v)):
                old_mapping[k] = {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"}
            elif type(v) is list:
                first = v[0]
                if type(first) is dict:
                    if not k in old_mapping or not old_mapping[k]['type'] is 'nested':
                        old_mapping[k] = {"type": "nested", "properties": {}}
                    format_mapping(old_mapping[k]['properties'], first)
                else:
                    old_mapping[k] = {"type": "text"}
            elif type(v) is dict:
                if not (k in old_mapping and old_mapping[k]['type'] is 'nested'):
                    old_mapping[k] = {"type": "nested", "properties": {}}
                format_mapping(old_mapping[k]['properties'], v)
            

# 递归格式化 mongo data
def format_data(data):
    if data:
        for k, v in data.items():
            if not v and type(v) in [str, list, dict]:
                data[k] = None
            elif type(v) is datetime.datetime:
                # 数据中时间类型转字符串类型
                data[k] = date_to_str(v)
            elif type(v) is float and math.isnan(v):
                data[k] = None
            elif type(v) is str and (v.upper() == 'NOUN' or v.upper() == 'MEAL_DEAL'):
                data[k] = None
            elif type(v) is dict:
                format_data(v)
            elif type(v) is list:
                for i in v:
                    if type(i) is dict:
                        format_data(i)

# 业务相关操作：时段聚合
def format_data_for_aggs(data):
    # pos
    if not data.get('terminal_open_time'):
        data['terminal_open_time_dict'] = None
    else:
        m_d_h = re.compile(r'....-(..)-(..) (..):.*?').findall(data['terminal_open_time'])[0]
        data['terminal_open_time_dict'] = {'month': m_d_h[0], 'day': m_d_h[1], 'hour': m_d_h[2]}
    if not data.get('store_branch'):
        data['store_branch_dict'] = None
    else:
        data['store_branch_dict'] = { i+1: v['id'] for i,v in enumerate(data['store_branch']) if v}
    if not data.get('store_geo'):
        data['store_geo_dict'] = None
    else:
        data['store_geo_dict'] = { i+1: v['id'] for i,v in enumerate(data['store_geo']) if v}

    # pos_store_sales_detail
    if not data.get('branch_ids'):
        data['branch_dict'] = None
    else:
        data['branch_dict'] = { i+1: v for i,v in enumerate(data['branch_ids']) if v}
    if not data.get('geo_ids'):
        data['geo_dict'] = None
    else:
        data['geo_dict'] = { i+1: v for i,v in enumerate(data['geo_ids']) if v}
    if not data.get('sales_count'):
        data['sales_count_list'] = None
    else:
        data['sales_count_list'] = [ k for k in data['sales_count'].keys() ]
    if not data.get('sales_amount'):
        data['sales_amount_list'] = None
    else:
        data['sales_amount_list'] = [ k for k in data['sales_amount'].keys() ]

    # pos_store_sales
    if not data.get('branch'):
        data['branch_dict'] = None
    else:
        data['branch_dict'] = { i: v['id'] for i,v in enumerate(data['branch']) if v}
