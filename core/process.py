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


# 业务相关操作：格式化数据
def format_data(table, data):
    # 数据库ID -> str id
    str_id = str(data['_id'])
    if table == 'pos_store_sales_detail':
        sales_date = date_to_str(data['sales_date'], sub='day')
        store_id = int(data['store_id'])
        amount = float(data['amount'])
        count = int(data['count'])
        sales_amount = str(data['sales_amount']).replace("'", '"')
        sales_count = str(data['sales_count']).replace("'", '"')
        return (str_id, sales_date, store_id, amount, count, sales_amount, sales_count)
    elif table == 'pos_store_sales':
        day = data['day']
        month = data['month']
        year = data['year']
        sales_date = date_to_str(data['sales_date'], sub='day')
        store_id = data['store_id']
        people = data['people']
        sum_discount = data['sum_discount']
        cancel_order_discount = data['cancel_order_discount']
        pre_sell_amount = data['pre_sell_amount']
        avg_item_count = data['avg_item_count']
        all_order_net_amount = data['all_order_net_amount']
        hex_sum_amount = data['hex_sum_amount']
        sum_master_discount_amount = data['sum_master_discount_amount']
        avg_order_amount = data['avg_order_amount']
        hex_sum_dis_amount = data['hex_sum_dis_amount']
        sum_amount = data['sum_amount']
        order_count = data['order_count']
        cancel_order_count = data['cancel_order_count']
        status = data['status']
        sum_net_amount = data['sum_net_amount']
        all_order_discount = data['all_order_discount']
        cancel_order_amount = data['cancel_order_amount']
        es_amount = data['es_amount']
        sum_item_count = data['sum_item_count']
        all_order_amount = data['all_order_amount']
        all_order_count = data['all_order_count']
        all_cup_sum_item_count = data['all_cup_sum_item_count']
        all_cup_sum_net_amount = data['all_cup_sum_net_amount']
        created = date_to_str(data['created']) 
        return (str_id, day, month, year, sales_date, store_id, people, sum_discount, cancel_order_discount, pre_sell_amount, avg_item_count, all_order_net_amount, hex_sum_amount, sum_master_discount_amount, avg_order_amount, hex_sum_dis_amount, sum_amount, order_count, cancel_order_count, status, sum_net_amount, all_order_discount, cancel_order_amount, es_amount, sum_item_count, all_order_amount, all_order_count, all_cup_sum_item_count, all_cup_sum_net_amount, created)


# 业务相关操作：获取sql语句
def get_sql(table):
    if table == 'pos_store_sales_detail':
        return '''
            INSERT INTO store_hours(id, sales_date, store_id, amount, count, sales_amount, sales_count) 
            VALUES(%s, %s, %s, %s, %s, %s, %s)
        '''
    elif table == 'pos_store_sales':
        return '''
            INSERT INTO store_sales(id, day, month, year, sales_date, store_id, people, sum_discount, cancel_order_discount, pre_sell_amount, avg_item_count, all_order_net_amount, hex_sum_amount, sum_master_discount_amount, avg_order_amount, hex_sum_dis_amount, sum_amount, order_count, cancel_order_count, status, sum_net_amount, all_order_discount, cancel_order_amount, es_amount, sum_item_count, all_order_amount, all_order_count, all_cup_sum_item_count, all_cup_sum_net_amount, created)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
