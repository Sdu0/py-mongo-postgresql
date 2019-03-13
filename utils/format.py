import hashlib
import datetime as dt
from datetime import date, datetime, timedelta


def str_to_md5str(my_str):
    hash_md5 = hashlib.md5(my_str.encode('utf-8'))
    return hash_md5.hexdigest()


def date_to_str(dt, sub='second'):
    pattern = {
        'year': '%Y',
        'month': '%Y-%m',
        'day': '%Y-%m-%d',
        'hour': '%Y-%m-%d %H',
        'minute': '%Y-%m-%d %H:%M',
        'second': '%Y-%m-%d %H:%M:%S',
    }
    return dt.strftime(pattern[sub])


def str_to_date(my_str, sub='second'):
    pattern = {
        'year': '%Y',
        'month': '%Y-%m',
        'day': '%Y-%m-%d',
        'hour': '%Y-%m-%d %H',
        'minute': '%Y-%m-%d %H:%M',
        'second': '%Y-%m-%d %H:%M:%S',
    }
    return datetime.strptime(dt, pattern[sub])


def date_format(my_str):
    return my_str.split(' ')[0]


#使用递归格式化深层次字典中的时间类型的值
def datetime_to_string(data):
    for k, v in data.items():
        if type(v) is dt.datetime:
            data[k] = date_to_str(v)
        elif type(v) is dict:
            datetime_to_string(v)
