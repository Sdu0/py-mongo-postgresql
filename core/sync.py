import re
import os
import sys
sys.path.append(os.getcwd())

from time import sleep
from pymongo import MongoClient, ASCENDING
from pymongo.cursor import CursorType
from pymongo.errors import AutoReconnect
from bson.timestamp import Timestamp

from process import (read_config, write_config, read_mapping,
                     format_data, get_sql, )
from utils.logger import Logger
from utils.mongo import Mongo
from utils.postgresql import Postgresql

# Time to wait for data or connection.
_SLEEP = 1


class Sync:

    def __init__(self):
        # inital config
        config = read_config()
        self.mongo = config['mongo']
        self.oplog = config['oplog']
        self.postgresql = config['postgresql']

        # inital logging
        self.logger = Logger('sync')

    # 基于 SQL 语句的全量同步
    def _full_sql(self):
        self.logger.record('Starting：based full sql...')

        for table in self.mongo['tables']:

            # sync
            self.mongo['table'] = table
            client = Mongo(self.mongo)
            total = client.count()
            offset = 0
            limit = 100
            while offset <= total:
                # record offset and limit
                self.logger.record('offset:{}, limit:{}'.format(offset, limit))
                queryset = client.find(offset=offset, limit=limit)
                actions = []
                action_ids = []
                for q in queryset:
                    data = format_data(table, q)
                    action_ids.append(str(q['_id']))
                    actions.append(data)

                # pg save for batch
                sql = get_sql(table)
                Postgresql(self.postgresql).insert_batch(sql, actions)

                sleep(_SLEEP)
                offset += limit

        self.logger.record('Ending：based full sql.')

    # 基于 oplog 日志的增量同步
    def _inc_oplog(self):
        self.logger.record('Starting：based increase oplog...')

        # sync
        oplog = Mongo(self.mongo).client().local.oplog.rs
        # 获取偏移量
        stamp = oplog.find().sort('$natural',ASCENDING).limit(-1).next()['ts'] if not self.oplog['ts'] else self.oplog['ts']

        while True:
            kw = {}

            kw['filter'] = {'ts': {'$gt': eval(stamp)}}
            kw['cursor_type'] = CursorType.TAILABLE_AWAIT
            kw['oplog_replay'] = True

            cursor = oplog.find(**kw)
            try:

                while cursor.alive:
                    for q in cursor:
                        stamp = q['ts']

                        # Do something with doc.
                        op = q['op'] # 操作 u i d
                        db, table = q['ns'].split('.') # 表的变动 saas_dq_uat.pos
                        doc = q['o']
                        
                        # 数据库ID -> 文档ID
                        doc_id = str(doc['_id'])
                        del doc['_id']

                        # format data
                        format_data(doc)
                        format_data_for_aggs(doc)

                        if op is 'u':
                            self.es.update(table, doc_id, doc)
                        elif op is 'i':
                            self.es.insert(table, doc_id, doc)
                        elif op is 'd':
                            self.es.delete(table, doc_id)

                        # 记录增量位置
                        write_config('oplog', 'ts', stamp)
                    sleep(_SLEEP)

            except AutoReconnect:
                sleep(_SLEEP)

        self.logger.record('Ending：based increase oplog.')


if __name__ == '__main__':
    sync = Sync()
    sync._full_sql()
    # sync._inc_oplog()
