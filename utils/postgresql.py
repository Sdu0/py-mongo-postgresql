import os
import sys
import psycopg2
sys.path.append(os.getcwd())

from utils.logger import Logger


class Postgresql:

    def __init__(self, conf):
        self.connect = psycopg2.connect(
            user=conf['user'],
            password=conf['password'],
            host=conf['host'],
            port=conf['port'],
            database=conf['database'],
        )
        self.connect.set_client_encoding(conf['encode']);
        self.logger = Logger('postgresql')

    def find(self, sql, values=()):
        """
        sql like this:
        select * from mobile where id = %s limit0,5
        """
        self.logger.record("find: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            cursor.execute(sql, values)
            return cursor.fetchall()
            self.logger.record("find result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in find operation", error)
            self.logger.record("Error in find operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                self.logger.record("PostgreSQL connection is closed")

    def find_one(self, sql, values=()):
        """
        sql like this:
        select * from mobile where id = %s limit0,5
        """
        self.logger.record("find: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            cursor.execute(sql, values)
            return cursor.fetchone()
            self.logger.record("find result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in find operation", error)
            self.logger.record("Error in find operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                self.logger.record("PostgreSQL connection is closed")

    def update(self, sql, values=()):
        """
        sql like this:
        Update mobile set price = %s where id = %s
        """
        self.logger.record("update: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            result = cursor.execute(sql, values)
            self.logger.record("updated result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in update operation", error)
            self.logger.record("Error in update operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                self.logger.record("PostgreSQL connection is closed")

    def delete(self, sql, values=()):
        """
        sql like this:
        Delete from mobile where id = %s
        """
        self.logger.record("delete: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            result = cursor.execute(sql, values)
            self.logger.record("updated result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in delete operation", error)
            self.logger.record("Error in delete operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                self.logger.record("PostgreSQL connection is closed")

    def insert_batch(self, sql, values=()):
        """
        sql like this:
        INSERT INTO mobile (id, model, price) VALUES (%s,%s,%s), (%s,%s,%s)

        values like this:
        [(4,'LG', 800), (5,'One Plus 6', 950)]
        """
        self.logger.record("insert batch: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            # executemany() to insert multiple rows rows
            result = cursor.executemany(sql_insert_query, records)
            self.logger.record("insert batch result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in insert batch operation", error)
            self.logger.record("Error in insert batch operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                self.logger.record("PostgreSQL connection is closed")


if __name__ == '__main__':
    conf = {
        'user': 'root',
        'password': 'abc@123',
        'host': '106.14.94.38',
        'port': '5432',
        'database': 'saas_dq_bi_test',
        'encode': 'utf8',
    }
    sql = 'select date, channel from store_channel_sales_detail limit 10 offset 0'
    # result = Postgresql(conf).find(sql)
    result = Postgresql(conf).find_one(sql)
    print(result)
