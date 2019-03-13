import psycopg2

from utils.logger import Logger


class Postgresql:

    def __init__(self, conf):
        self.connect = psycopg2.connect(
            user=conf['user'],
            password=conf['password'],
            host=conf['host'],
            port=conf['port'],
            database=conf['database'],
            encode=conf['encode']
        )
        self.logger = Logger('postgresql')

    def find(self, sql, values=()):
        """
        sql like this:
        select * from mobile where id = %s limit0,5
        """
        logger.record("find: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            result = cursor.execute(sql, values)
            logger.record("find result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in find operation", error)
            logger.record("Error in find operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                logger.record("PostgreSQL connection is closed")

    def update(self, sql, values=()):
        """
        sql like this:
        Update mobile set price = %s where id = %s
        """
        logger.record("update: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            result = cursor.execute(sql, values)
            logger.record("updated result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in update operation", error)
            logger.record("Error in update operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                logger.record("PostgreSQL connection is closed")

    def delete(self, sql, values=()):
        """
        sql like this:
        Delete from mobile where id = %s
        """
        logger.record("delete: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            result = cursor.execute(sql, values)
            logger.record("updated result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in delete operation", error)
            logger.record("Error in delete operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                logger.record("PostgreSQL connection is closed")

    def insert_batch(self, sql, values=()):
        """
        sql like this:
        INSERT INTO mobile (id, model, price) VALUES (%s,%s,%s), (%s,%s,%s)

        values like this:
        [(4,'LG', 800), (5,'One Plus 6', 950)]
        """
        logger.record("insert batch: \nsql -> {}\nvalues -> {}".format(sql, values))
        try:
            cursor = self.connect.cursor()
            # executemany() to insert multiple rows rows
            result = cursor.executemany(sql_insert_query, records)
            logger.record("insert batch result: {}".format(result))
            self.connect.commit()
        except (Exception, psycopg2.Error) as error:
            print("Error in insert batch operation", error)
            logger.record("Error in insert batch operation: {}".format(error))
        finally:
            # closing database connection.
            if (self.connect):
                cursor.close()
                self.connect.close()
                logger.record("PostgreSQL connection is closed")


if __name__ == '__main__':
    result = Postgresql().find()
    print(result)
