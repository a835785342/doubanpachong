from DBUtils.PooledDB import PooledDB
import db_config as config
from sshtunnel import SSHTunnelForwarder

"""
@功能：创建数据库连接池
"""


class MyConnectionPool(object):
    __pool = None

    # def __init__(self):
    #     self.conn = self.__getConn()
    #     self.cursor = self.conn.cursor()

    # 创建数据库连接conn和游标cursor
    def __init__(self):
        print('初始化dbutil')
        self.server = SSHTunnelForwarder(
            (config.DB_TEST_HOST, 22),
            ssh_password=config.SERVER_TEST_PASSWORD,
            ssh_username=config.SERVER_TEST_USER,
            remote_bind_address=(config.DB_TEST_HOST, 3306))
        self.conn = self.__getconn(self.server)
        self.cursor = self.conn.cursor()

    # 创建数据库连接池
    def __getconn(self,server):
        if self.__pool is None:
            server.start()
            self.__pool = PooledDB(
                creator=config.DB_CREATOR,
                mincached=config.DB_MIN_CACHED,
                maxcached=config.DB_MAX_CACHED,
                maxshared=config.DB_MAX_SHARED,
                maxconnections=config.DB_MAX_CONNECYIONS,
                blocking=config.DB_BLOCKING,
                maxusage=config.DB_MAX_USAGE,
                setsession=config.DB_SET_SESSION,
                host='127.0.0.1',
                port=server.local_bind_port,
                user=config.DB_TEST_USER,
                passwd=config.DB_TEST_PASSWORD,
                db=config.DB_TEST_DBNAME,
                use_unicode=False,
                charset=config.DB_CHARSET
            )

        return self.__pool.connection()

    # 从连接池中取出一个连接
    def getconn(self):
        conn = self.__getconn(self.server)
        cursor = conn.cursor()
        return cursor, conn

    def getserver(self):
        return self.server

    # 释放连接池资源
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()
        self.server.close()


# 关闭连接归还给链接池
# def close(self):
#     self.cursor.close()
#     self.conn.close()




# 获取连接池,实例化
def get_my_connection():
    return MyConnectionPool()
