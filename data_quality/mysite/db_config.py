import pymysql
from sqlalchemy import create_engine

from mysite.settings import MYSQL_HOST, MYSQL_PORT, CONN_USER, CONN_PASSWORD, DATABASE

mysql_host    = MYSQL_HOST
mysql_port    = MYSQL_PORT
conn_user     = CONN_USER
conn_password = CONN_PASSWORD
database      = DATABASE
conn_charset  = 'utf8mb4'
socket        = '/var/lib/mysql/mysql.sock'

def mysql_connect():
    # MySQLdb
    conn = pymysql.connect(host=mysql_host,
                           port=mysql_port,
                           user=conn_user,
                           passwd=conn_password,
                           db=database,
                           charset=conn_charset,
                           # unix_socket=socket,
                           use_unicode=True)
    return conn


def sqlalchemy_conn():
    # &unix_socket={socket},module 'socket' has no attribute 'AF_UNIX'
    engine = create_engine(
        f'mysql+mysqldb://{conn_user}:{conn_password}@{mysql_host}/{database}?charset={conn_charset}',
        echo=False,                     # 打印sql语句
        max_overflow=0,                 # 超过连接池大小外最多创建的连接
        pool_size=5,                    # 连接池大小
        pool_timeout=30,                # 池中没有线程最多等待的时间，否则报错
        pool_recycle=-1,                # 多久之后对线程池中的线程进行一次连接的回收（重置）
    )
    return engine

#test
if __name__ == '__main__':
    conn = mysql_connect()
    print('success')