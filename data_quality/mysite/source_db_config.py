import MySQLdb
# import pymssql
# import cx_Oracle
import os

# SQL server数据库
# def sqlserver_db():
#     conn = pymssql.connect(host='',
#                            user='',
#                            password='',
#                            database='',
#                            charset='utf8mb4'
#                            )
#     return conn

# Oracle数据库
# def oracle_db():
#     os.environ['NLS_LANG']    = 'AMERICAN_AMERICA.UTF8'
#     os.environ['ORACLE_HOME'] = ''
#     conn = cx_Oracle.connect('')
#     return conn

# MySQL数据库
def mysql_db():
    conn = MySQLdb.connect(host='',
                           port='',
                           user='',
                           passwd='',
                           db='',
                           charset='utf8mb4',
                           use_unicode=True
                           )
    return conn


hive_config = {
    'mapreduce.job.queuename': 'my_hive',
    'hive.exec.compress.output': 'false',
    'hive.exec.compress.intermediate': 'true',
    'mapred.min.split.size.per.node': '1',
    'mapred.min.split.size.per.rack': '1',
    'hive.map.aggr': 'true',
    'hive.groupby.skewindata': 'true'
}

# hive数据库
try:
    from pyhive import hive
except:
    print('import pyhive error')
def hive_db():
    conn = hive.connect(host="003.bg.qkt",port=10000, auth="...", database="tmp",username="zhangzhijun",
                        password="zhangzhijun123456", configuration=hive_config)
    return conn

