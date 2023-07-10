
from pyhive import hive
from TCLIService.ttypes import TOperationState

conn = hive.Connection(host='003.bg.qkt', port=10000, username='zhangzhijun', database='tmp')
cursor = conn.cursor()
cursor.execute('show tables')


hive_config = {
    'mapreduce.job.queuename': 'my_hive',
    'hive.exec.compress.output': 'false',
    'hive.exec.compress.intermediate': 'true',
    'mapred.min.split.size.per.node': '1',
    'mapred.min.split.size.per.rack': '1',
    'hive.map.aggr': 'true',
    'hive.groupby.skewindata': 'true'
}



conn = hive.connect(host="003.bg.qkt",port=10000, auth="...", database="tmp",username="zhangzhijun",
                    password="zhangzhijun123456", configuration=hive_config)




def test_hive():
    cursor = hive.connect('003.bg.qkt').cursor()
    cursor.execute('SELECT * FROM tmp LIMIT 10')

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        for message in logs:
            print(message)

        # If needed, an asynchronous query can be cancelled at any time with:
        # cursor.cancel()

        status = cursor.poll().operationState

    print(cursor.fetchall())

if __name__ == "__main__":
    test_hive()