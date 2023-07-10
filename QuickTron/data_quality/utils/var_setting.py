#变量定义
import datetime

#获取动态的时间差
def get_prev_10m_datetime():
    return str(datetime.datetime.now()-datetime.timedelta(minutes=25))[:15]

def get_testtime():
    return  datetime.datetime.now()

testtime = datetime.datetime.now()