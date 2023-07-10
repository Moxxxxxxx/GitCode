import logging
import time


DATEFORMATSTR = '%Y-%m-%d %H:%M:%S'
logger = logging.getLogger('')

#return timestr2<timestr1 result:-1 timestr2=timestr1  result= 0  > 1 error -2
def compare_time(timestr1, timestr2):
    try:
        s_time1 = time.mktime(time.strptime(str(timestr1), DATEFORMATSTR))
        s_time2 = time.mktime(time.strptime(str(timestr2), DATEFORMATSTR))
        #日期转化为int比较
        diff = int(s_time2)-int(s_time1)
        print(diff)
        if diff > 0:
            return 1
        if diff == 0:
            return 0
        else:
            return -1
    except Exception as e:
        logger.error(e)
        logger.error(timestr1)
        logger.error(timestr2)
        return -2

if __name__=='__main__':
    print(compare_time("2020-06-16 20:08:00" , '2020-06-16 20:08:01'))