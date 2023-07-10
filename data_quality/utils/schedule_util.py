import datetime
import logging
import threading
import time

import schedule

from check.autocheck import Check
from mysite import db_config

logger = logging.getLogger('')

def abnormal_notice():
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print("start excuete abnormal_notice schedule task", ts)
    Check.abnormal_notice()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print('end excute  abnormal_notice :',ts)

def run_check():
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    check = Check()
    print('crate excute schedule run_check task,the  time：',ts)
    check.run_allcheck("automationer")
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    print('start excute schedule run_check task,the  time：',ts)

def tasklist():
    #清空任务
    schedule.clear()
    #start_schedule_task.start_breakdown_monitor()
    #schedule.every(60).seconds.do(sys_breakdown_to_handle)
    #创建一个按120秒间隔执行任务.先注释掉，执行check操作时候，直接通知
    #schedule.every(120).seconds.do(abnormal_notice)
    #创建一个按2分钟秒间隔执行任务
    schedule.every(300).seconds.do(run_check)

    #执行10S
    f = open("schedule.conf", 'r', encoding='utf-8')
    while True:
        switchs = f.readlines()
        switch = ''
        for switch in switchs:
            if switch[0] == '#':
                continue
        if switch:
            is_switch = switch.split('=')[1].strip()
        f.seek(0)
        if int(is_switch) < 1:
            print("schedule task end, will exit")
            break
        logger.info('excute schedule')
        schedule.run_pending()
        time.sleep(10)

#不传参数，将直接启动
def start_scheduletask(switch=None):
    if switch: #写到定时器配置
        f = open("schedule.conf", 'w+', encoding='utf-8')
        f.write('switch=' + str(switch))
        f.close()
    if int(switch) > 0 or not switch: #如果定时器switch >0则调用定时任务
        print('start schedule task!!!')
        logger.info('start schedule task!!!')
        schedule_thread = threading.Thread(target=tasklist)
        schedule_thread.start()
        try:
            conn = db_config.mysql_connect()
            with conn.cursor() as curs:
                sql = f"insert into check_execute_log(productname, check_item, execute_date, execute_user, status, execute_result_info) " \
                      f"values('schedule_switch','schedule_switch is on',now(),'automationer','success'," \
                      f"'excute schedule task include[run_check规则校验], abnormal_notice 异常通知')"
                curs.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error('start schedule fail!!!')
            logger.error(e)
        finally:
            conn.close()
    else:
        try:
            schedule.clear()
        except:
            logger.info('schedule.clear() fail!!!')
        logger.info('end schedule task!!!')

#test
if __name__ == '__main__':
    tasklist()