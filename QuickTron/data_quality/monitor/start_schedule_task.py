# -*- encoding: utf-8 -*-
"""
@File    :   start_task.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 13:46   parker      1.0         整个程序主要是用来做定时
"""
from pathlib import Path
import sys

sys.path.append("{}".format(Path(__file__).resolve().parent.parent))

from monitor.sys_breakdown.start_flow import start_sys_breakdown_to_work_order, start_sys_breakdown_to_dingtalk
from apscheduler.schedulers.blocking import BlockingScheduler

from monitor import settings
from monitor.db.rdbms import MyRDBMS

db = MyRDBMS(
    host=settings.DB_HOST,
    port=settings.DB_PORT,
    user=settings.DB_USER,
    password=settings.DB_PASSWORD,
    db=settings.DB_DATABASE
)

task_schedule_conf = [
    # 创建工单任务
    {
        "func": start_sys_breakdown_to_work_order,
        "kwargs": {"db": db},
        "trigger": "interval",
        "seconds": 900,  # 15分钟一次
        "id": "start_sys_breakdown_to_work_order"
    },

    # 发送钉钉
    {
        "func": start_sys_breakdown_to_dingtalk,
        "kwargs": {"db": db},
        "trigger": "interval",
        "seconds": 60,  # 一分钟一次
        "id": "start_sys_breakdown_to_dingtalk"
    }
]


def start_breakdown_monitor():
    """
    开始系统错误数据的监控
    :return:
    """
    schedule = BlockingScheduler()
    for task_item in task_schedule_conf:
        if not isinstance(task_item, (dict,)):
            continue
        schedule.add_job(**task_item)
    schedule.start()


if __name__ == '__main__':
    start_breakdown_monitor()
