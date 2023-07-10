# -*- encoding: utf-8 -*-
"""
@File    :   get_db_sys_breakdown.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/26 14:27   parker      1.0        标记是否创建工单
"""
from monitor.db.rdbms import MyRDBMS
from monitor import settings


class DBSysBreakdownSign:
    """
    获取数据库中的系统故障数据
    """
    db: MyRDBMS

    def __init__(self, db=None):
        """
        初始化
        """
        if db:
            self.db = db
        else:
            self.db = MyRDBMS(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                db=settings.DB_DATABASE
            )

    def sign_work_order_status(self, sys_breakdown_id, work_order_code="NULL"):
        """
        标记工单
        :param sys_breakdown_id: 系统故障id
        :param work_order_code: 工单id
        :return:
        """
        sql = """
        update sys_breakdown 
        SET work_order_code = '{work_order_code}', work_order_status = {work_order_status} 
        WHERE id = {sys_breakdown_id}
        """.format(
            work_order_code=work_order_code,
            work_order_status=1,
            sys_breakdown_id=sys_breakdown_id,
        )
        self.db.execute(sql)

    def sign_dingtalk_status(self, sys_breakdown_id):
        """
        标记钉钉
        :param sys_breakdown_id: 系统故障id
        :return:
        """
        sql = "update sys_breakdown  SET dingtalk_status = {dingtalk_status}  WHERE id = {sys_breakdown_id}".format(
            dingtalk_status=1,
            sys_breakdown_id=sys_breakdown_id,
        )
        self.db.execute(sql)
