# -*- encoding: utf-8 -*-
"""
@File    :   get_db_sys_breakdown.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/26 14:27   parker      1.0         获取数据库中的系统故障信息
"""
from monitor.db.rdbms import MyRDBMS
from monitor import settings


class DBProjectMSGSettings:
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

    def get_project_msg_settings(self) -> dict:
        """
        获取项目配置信息
        :return:
        """
        db_back: dict = {}
        # 在扫描时间段内，只取最新一个同类错误
        sql = "SELECT * FROM project_msg_settings WHERE use_status = 1"
        for _line in self.db.execute(sql):
            _line_dict = dict(zip(_line.keys(), _line))
            db_back[_line_dict.get("project_code")] = _line_dict
        return db_back
