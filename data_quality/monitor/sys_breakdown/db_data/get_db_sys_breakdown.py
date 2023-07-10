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


class DBSysBreakdown:
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

    def get_sys_breakdown(self) -> dict:
        """
        返回系统错误故障数据,按照项目合并成一条
        :return:
        """
        db_back: dict = {}
        # 在扫描时间段内，只取最新一个同类错误
        sql = open(settings.BASE_DIR.joinpath("monitor/sys_breakdown/sql/get_db_sys_breakdown.sql")).read()
        for _line in self.db.execute(sql):
            _line_dict = dict(zip(_line.keys(), _line))
            _rk = "{project_code}".format(**_line_dict)
            _value = db_back.get(_rk, [])
            _value.append(_line_dict)
            db_back[_rk] = _value
        return db_back

    def get_sys_breakdown_dingtalk(self) -> dict:
        """
        返回系统错误故障是否发送钉钉,按照项目合并成一条
        :return:
        """
        db_back: dict = {}
        # 在扫描时间段内，只取最新一个同类错误
        sql = open(settings.BASE_DIR.joinpath("monitor/sys_breakdown/sql/get_db_sys_breakdown_dingtalk.sql")).read()
        for _line in self.db.execute(sql):
            _line_dict = dict(zip(_line.keys(), _line))
            _rk = "{project_code}##{item_status}".format(**_line_dict)
            _value = db_back.get(_rk, [])
            _value.append(_line_dict)
            db_back[_rk] = _value
        return db_back

    def get_resolved_sys_breakdown(self) -> dict:
        """
        返回系统错误故障恢复数据
        :return:
        """
        db_back: dict = {}
        # 在扫描时间段内，只取最新一个同类错误
        sql = open(settings.BASE_DIR.joinpath("monitor/sys_breakdown/sql/get_db_sys_breakdown_resolved.sql")).read()
        for _line in self.db.execute(sql):
            _line_dict = dict(zip(_line.keys(), _line))
            db_back[_line_dict.get("pk_md5")] = _line_dict
        return db_back
