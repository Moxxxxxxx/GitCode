# -*- encoding: utf-8 -*-
"""
@File    :   start_flow.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 14:44   parker      1.0         None
"""
import copy

from monitor.settings import Logger
from monitor.sys_breakdown.db_data.gen_db_sys_breakdown_sign import DBSysBreakdownSign
from monitor.sys_breakdown.db_data.get_db_project_msg_settings import DBProjectMSGSettings
from monitor.sys_breakdown.db_data.get_db_sys_breakdown import DBSysBreakdown
from monitor.sys_breakdown.send_dingtalk import send_dingtalk
from monitor.sys_breakdown.work_order.gen_work_order import gen_work_order
from monitor.sys_breakdown.work_order.get_token import token
from monitor.sys_breakdown.work_order.get_work_order import get_exists_work_order


def create_work_order(
        authorization: str,
        current_project_settings: dict,
        sys_breakdown_list: list,
        sys_breakdown_sign: DBSysBreakdownSign):
    """
    创建工单
    :param authorization:
    :param current_project_settings:
    :param sys_breakdown_list:
    :param sys_breakdown_sign:
    :return:
    """
    # 创建工单
    work_order_code = gen_work_order(
        authorization=authorization,
        current_project_settings=current_project_settings,
        sys_breakdown_list=sys_breakdown_list)

    # 标记工单
    if work_order_code:
        Logger.info("[项目:{}]创建工单成功[{}]".format(current_project_settings.get("project_code"), work_order_code))
        for _line_sys_breakdown in sys_breakdown_list:
            sys_breakdown_sign.sign_work_order_status(
                sys_breakdown_id=_line_sys_breakdown.get("id"),
                work_order_code=work_order_code)


def start_sys_breakdown_to_work_order(db=None):
    """
    开始根据系统错误创建工单
    (1)获取数据库数据（获取错误未创建工单的数据及其最后一次恢复的数据）(24小时内)
    (2)获取所有未结束的工单(24小时内)
    (3)把（1）和（2）的数据进行处理：
        1）如果新的系统错误数据对应类型的工单存在未结束工单，那就判断未结束的工单创建之后是否存在错误恢复数据，如果没有，则忽略
        2）其它情况都会创建工单

    工单合并，按照项目进行合并
    :param db:
    :return:
    """
    # 获取数据库中的系统错误数据
    db_sys_breakdown = DBSysBreakdown(db=db)
    sys_breakdown_data: dict = db_sys_breakdown.get_sys_breakdown()
    if not sys_breakdown_data:
        return

    # 读取数据库中的项目配置信息
    db_project_msg_settings = DBProjectMSGSettings(db=db)
    project_msg_settings_dict = db_project_msg_settings.get_project_msg_settings()

    # 获取 token
    authorization = "{token_type} {access_token}".format(**token())

    # "已存在的工单" 定义 = 只获取除了“已关闭”与“已驳回”的工单
    # 获取所有"已存在的工单"
    exists_work_order = get_exists_work_order(authorization=authorization, filter_hour=24)

    # 标记类
    sys_breakdown_sign = DBSysBreakdownSign(db=db)

    # 判断是否需要创建工单（这里需要注意项目第一次初始化冲突）
    # 废弃：(1) 根据系统错误数据，判断是否存在项目配置，如果不存在就跳过该条数据，如果存在就执行 (2)
    # 废弃：(2) 判断是否存在 "已存在的工单"，如果不存在就创建工单，完成该条处理，如果存在就执行 (3)
    # 废弃：(3) 判断 "已存在的工单" 的创建时间是否大于改类工单的恢复系统错误数据的开始时间，如果大于就说明已经创建过，该条数据忽略，否则创建工单
    # 新版(2021-11-09): 以项目来，只要存在未关闭的工单，就创建新工单
    for project_code, sys_breakdown_list in sys_breakdown_data.items():
        if not isinstance(sys_breakdown_list, (list, tuple,)):
            continue

        # 不存在项目配置就跳过
        current_project_settings: dict = project_msg_settings_dict.get(project_code)
        if not current_project_settings:
            Logger.info("项目[{}]工单配置不存在".format(project_code))
            continue

        # 不存在就创建工单
        if not exists_work_order.get(project_code):
            create_work_order(authorization, current_project_settings, sys_breakdown_list, sys_breakdown_sign)
        else:
            Logger.info("项目[{}]已存在工单".format(project_code))


def start_sys_breakdown_to_dingtalk(db=None):
    """
    开始根据系统错误发送钉钉
    :param db:
    :return:
    """
    # 获取数据库中的系统错误数据
    db_sys_breakdown = DBSysBreakdown(db=db)

    # 读取数据库中的项目配置信息
    db_project_msg_settings = DBProjectMSGSettings(db=db)
    project_msg_settings_dict = db_project_msg_settings.get_project_msg_settings()

    sys_breakdown_sign = DBSysBreakdownSign(db=db)  # 标记类

    # 发送钉钉
    for k, sys_breakdown_list in db_sys_breakdown.get_sys_breakdown_dingtalk().items():
        if not isinstance(sys_breakdown_list, (list, tuple,)):
            continue

        project_code, item_status = str(k).split("##")

        # 不存在项目配置就跳过
        current_project_settings: dict = project_msg_settings_dict.get(project_code)
        if not current_project_settings:
            Logger.info("项目[{}]钉钉配置不存在".format(project_code))
            continue

        # 发送钉钉
        __dd = copy.deepcopy(current_project_settings)
        if send_dingtalk(sys_breakdown_list=sys_breakdown_list, item_status=item_status, **__dd):
            Logger.info("[项目:{}]发送钉钉成功".format(project_code))
            for _line_sys_breakdown in sys_breakdown_list:
                sys_breakdown_sign.sign_dingtalk_status(sys_breakdown_id=_line_sys_breakdown.get("id"))
        else:
            Logger.info("[项目:{}]发送钉钉失败".format(project_code))
