# -*- encoding: utf-8 -*-
"""
@File    :   gen_work_order.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 13:50   parker      1.0         生成工单
"""
import datetime
import json

import pytz
import requests

from monitor import settings
from monitor.settings import Logger
from monitor.utils.str import string_similar


def get_project_info(authorization, **kwargs) -> dict:
    """
    根据项目信息，返回系统中最匹配的项目基础信息
    :param authorization:
    :param kwargs:
    :return:
    """
    resp_project_list = []
    # for search_value in [kwargs.get("project_code"), kwargs.get("project_name")]:
    for search_value in [kwargs.get("project_code"), ]:
        project_index_resp = requests.get(
            headers={
                "Host": settings.WORK_ORDER_HOST,
                "Authorization": authorization,
                "Accept-Language": "zh-CN",
            },
            url="{base_url}/api/vlist/ExecuteQueryForLookUp".format(base_url=settings.WORK_ORDER_BASE_URL),
            params={
                "queryid": "c86f0074-640a-0ad0-0000-06026b4d9513",
                "entityName": "account",
                "count": "20",
                "page": "1",
                "select": "name,accountid,accountnumber,createdon",
                "orderby": "name asc",
                "filter": "",
                "filterValue": search_value,
                "countType": "0",
                "returnTotalRecordCount": "true",
            }
        )
        resp_project_list.extend(project_index_resp.json().get("Data", {}).get("Data", []))

    # 如果没有找到项目，则返回
    if not resp_project_list:
        return {}

    # 找到最匹配的项目
    project_list = []
    for line_project in resp_project_list:
        if not isinstance(line_project, (dict,)):
            continue
        line_project.update({"string_similar": string_similar(line_project.get("name"), kwargs.get("project_name"))})
        project_list.append(line_project)

    project_dict = max(project_list, key=lambda x: x.get("string_similar"))

    # 根据项目的信息获取匹配后的项目数据
    project_info_resp = requests.get(
        headers={
            "Host": settings.WORK_ORDER_HOST,
            "Authorization": authorization,
            "Accept-Language": "zh-CN",
        },
        url="{base_url}/api/dynamic/account/Get".format(base_url=settings.WORK_ORDER_BASE_URL),
        params={"id": project_dict.get("accountid")}
    )
    return project_info_resp.json().get("Data", {})


def get_work_order_user_info(authorization: str, username: str, **kwargs) -> dict:
    """
    根据用户名，返回系统中最匹配的一个用户信息
    :param authorization:
    :param username:
    :param kwargs:
    :return:
    """
    user_resp = requests.get(
        headers={
            "Host": settings.WORK_ORDER_HOST,
            "Authorization": authorization,
            "Accept-Language": "zh-CN",
        },
        url="{base_url}/api/crmlookupview/getdata".format(base_url=settings.WORK_ORDER_BASE_URL),
        params={
            "entityName": "systemuser",
            "page": 1,
            "count": 20,
            "select": "fullname,systemuserid,domainname,businessunitid,mobilephone",
            "orderby": "",
            "filter": "domainname,fullname,systemuserid,businessunitid,mobilephone",
            "filterValue": username,
            "condition": "new_usertype eq 1",
            "countType": 0,
            "returnTotalRecordCount": "true"
        }
    )

    users_list = []
    for line_user in user_resp.json().get("Data", {}).get("Data", []):
        if not isinstance(line_user, (dict,)):
            continue
        line_user.update({"string_similar": string_similar(line_user.get("fullname"), username)})
        users_list.append(line_user)

    return max(users_list, key=lambda x: x.get("string_similar"))


def distribution_work_order(authorization: str, owner_id: str, work_order_code: str, **kwargs):
    """
    根据用户名，返回系统中最匹配的一个用户信息
    :param authorization: 认证
    :param owner_id: 用户id
    :param work_order_code: 工单id
    :param kwargs:
    :return:
    """
    distribution_resp = requests.post(
        headers={
            "Host": settings.WORK_ORDER_HOST,
            "Authorization": authorization,
            "Accept-Language": "zh-CN",
            "Content-Type": "application/json;charset=UTF-8",
        },
        url="{base_url}/api/dynamic/BatchAssign".format(base_url=settings.WORK_ORDER_BASE_URL),
        params={
            "entityName": "incident",
            "ownerid": owner_id
        },
        json=[work_order_code]
    )
    return distribution_resp.status_code


def gen_work_order(authorization: str, current_project_settings: dict, sys_breakdown_list: (list, tuple)) -> str:
    """
    创建工单
    :param authorization: 认证
    :param current_project_settings: 当前项目的配置
    :param sys_breakdown_list: 错误信息列表
    :return:
    """
    # 取出项目基础信息,取出需要合并的错误信息
    order_body, base_project = {}, {}
    for _line_breakdown in sys_breakdown_list:
        order_body[_line_breakdown.get("item_name")] = str(_line_breakdown.get("breakdown_time"))
        base_project.update(**_line_breakdown)

    # 获取项目信息
    project_info_dict = get_project_info(authorization=authorization, **base_project)
    if not project_info_dict:
        Logger.warn("[{project_name}]工单系统没有找到项目配置".format(**base_project))
        return None

    # 获取本用户信息
    current_user_rsp = requests.get(
        headers={"Authorization": authorization},
        url="{base_url}/api/PortalUser/userInfo".format(base_url=settings.WORK_ORDER_BASE_URL),
    )
    current_user_rsp = current_user_rsp.json().get("Data", {})

    work_order_data = {
        "quicktron_iscopy": "false",
        "new_ifsubmit": "true",
        "prioritycode": 1,

        # 10-未响应,20-已响应，30-处理中
        "quicktron_case_newstatus": 10,  # 未响应
        "ownerid": {
            "id": current_user_rsp.get("SystemUserId"),
            "logicalname": "systemuser",
            "name": current_user_rsp.get("UserName")
        },
        "new_type": 40,
        "new_accepttime": datetime.datetime.now(tz=pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "quicktron_onlinedefects": 1,
        "quicktron_problemtype": 20,
        "quicktron_dealways": 10,

        # https://quicktroncrm.recloud.com.cn/api/crmpicklist/options/incident/caseorigincode
        # 判断类型: 售后-微信(4)，非售后-钉钉(6)
        "caseorigincode": 4 if project_info_dict.get("new_isfrozen") else 6,

        "statuscode": 1,
        "customerid": {
            "name": project_info_dict.get("name"),
            "id": project_info_dict.get("accountid"),
            "logicalname": "account"
        },
        "quicktron_projectnumber": project_info_dict.get("accountnumber"),
        "quicktron_prosys": project_info_dict.get("quicktron_prosys"),
        "quicktron_case_sysver": project_info_dict.get("quicktron_sysversion"),
        "quicktron_custom_case": 2,
        "quicktron_case_agv": project_info_dict.get("quicktron_agvtype", {}),  # 项目小车信息
        "quicktron_new_phase": 30,
        "quicktron_submitver": project_info_dict.get("quicktron_sysversion"),
        # "quicktron_firstcategory": 10,  # 问题大类
        # "quicktron_secondcategory": 40,  # 问题子类
        "quicktron_onetype": {
            "id": "08240074-640a-0ac1-0000-0622dc8bdcd4",
            "name": "服务器",
            "logicalName": "quicktron_bigcategory"
        },
        "quicktron_twotype": {
            "id": "08240074-640a-0a32-0000-0622dc8d590e",
            "name": "其他",
            "logicalName": "quicktron_smallcategory"
        },
        "new_memo": json.dumps(order_body, ensure_ascii=False),
        "quicktron_supplement": json.dumps(order_body, ensure_ascii=False),
        "quicktron_dealsteps": json.dumps(order_body, ensure_ascii=False)
    }

    create_work_order_resp = requests.post(
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json;charset=UTF-8",
        },
        url="{base_url}/api/dynamic/incident/SaveAndFetch".format(base_url=settings.WORK_ORDER_BASE_URL),
        json=work_order_data,
    )

    # # 获取匹配的用户,distribution_user 分配用户
    # distribution_user = get_work_order_user_info(
    #     authorization=authorization,
    #     username=current_project_settings.get("work_order_msg_user_name"))

    work_order_code = create_work_order_resp.json().get("Data", {}).get("incidentid")

    # # 分配工单
    # distribution_work_order(
    #     authorization=authorization,
    #     owner_id=distribution_user.get("systemuserid"),
    #     work_order_code=work_order_code
    # )
    return work_order_code
