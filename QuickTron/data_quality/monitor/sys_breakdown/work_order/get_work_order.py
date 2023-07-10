# -*- encoding: utf-8 -*-
"""
@File    :   get_work_order.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 16:19   parker      1.0         获取工单
"""
import datetime

import requests

from monitor import settings
from monitor.utils.str import str_to_time, gen_md5


def get_exists_work_order(authorization: str, filter_hour: int) -> dict:
    """
    获取当前时间的相对小时内所有未响应
    """
    result_rows, base_time = [], datetime.datetime.now() + datetime.timedelta(hours=-filter_hour)
    # 状态编码
    # 10-未响应, 20-已响应, 30-处理中, 40-转研发, 45-研发已处理, 50-已关闭, 60-已驳回
    for case_new_status in [10, 20, 30, 40, 45]:
        next_page, page = True, 1
        while next_page:
            resp = requests.get(
                headers={
                    "Host": settings.WORK_ORDER_HOST,
                    "Authorization": authorization,
                    "Accept-Language": "zh-CN",
                },
                url="{base_url}/api/vlist/ExecuteQuery".format(base_url=settings.WORK_ORDER_BASE_URL),
                params={
                    "queryid": "94100074-640a-0a5a-0000-05ff5619f680",
                    "isPreview": "false",
                    "$pageSize": 20,
                    "$pageIndex": page,
                    "$paging": "true",
                    "$search": "",
                    "$additionalConditions": '{"quicktron_case_newstatus": ' + str(case_new_status) + '}',
                }
            )

            # 判断是否需要下一页
            current_rows_num = page * 20
            if int(resp.json().get("Data", {}).get("TotalRecordCount", 0)) > current_rows_num:
                page += 1
            else:
                next_page = False
            result_rows += resp.json().get("Data", {}).get("Entities", [])

    # 筛选出在24小时内的数据
    # 每个错误类型的数据仅获取最新一条
    result_dict = {}
    for line in result_rows:
        if not isinstance(line, (dict,)):
            continue

        line_create_time = str_to_time(line.get("createdon"))
        if line_create_time >= base_time:
            if "quicktron_projectnumber" not in line:
                continue
            _key = "{quicktron_projectnumber}".format(**line)
            key_max_time = str_to_time(result_dict.get(_key, {}).get("createdon", "1997-01-01 00:00:00"))
            if line_create_time > key_max_time:
                result_dict[_key] = line

    return result_dict
