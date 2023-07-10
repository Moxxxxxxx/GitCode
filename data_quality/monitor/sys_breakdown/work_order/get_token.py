# -*- encoding: utf-8 -*-
"""
@File    :   get_token.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 13:52   parker      1.0         获取工单系统的 token
"""
from urllib.parse import urlencode

import requests

from monitor import settings


def token() -> dict:
    """
    获取工单系统 token
    :return:
    """
    rsp = requests.post(
        url="{base_url}/token".format(base_url=settings.WORK_ORDER_BASE_URL),
        data=urlencode({
            "grant_type": "xrm",
            "username": settings.WORK_ORDER_USERNAME,
            "password": settings.WORK_ORDER_PASSWORD
        }),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        },
    )

    return rsp.json()
