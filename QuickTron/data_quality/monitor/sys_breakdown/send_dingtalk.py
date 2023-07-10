# -*- encoding: utf-8 -*-
"""
@File    :   send_dingtalk.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/26 19:14   parker      1.0        发送钉钉消息
"""
import base64
import hashlib
import hmac
import time
import urllib.parse

import requests


def send_dingtalk(sys_breakdown_list, item_status, dingtalk_token, dingtalk_token_secret=None, **kwargs) -> bool:
    if dingtalk_token_secret:
        timestamp = str(round(time.time() * 1000))
        string_to_sign_enc = '{}\n{}'.format(timestamp, dingtalk_token_secret).encode('utf-8')
        hmac_code = hmac.new(dingtalk_token_secret.encode('utf-8'), string_to_sign_enc,
                             digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        params = {
            "access_token": dingtalk_token,
            "timestamp": timestamp,
            "sign": sign
        }
    else:
        params = {
            "access_token": dingtalk_token,
        }

    # 取出项目基础信息
    order_body, base_project = [], {}
    for _line_breakdown in sys_breakdown_list:
        order_body.append("[{}]".format(_line_breakdown.get("item_name")))
        base_project.update(**_line_breakdown)

    if item_status != "resolved":
        title = "现场出现了故障，请立即处理"
        title_col = '<font color="#dd0000" size="3">现场出现了故障,请立即处理</font>'
        item_name_str = "".join(['<font color="#dd0000">{}</font>'.format(ob) for ob in order_body])
    else:
        title = "现场故障已恢复"
        title_col = '<font color="#00dd00" size="3">现场故障已恢复</font>'
        item_name_str = "".join(['<font color="#00dd00">{}</font>'.format(ob) for ob in order_body])

    # 更新数据
    kwargs.update(**base_project)
    kwargs.update({"item_name": item_name_str})

    resp = requests.post(
        url="https://oapi.dingtalk.com/robot/send",
        params=params,
        headers={"Content-Type": "application/json;charset=utf-8"},
        json={
            "msgtype": "markdown",
            "markdown":
                {
                    "title": title,
                    "text": "{title_col} \n\n > "
                            "**[项目编码]** {project_code} \n\n > "
                            "**[项目名称]** {project_name} \n\n > "
                            "**[问题]** {item_name} \n\n > "
                            "**[故障等级]** {breakdown_level} \n\n > "
                            "###### @来自系统告警".format(title_col=title_col, **kwargs),
                },
            "at": {
                "isAtAll": "false"
            },
        }
    )
    if int(resp.json().get("errcode")) == 0:
        return True
    return False
