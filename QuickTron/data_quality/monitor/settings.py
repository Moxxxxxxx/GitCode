# -*- encoding: utf-8 -*-
"""
@File    :   settings.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/9/24 13:54   parker      1.0         这是设置文件
"""
from pathlib import Path
import logging

BASE_DIR = Path(__file__).resolve().parent.parent

# 工单基础信息
WORK_ORDER_HOST = "quicktroncrm.recloud.com.cn"
WORK_ORDER_BASE_URL = "https://{host}".format(host=WORK_ORDER_HOST)
WORK_ORDER_USERNAME = "monitor-reboot"
WORK_ORDER_PASSWORD = "1Passw0Rd@123!"

# 数据库地址
DB_HOST = "172.31.237.5"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "tClEDdt6"
DB_DATABASE = "kc_collect"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s]%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
Logger = logging.getLogger(name="sys_breakdown")
