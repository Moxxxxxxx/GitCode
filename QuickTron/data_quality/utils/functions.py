import datetime
import math
from functools import wraps

import numpy as np
from django.shortcuts import redirect

from mysite import db_config


# from itertools import chain

class  is_login():
    """从session判断发起请求的账号是否已经登录，session中未有登录信息则跳转到登录界面
    """

    def __new__(self, func):
        @wraps(func)
        def _wrap(request):
            if request.session.get('is_login') is None or request is None:
                return redirect("../../authorize/login")
            else:
                f = func(request)
                return f

        return _wrap


def get_quarter_list():
    """获取检核结果库中所有季度的列表
    """
    """
    """
    return '2019Q1', '2019Q2', '2019Q3', '2019Q4'


# 检核结果Excel明细
def get_result_detail(productname, quarter):
    return []


def query_check_progressbar(productname, quarter):
    """查询当前检核进度
    """
    conn = db_config.mysql_connect()
    curs = conn.cursor()
    try:
        sql = f"select count(*) from check_result_{productname}_{quarter} where check_sql is not null and check_sql != '' "
        curs.execute(sql)
        to_be_check_cnt = curs.fetchone()[0]

        sql = f"select count(*) from check_result_{productname}_{quarter} where check_sql is not null and check_sql != '' and update_flag='Y'"
        curs.execute(sql)
        checked_cnt = curs.fetchone()[0]
        return round(checked_cnt / to_be_check_cnt * 100, 2)
    except Exception:
        return 0
    finally:
        curs.close()
        conn.close()


def get_user_quarter(request):
    """初始化仪表盘季度
    传入参数：request
         如果GET请求没有传入quarter参数，则先判断用户session是否有上一次选定的季度
              - 如果上一次有选定季度，则显示上次选定的季度
              - 没有选定季度，则默认显示上一季度数据

    返回参数：quarter
    """
    if request.GET.get('quarter') is None:
        if request.session.get('selected_quarter') is None:
            if math.ceil(datetime.datetime.now().month / 3.) - 1 == 0:  # 如果季度=0，则显示去年Q4季度
                quarter = str(datetime.datetime.now().year - 1) + "Q4"
            else:
                quarter = str(datetime.datetime.now().year) + "Q" + str(
                    math.ceil(datetime.datetime.now().month / 3.) - 1)
        else:
            quarter = request.session['selected_quarter']
    else:
        quarter = request.GET.get('quarter')
        request.session['selected_quarter'] = request.GET.get('quarter')
    return quarter



def query_data_year():
    try:
        conn = db_config.mysql_connect()
        curs = conn.cursor()
        sql = """select date_format(execute_date,'%Y'),count(distinct productname) as cnt from check_execute_log
                where status='success'
                group by date_format(execute_date,'%Y')
                # having count(distinct productname)>=7
                order by 1 desc"""
        curs.execute(sql)
        year = curs.fetchall()
        year = [y[0] for y in year]
        return year
    except Exception as e:
        print('获取年份错误:', e)
        return False
    finally:
        curs.close()
        conn.close()
