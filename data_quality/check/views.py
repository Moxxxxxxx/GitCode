import datetime
import logging
import sys

from crontab import CronTab
from django.http.response import HttpResponseBadRequest
from django.http.response import JsonResponse
from django.shortcuts import render

from check import autocheck
from utils.schedule_util import start_scheduletask

sys.path.insert(0, '..')
from mysite import db_config
from utils.functions import is_login

logger = logging.getLogger('')

@is_login
def rule_list(request):
    """
    检核规则列表
    :param request:
    :return:
    """
    productname      = request.GET.get('productname')
    username           = request.session['username']

    return render(request, "check/rule_list.html", {"productname": productname,
                                                    "username": username
                                                    }
                  )


@is_login
def rule_edit(request):
    """
    单条检核规则页面
    :param request:
    :return:
    """
    return render(request, "check/rule_edit.html", {"username": request.session['username'],
                                                   "productname": request.GET.get('productname'),
                                                   "id": request.GET.get('id')
                                                   })


@is_login
def rule_execute_manual(request):
    """
    查询检核进度
    :param request:
    :return:
    """
    date = str(datetime.datetime.now().year) + "-" + str(datetime.datetime.now().month) + '-' + str(datetime.datetime.now().day)
    return render(request, "check/rule_exec.html", {"date": date})


@is_login
def job_add(request):
    """新增执行任务
    """
    return render(request, "check/jobs_add.html")

#异常通知
@is_login
def abnormal_notice(request):
    """异常通知
    """
    autocheck.Check.abnormal_notice()
    return    JsonResponse({'status': 'success', 'msg': "已发送通知！"})


@is_login
def job_insert(request):
    productname = request.POST.get('productname')
    jobdes = request.POST.get('jobdes')
    jobname = request.POST.get('jobname')
    db = request.POST.get('db')
    try:
        conn = db_config.mysql_connect()
        with conn.cursor() as curs:
            sql = f"""insert into check_jobs(productname,jobname,job_description,db)
                        values('{productname}','{jobname}','{jobdes}','{db}')"""
            curs.execute(sql)
        conn.commit()
        return JsonResponse({'data': '新增成功', 'code': 1000})
    except Exception as e:
        conn.rollback()
        return HttpResponseBadRequest(content=e)
    finally:
        conn.close()

@is_login
def show_jobs(request):
    """
    运行校验规则job
    :param request:
    :return:
    """
    conn = db_config.mysql_connect()
    with conn.cursor() as curs:
        # 查询各个产品线检核规则配置的数据库、上次检核任务的运行情况
        sql = """select distinct
                                productname,
                                db,
                                CAST(check_date as char),
                                status,
                                id,
                                check_item,
                                check_result
                from check_result_template 
                where db is not null and  ((check_sql is not null
                    and check_sql != '') or (check_code is not null and check_code != ''))
                """
        # group by productname
        # sql = """select distinct
        #                         a.productname,
        #                         a.db,
        #                         CAST(c.execute_date as char),
        #                         c.status,
        #                         c.id,
        #                         a.check_item
        #         from check_result_template a,
        #         (select id, productname, execute_date, status  from check_execute_log ) c
        #         where a.productname=c.productname and a.db is not null
        #         """
        #   (select productname, jobname,job_excute_date,is_excuted,id from check_jobs  where is_excuted = 0
        curs.execute(sql)
        jobs = curs.fetchall()

    # 根据数据源中的产品线和数据库信息匹配crontab定时任务  CronTab(user=True)
    cron = CronTab(user=True)
    data = []
    for i in jobs:
        job = list(cron.find_comment(f'autocheck-{i[1]}-{i[2]}'))
        t = list(i)
        if len(job) > 0:
            enable = job[0].is_enabled()                                #
            job_time = job[0].description(use_24hour_time_format=True, locale_code='zh_CN')
            t.extend([enable, job_time])
        else:
            t.append(None)
        data.append(t)
    return render(request, "check/joblist.html", {"jobs": data})

@is_login
def delete_log_excute_log(request):
    conn = db_config.mysql_connect()
    with conn.cursor() as curs:
        # 清除日志
        sql = "update check_execute_log set is_delete=1"
        curs.execute(sql)
    conn.commit()
    return render(request, "check/excute_log.html", {"excute_logs": []})

@is_login
def show_excute_log(request):
    """
    执行日志列表
    :param request:
    :return:
    """
    conn = db_config.mysql_connect()
    with conn.cursor() as curs:
        # 查询各个产品线检核规则配置的数据库、上次检核任务的运行情况
        sql = """select distinct
                                id,
                                productname,
                                check_item,
                                CAST(execute_date as char),
                                status,
                                execute_result_info
                from check_execute_log 
                where is_delete = 0
                order by execute_date desc limit 50
                """
        curs.execute(sql)
        excute_logs = curs.fetchall()
    data = []
    for i in excute_logs:
        data.append(list(i))
    return render(request, "check/excute_log.html", {"excute_logs": data})

@is_login
def schedule_config(request):
    """
    :param request:
    :return:
    """
    f = open("schedule.conf", 'r', encoding='utf-8')
    data = [f.readlines()]
    return render(request, "check/schedule_config.html", {"schedule_config": data})

@is_login
def schedule_switch(request):
    """
    :param request:
    :return:
    """
    switch = request.GET.get('switch')
    try:
        if switch:
            start_scheduletask(switch)
    except Exception as e:
        JsonResponse({'status': '', 'msg': "修改失败！"})
    return JsonResponse({'status': 'success', 'msg': "修改成功！"})

@is_login
def show_abnormal_info(request):
    """
    执行日志列表
    :param request:
    :return:
    """
    conn = db_config.mysql_connect()
    with conn.cursor() as curs:
        # 查询各个产品线检核规则配置的数据库、上次检核任务的运行情况
        sql = """select distinct
                                id,
                                subject,
                                abnormal_info,
                                CAST(abnormal_date as char),
                                severity_level
                from prewarning_info 
                WHERE  is_deleted = 0 
                ORDER BY abnormal_date desc LIMIT 100
                """
        curs.execute(sql)
        abnormal_infos = curs.fetchall()
    data = []
    for i in abnormal_infos:
        data.append(list(i))
    return render(request, "check/abnormal_info.html", {"abnormal_infos": data})

@is_login
def show_crontab(request):
    """
    自动检核配置页面
    :param request:
    :return:
    """
    conn = db_config.mysql_connect()
    with conn.cursor() as curs:
        # 查询各个产品线检核规则配置的数据库、上次检核任务的运行情况
        sql = """select distinct b.name,
                                a.productname,
                                a.db,
                                CAST(c.execute_date as char),
                                c.status
                from check_result_template a,
                source_db_info b,
                (select db,productname,execute_date,status from check_execute_log  where id in 
                    (
                        select id from (select max(id) id,productname,db from check_execute_log where db is not null group by productname,db) a
                    )
                ) c
                where a.db=b.alias
                and a.db=c.db
                and a.productname=c.productname
                order by 1,2,3"""
        curs.execute(sql)
        jobs = curs.fetchall()
        
    # 根据数据源中的产品线和数据库信息匹配crontab定时任务  CronTab(user=True)
    cron = CronTab(user=True)
    data = []
    for i in jobs:
        job = list(cron.find_comment(f'autocheck-{i[1]}-{i[2]}'))
        t = list(i)
        if len(job) > 0:
            enable = job[0].is_enabled()                                # 获取crontab启用状态
            job_time = job[0].description(use_24hour_time_format=True, locale_code='zh_CN') # 获取crontab的调度周期 
            t.extend([enable, job_time])
        else:
            t.append(None)
        data.append(t)
    
    return render(request, "check/crontab.html", {"jobs": data})
