import datetime
import sys

import MySQLdb
import pandas as pd
from django.http.response import JsonResponse, HttpResponse, HttpResponseBadRequest
from django.views.decorators.http import require_http_methods

from mysite.loger import logging

sys.path.insert(0, '..')
from mysite import db_config
from check.autocheck import Check, MyThread

ALLCHECKCOUNT = 0

try:
    logger = logging.getLogger()
except:
    print('logging.getLogger error')


@require_http_methods(['GET'])
def rule(request):
    """
    根据产品线名查询所有检核规则详情
    """
    productname = request.GET.get('productname')
    if not productname:
        productname = '%'
    conn = db_config.mysql_connect()
    curs = conn.cursor()
    curs.execute('set autocommit=0')
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S')
    logger.info("rule start time " + ts)
    try:
        sql = f"""select id,check_item,target_table,problem_type,db,check_sql,note,status,productname
                    from check_result_template
                    where productname like ('{productname}')
                    order by id"""
        curs.execute(sql)
        result = curs.fetchall()
        # 构造json
        result_list = []
        for i in result:
            result_dict = {"id": i[0], "check_item": i[1], "target_table": i[2],
                           "problem_type": i[3], "db": i[4], "check_sql": i[5], "note": i[6], "status": i[7],
                           "productname": i[8]}
            result_list.append(result_dict)
        json_data = {'data': result_list}
        now = datetime.datetime.now()
        ts = now.strftime('%Y-%m-%d %H:%M:%S')
        logger.info("rule end time " + ts)
        return JsonResponse(json_data)
    except Exception as e:
        print(e)
        return HttpResponse('error', status=500)
    finally:
        curs.close()
        conn.close()

@require_http_methods(['GET'])
def query_game(request):
    """
    查询产品线名
    """
    try:
        conn = db_config.sqlalchemy_conn()
        result_list = pd.read_sql("select productname from check_result_template where is_deleted = 0 GROUP BY productname", con=conn)
        data = {
            'productname': result_list['productname'].values.tolist(),
        }
        return JsonResponse({'data': data, 'code': 1000})
    except Exception as e:
        return HttpResponseBadRequest(content=e)
    finally:
        conn.dispose()




@require_http_methods(['GET'])
def rule_detail(request):
    """
    根据产品线名、id查询单条规则详情
    """
    productname = request.GET.get('productname')
    id =  request.GET.get('id')
    
    data = {
        "id": None,
        "productname": None,
        "check_item": None,
        "target_table": None,
        # "risk_market_item": None,
        "problem_type": None,
        "db": None,
        "check_sql": None,
        "note": None,
        "status": None,
    }
    if id == 'null':
        return JsonResponse(data)
    
    sql = f"""select id,check_item,target_table,problem_type,db,check_sql,note,status
    from check_result_template
    where productname in ('{productname}') and id={id}"""
    conn = db_config.sqlalchemy_conn()
    try:
        result = pd.read_sql(sql, con=conn)
        data = {
            "check_item": result['check_item'].values.tolist()[0],
            "target_table": result['target_table'].values.tolist()[0],
            "problem_type": result['problem_type'].values.tolist()[0],
            "db": result['db'].values.tolist()[0],
            "check_sql": result['check_sql'].values.tolist()[0],
            "note": result['note'].values.tolist()[0],
            "status": result['status'].values.tolist()[0],
        }
        return JsonResponse(data)
    except Exception as e:
        return HttpResponseBadRequest(e)
    finally:
        conn.dispose()


@require_http_methods(['POST'])
def rule_update(request):
    """
    执行修改检核规则
    """
    id = request.POST.get('id')
    productname = request.POST.get('productname')
    check_item = request.POST.get('check_item')
    target_table = request.POST.get('target_table')
    remote_ip = request.POST.get('remote_ip')
    problem_type = request.POST.get('problem_type')
    db = request.POST.get('db')
    check_sql = request.POST.get('check_sql')
    note = request.POST.get('note')
    status = request.POST.get('status')

    # 把"转义为'，再把'转义为''
    check_sql = MySQLdb.escape_string(check_sql).encode('utf-8').decode('utf-8')
    # print(check_sql)
    try:
        conn = db_config.mysql_connect()
        curs = conn.cursor()
        curs.execute('set autocommit=0')
        sql = f"""update check_result_template set check_item='{check_item}',
                                                target_table='{target_table}',
                                                problem_type='{problem_type}',
                                                db='{db}',
                                                check_sql='{check_sql}',
                                                note='{note}',
                                                remote_ip='{remote_ip}',
                                                status='{status}'
                                                where id={id} and productname='{productname}'"""
        # print(sql)
        curs.execute(sql)
        conn.commit()
        return JsonResponse({'msg': '修改成功', 'code': 1000})
    except Exception as e:
        return HttpResponse(e, status=500)
    finally:
        curs.close()
        conn.close()


@require_http_methods(['POST'])
def rule_add(request):
    """
    新增检核规则
    """
    productname = request.POST.get('productname')
    check_item = request.POST.get('check_item')
    target_table = request.POST.get('target_table')
    remote_ip = request.POST.get('remote_ip')
    problem_type = request.POST.get('problem_type')
    db = request.POST.get('db')
    check_sql = request.POST.get('check_sql')
    note = request.POST.get('note')
    status = request.POST.get('status')

    # 处理检核SQL中含有''的情况
    #MySQLdb.escape_string(check_sql).encode('utf-8').decode('utf-8')
    check_sql = MySQLdb.escape_string(check_sql).encode('utf-8').decode('utf-8')
    try:
        # 连接数据库
        conn = db_config.mysql_connect()
        curs = conn.cursor()
        curs.execute('set autocommit=0')
        sql = "select max(id)+1 from check_result_template where productname in ('" + productname + "')"
        curs.execute(sql)
        result = curs.fetchone()
        if result and result[0]:
            new_id = str(result[0])  # 获取新增的id
        else:
            new_id = 1
        sql = f"""insert into check_result_template(id,
                                                    productname,
                                                    check_item,
                                                    target_table,
                                                    problem_type,
                                                    db,
                                                    check_sql,
                                                    note,
                                                    remote_ip,
                                                    status)
                values({new_id},
                        '{productname}',
                        '{check_item}',
                        '{target_table}',
                        '{problem_type}',
                        '{db}',
                        '{check_sql}',
                        '{note}',
                        '{remote_ip}',
                        '{status}')"""
        # print(sql)
        curs.execute(sql)
        conn.commit()
        return JsonResponse({'msg': '修改成功', 'code': 1000})
    except Exception as e:
        logging.error('insert check rule error')
        return HttpResponse(e, status=500)
    finally:
        curs.close()
        conn.close()


@require_http_methods(['POST'])
def rule_status_modify(request):
    """修改检核规则状态，禁用/启用 规则的状态
    """
    id = request.POST.get('id')
    post_status = request.POST.get('status')
    productname = request.POST.get('productname')

    conn = db_config.mysql_connect()
    curs = conn.cursor()
    curs.execute('set autocommit=0')
    # 修改状态
    try:
        # if post_status == '已启用':
        if post_status == '1':
            sql = f"update check_result_template set status=0 where id={id} and productname='{productname}'"
            rr = curs.execute(sql)
            conn.commit()
            return JsonResponse({'msg': '修改成功', 'code': 1000})
        else:
            sql = f"update check_result_template set status=1 where id={id} and productname='{productname}'"
            rr = curs.execute(sql)
            conn.commit()
            return JsonResponse({'msg': '修改成功', 'code': 1000})
    except:
        return HttpResponse('error', status=500)
    finally:
        curs.close()
        conn.close()

@require_http_methods(['POST'])
def all_rule_execute(request):
    try:
        username = request.POST.get('username')
        check = Check()
        global ALLCHECKCOUNT
        if ALLCHECKCOUNT < 1:
            ALLCHECKCOUNT += 1
            thread = MyThread(func=check.run_allcheck,
                               args=(username,))
            thread.start()
            thread.join()
            data= {
                "status": "success",
                "msg": "规则执行完成！"
            }
            ALLCHECKCOUNT = 0
        else:
            data= {
                "status": "fail",
                "msg": "已有检核任务在执行！"
            }
    except:
        {
            "status":"发生错误",
            "msg": "未知"
        }
    return JsonResponse(data)

@require_http_methods(['POST'])
def rule_execute(request):
    """执行检核
    """
    productname = request.POST.get('productname')
    username = request.POST.get('username')
    ruleid   = request.POST.get('id')
    # quarter  = request.POST.get('quarter')
    check = Check(productname)

    if check.init_table(productname):
        # 初始化3个线程
        # thread1 = MyThread(func=check.run_check,
        #                    args=(productname, productname, quarter, 'oracle'))
        # thread2 = MyThread(func=check.run_check,
        #                    args=(productname, productname, quarter, 'sqlserver'))
        # thread3 = MyThread(func=check.xt_spec, args=(quarter,))
        thread4 = MyThread(func=check.run_rule,
                           args=(ruleid, productname,  'mysql', username))
        # 开启3个线程
        # thread1.start()
        # thread2.start()
        # thread3.start()
        thread4.start()
        # 等待运行结束
        # thread1.join()
        # thread2.join()
        # thread3.join()
        thread4.join()

        # if thread1.get_result() is True:
        #     if thread2.get_result() is True:
        #         if thread3.get_result() is True:
        #             if thread4.get_result() is True:
        #                 run = True
        #             else:
        #                 return JsonResponse({
        #                     "status":
        #                         "检核过程发生错误：" + str(thread4.get_result())
        #                 })
        #         else:
        #             return JsonResponse({
        #                 "status":
        #                     "检核过程发生错误：" + str(thread3.get_result())
        #             })
        #     else:
        #         return JsonResponse(
        #             {"status": "检核过程发生错误：" + str(thread2.get_result())})
        # else:
        #     return JsonResponse(
        #         {"status": "检核过程发生错误：" + str(thread1.get_result())})
        if thread4.get_result() is True:
            run = True
        else:
                        return JsonResponse({
                            "status":
                                "检核过程发生错误：" + str(thread4.get_result())
                        })

    else:
        return JsonResponse({"status": "初始化检核表发生错误"})

    # elif productname == 'game2':
    #     productname = 'game2'
    #     check = Check()
    #     if check.init_table(productname, productname, quarter):
    #         run = check.run_check(productname, productname, quarter, None)
    #         if run is not True:
    #             return JsonResponse({"status": "检核过程发生错误：" + str(run)})
    #     else:
    #         return JsonResponse({"status": "初始化检核表发生错误"})

    if run is True:
        return JsonResponse({
            "status": "success",
            "msg": productname + "检核成功！"
        })


def update_crontab(request):
    """更新crontab命令
    """
    job_time = request.POST.get('job_time')

    try:
        # cron  = CronTab(user=True)
        # job = list(cron.find_comment('自动进行数据质量检核'))[0]
        # job.setall(job_time)
        return JsonResponse({"msg": "操作成功"})
    except Exception as e:
        return JsonResponse({"msg": "操作失败", "reason": e})
    
    
@require_http_methods(['GET'])
def query_check_progress(request):
    """
    查询正在运行的检核任务执行进度
    :param request:
    :return:
    """
    productname = request.GET.get('productname')
    db = request.GET.get('db')
    
    data = {}
    try:
        conn = db_config.mysql_connect()
        for productname in ('product1', 'product2'):
            data[productname] = {}
            
            with conn.cursor() as curs:
                # 已检核指标总数
                sql  = f"""select a.db,count(*)
                            from check_result_{productname} a,
                            (
                                select max(check_version) check_version,db from check_result_{productname}
                                where db in (select distinct alias from source_db_info where productname='{productname}')
                                group by db
                            ) b
                            where a.check_sql is not null
                            and a.check_sql != ''
                            and a.check_version=b.check_version
                            and a.db=b.db
                            and a.update_flag='Y'
                            group by a.db"""
                curs.execute(sql)
                result = curs.fetchall()
                for i in result:
                    data[productname][i[0]] = i[1]
                    
                # 待检核指标总数
                sql  = f"""select a.db,count(*)
                            from check_result_{productname} a,
                            (
                                select max(check_version) check_version,db from check_result_{productname}
                                where db in (select distinct alias from source_db_info where productname='{productname}')
                                group by db
                            ) b
                            where a.check_sql is not null
                            and a.check_sql != ''
                            and a.check_version=b.check_version
                            and a.db=b.db
                            group by a.db"""
                curs.execute(sql)
                result = curs.fetchall()
                for i in result:
                    if i[1] == 0:
                        data[productname][i[0]] = 0
                    else:
                        data[productname][i[0]] = round(data[productname][i[0]]/i[1]*100, 2)
                    
        return JsonResponse(data)
    except Exception as e:
        return HttpResponseBadRequest(e)
    finally:
        conn.close()



@require_http_methods(['GET'])
def job_progress(request):
    """
    查询正在运行的检核任务执行进度
    :param request:
    :return:
    """
    data = {}
    return JsonResponse({})

@require_http_methods(['GET'])
def job_progress_1(request):
    """
    查询正在运行的检核任务执行进度
    :param request:
    :return:
    """
    data = {}
    try:
        conn = db_config.mysql_connect()
        for productname in ('product1', 'product2'):
            data[productname] = {}
            with conn.cursor() as curs:
                # 已检核指标总数
                sql  = f"""select a.db,count(*)
                            from check_result_{productname} a,
                            (
                                select max(check_version) check_version,db from check_result_{productname}
                                where db in (select distinct alias from source_db_info where productname='{productname}')
                                group by db
                            ) b
                            where a.check_sql is not null
                            and a.check_sql != ''
                            and a.check_version=b.check_version
                            and a.db=b.db
                            and a.update_flag='Y'
                            group by a.db"""
                curs.execute(sql)
                result = curs.fetchall()
                for i in result:
                    data[productname][i[0]] = i[1]

                # 待检核指标总数
                sql  = f"""select a.db,count(*)
                            from check_result_{productname} a,
                            (
                                select max(check_version) check_version,db from check_result_{productname}
                                where db in (select distinct alias from source_db_info where productname='{productname}')
                                group by db
                            ) b
                            where a.check_sql is not null
                            and a.check_sql != ''
                            and a.check_version=b.check_version
                            and a.db=b.db
                            group by a.db"""
                curs.execute(sql)
                result = curs.fetchall()
                for i in result:
                    if i[1] == 0:
                        data[productname][i[0]] = 0
                    else:
                        data[productname][i[0]] = round(data[productname][i[0]]/i[1]*100, 2)
        return JsonResponse(data)
    except Exception as e:
        return HttpResponseBadRequest(e)
    finally:
        conn.close()