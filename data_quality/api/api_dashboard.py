import numpy as np
from django.http.response import JsonResponse
from django.views.decorators.http import require_http_methods

import sys, MySQLdb

sys.path.insert(0, '..')
from mysite import db_config
from utils import functions as f

# np.set_printoptions(precision=2, suppress=True)


@require_http_methods(['GET'])
def avg_problem_percentage(request):
    """
    平均问题占比
    :param request:
    :return:
    """
    # 接口返回值列表
    data = []
    data_quarter = ['quarter']
    data_productname = []
    
    # 获取所有年所有季度
    # year = f.query_data_year()
    # quarter = []
    # for y in year:
    #     q = f.query_data_quarter(y)
    #     quarter.extend([(y, i) for i in q])
    #     [data_quarter.append(str(y)+'Q'+str(i)) for i in q]
    # data.append(data_quarter)


    return JsonResponse(data, safe=False)
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()


@require_http_methods(['GET'])
def same_problem_top5(request):
    """
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # quarter = request.GET.get('quarter')
    # month = request.GET.get('month')
    # day = request.GET.get('day')

    # try:
    #     conn = db_config.mysql_connect()
    #     curs = conn.cursor()
    #     sql = f"""select check_item,count(*) cnt,sum(problem_per) from (
    #             select check_item,problem_per,check_date from check_result_xt where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_zc where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_db where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_jk where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_jj1 where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_jj2 where risk_market_item='是' and problem_per is not null
    #             union
    #             select check_item,problem_per,check_date from check_result_jz where risk_market_item='是' and problem_per is not null
    #             ) a, dim_date b
    #         where DATE_FORMAT(a.check_date,'%Y%m%d') = b.day_id
    #         and b.year={year}
    #         and b.quarter={quarter}
    #         and b.month={month}
    #         and b.day={day}
    #         group by check_item
    #         having count(*)>1
    #         order by 3 desc,2 desc"""
    #     curs.execute(sql)
    #     result = curs.fetchall()
    #
    #     check_item = []
    #     total_problem = []
    #     [check_item.append(r[0]) for r in result]
    #     [total_problem.append(float(str(r[2]))) for r in result]
    #
    #     # 取top4问题项及占比
    #     top4_item = check_item[0:4]
    #     top4_problem = total_problem[0:4]
    #     other_problem = sum(total_problem[4:])
    #     # 合并 其他项
    #     top4_item.append('其他')
    #     top4_problem.append(other_problem)
    #     data = {
    #         'name': top4_item,
    #         'value': top4_problem
    #     }
    return JsonResponse({})
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()



@require_http_methods(['GET'])
def subproductname_data_percentage(request):
    """
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # quarter = request.GET.get('quarter')
    # month = request.GET.get('month')
    # day = request.GET.get('day')

    data = []
    # try:
    #     conn = db_config.mysql_connect()
    #     curs = conn.cursor()
    #     for productname in ('game1', 'game2'):
    #         sql = f"""select sum(distinct item_count) from check_result_{productname} a,
    #                     (
    #                         select max(a.check_version) check_version
    #                         from check_result_{productname} a,dim_date b where DATE_FORMAT(a.check_date,'%Y%m%d') = b.day_id
    #                         and b.year={year}
    #                         and b.quarter={quarter}
    #                         and b.month={month}
    #                         and b.day={day}
    #                     ) b
    #                     where a.check_version=b.check_version
    #                     and a.risk_market_item='是'"""
    #         curs.execute(sql)
    #         result = curs.fetchone()
    #         if result[0] is None:
    #             data.append({'name': productname, 'value': 0})
    #         else:
    #             data.append({'name': productname, 'value': float(str(result[0]))})
    return JsonResponse(data, safe=False)
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()


def count_db_rows(request):
    """统计各类数据库数据量
    """
    quarter = request.GET.get('quarter')
    
    data = [{
                "name": "MySQL",
                "value": np.random.randint(1000,99999),
            },
            {
                "name": "Oracle",
                "value": np.random.randint(1000,99999)
            },
            {
                "name": "SQL server",
                "value": np.random.randint(1000,99999)
            },
            {
                "name": "HBase",
                "value": np.random.randint(1000,99999)
            },
            ]
    return JsonResponse(data, safe=False)


@require_http_methods(['GET'])
def data_overview_total(request):
    """
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # quarter = request.GET.get('quarter')
    # month = request.GET.get('month')
    # day = request.GET.get('day')

    all_cnt = 0
    problem_cnt = 0

    # conn = db_config.mysql_connect()
    # curs = conn.cursor()
    # for productname in ('xt', 'zc', 'db', 'jk', 'jj1', 'jj2', 'jz'):
    #     sql = f"""select sum(a.item_count),sum(a.problem_count) from check_result_{productname} a,
    #                 (
    #                     select max(a.check_version) check_version
    #                     from check_result_{productname} a,dim_date b where DATE_FORMAT(a.check_date,'%Y%m%d') = b.day_id
    #                     and b.year={year}
    #                     and b.quarter={quarter}
    #                     and b.month={month}
    #                     and b.day={day}
    #                 ) b
    #                 where a.check_version=b.check_version
    #                 and a.risk_market_item='是'"""
    #     curs.execute(sql)
    #     result = curs.fetchone()
    #     if result[0] is None:
    #         continue
    #     else:
    #         all_cnt = all_cnt + result[0]
    #         problem_cnt = problem_cnt + result[1]

    response = {
        'all_cnt': all_cnt,
        'problem_cnt': problem_cnt,
        'problem_per': 0.0 #round(problem_cnt / all_cnt * 100, 2)
    }
    return JsonResponse(response)

    
@require_http_methods(['GET'])
def data_overview_productname(request):
    """
    统计风险集市相关 各公司 检核数据量、问题数据量、问题数据占比
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # quarter = request.GET.get('quarter')
    # month = request.GET.get('month')
    # day = request.GET.get('day')
    

    data = []
    # try:
    #     conn = db_config.mysql_connect()
    #     curs = conn.cursor()
    #     for productname in ('gam1', 'game2'):
    #         sql = f"""select sum(a.item_count),sum(a.problem_count),round(sum(a.problem_count)/sum(a.item_count)*100,2)
    #                     from check_result_{productname} a,
    #                     (
    #                         select max(a.check_version) check_version
    #                         from check_result_{productname} a,dim_date b where DATE_FORMAT(a.check_date,'%Y%m%d') = b.day_id
    #                         and b.year={year}
    #                         and b.quarter={quarter}
    #                         and b.month={month}
    #                         and b.day={day}
    #                     ) b
    #                     where a.check_version=b.check_version
    #                     and a.risk_market_item='是'"""
    #         curs.execute (sql)
    #         result = curs.fetchone()
    #         data.append([productname, result[0], result[1], result[2]])
    #     return JsonResponse(data, safe=False)
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()
    #
    return JsonResponse(data, safe=False)
        
@require_http_methods(['GET'])
def data_overview_productname_trend(request):
    """
    统计风险集市相关 各公司 检核数据量、问题数据量、问题数据占比
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # month = request.GET.get('month')
    # day = request.GET.get('day')
    # productname = request.GET.get('productname')
    
    # try:
    #     conn = db_config.mysql_connect()
    #     curs = conn.cursor()
    #     sql = f"""select round(sum(problem_count)/sum(item_count)*100,2),check_version
    #                 from check_result_{productname}
    #                 where risk_market_item='是'
    #                 and check_date < date_add('{year}-{month}-{day}',interval 1 day)
    #                 group by check_version
    #                 order by check_version asc"""
    #     curs.execute(sql)
    #     result = curs.fetchall()
    #     result = [r[0] for r in result]
    #     return JsonResponse(result, safe=False)
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()
    return JsonResponse([], safe=False)


@require_http_methods(['GET'])
def total_trend(request):
    """
    显示总问题占比走势
    :param request:
    :return:
    """
    value = []
    try:
        conn = db_config.mysql_connect()
        curs = conn.cursor()
        sql = f"""select DATE_FORMAT(a.check_date,'%Y-%m-%d'),
                        round(sum(a.problem_count)/sum(a.item_count)*100,2),
                        count(distinct productname) from
                (
                select 'xt' productname,item_count,problem_count,check_date from check_result_xt where risk_market_item='是'
                union
                select 'zc' productname,item_count,problem_count,check_date from check_result_zc where risk_market_item='是'
                union
                select 'db' productname,item_count,problem_count,check_date from check_result_db where risk_market_item='是'
                union
                select 'jk' productname,item_count,problem_count,check_date from check_result_jk where risk_market_item='是'
                union
                select 'jj1' productname,item_count,problem_count,check_date from check_result_jj1 where risk_market_item='是'
                union
                select 'jj2' productname,item_count,problem_count,check_date from check_result_jj2 where risk_market_item='是'
                union
                select 'jz' productname,item_count,problem_count,check_date from check_result_jz where risk_market_item='是'
                ) a
                group by DATE_FORMAT(a.check_date,'%Y-%m-%d')
                having count(distinct productname)=7
                order by 1 asc"""
        curs.execute(sql)
        result = curs.fetchall()
        return JsonResponse({'datatime': [r[0] for r in result], 'value': [r[1] for r in result]}, safe=False)
    except Exception as e:
        print(e)
        return JsonResponse({'msg': str(e)})
    finally:
        curs.close()
        conn.close() 
        

@require_http_methods(['GET'])
def subproductname_problem_count(request):
    """
    -问题数据项统计
    :param request:
    :return:
    """
    year = request.GET.get('year')
    # quarter = request.GET.get('quarter')
    # month = request.GET.get('month')
    # day = request.GET.get('day')
    # productname = request.GET.get('productname')
    #
    # try:
    #     conn = db_config.mysql_connect()
    #     curs = conn.cursor()
    #     # 问题占比 | 问题数据总量 | 问题数据项
    #     sql = f"""select sum(a.item_count),sum(a.problem_count),a.check_item from (
    #                 select a.check_item,a.item_count,a.problem_count
    #                                     from check_result_{productname} a,
    #                                     (
    #                                         select max(a.check_version) check_version
    #                                         from check_result_{productname} a,dim_date b where DATE_FORMAT(a.check_date,'%Y%m%d') = b.day_id
    #                                         and b.year={year}
    #                                         and b.quarter={quarter}
    #                                         and b.month={month}
    #                                         and b.day={day}
    #                                     ) b
    #                                     where a.problem_count is not null
    #                                     and a.problem_count !=0
    #                                     and a.risk_market_item='是'
    #                                     and a.check_version=b.check_version
    #                 ) a
    #                 group by a.check_item
    #                 order by 2 desc"""
    #     curs.execute(sql)
    #     result = curs.fetchall()
    #     result_list = [['问题占比', '问题数据总量', '问题数据项'], ]
    #     for i in result:
    #         problem_per = (int(i[1]) / int(i[0]))
    #         problem_per = round(problem_per * 100, 2)
    #         problem_total = int(i[1])
    #         item = i[2]
    #         result_list.append([problem_per, problem_total, item])
    #     return JsonResponse(result_list, safe=False)
    # except Exception as e:
    #     print(e)
    #     return JsonResponse({'msg': str(e)})
    # finally:
    #     curs.close()
    #     conn.close()

