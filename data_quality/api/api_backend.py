from django.http.response import JsonResponse
from django.http.response import HttpResponseBadRequest
from django.views.decorators.http import require_http_methods
import pandas as pd
from crontab import CronTab

from mysite import db_config

CHARSET = 'utf8mb4'

def encrypy_password(connection_string):
    """将连接串中的密码替换为*号
    """
    str1 = connection_string.split('@')[0].split(':')[0]
    str2 = connection_string.split('@')[0].split(':')[1]
    str3 = connection_string.split('@')[1]
    return f'{str1}:{str2}:******@{str3}'


@require_http_methods(['GET'])
def server_query(request):
    try:
        conn = db_config.sqlalchemy_conn()
        server = pd.read_sql("select id,productname, server_name,ip,user,port,note from server_info order by id", con=conn)
        data = {
            'rowid': server['id'].values.tolist(),
            'productname': server['productname'].values.tolist(),
            'server_name': server['server_name'].values.tolist(),
            'ip': server['ip'].values.tolist(),
            'user': server['user'].values.tolist(),
            'port': server['port'].values.tolist(),
            'note': server['note'].values.tolist(),
        }
        return JsonResponse({'data': data, 'code': 1000})
    except Exception as e:
        return HttpResponseBadRequest(content=e)
    finally:
        conn.dispose()

@require_http_methods(['GET'])
def remote_server_query(request):
    try:
        conn = db_config.sqlalchemy_conn()
        remote_server = pd.read_sql("select id,productname, server_name, ip from server_info GROUP BY productname", con=conn)
        data = {
            'productname': remote_server['productname'].values.tolist(),
            'id': remote_server['id'].values.tolist(),
            'server_name': remote_server['server_name'].values.tolist(),
            'ip': remote_server['ip'].values.tolist(),
        }
        return JsonResponse({'data': data, 'code': 1000})
    except Exception as e:
        return HttpResponseBadRequest(content=e)
    finally:
        conn.dispose()


@require_http_methods(['GET'])
def db_query(request):
    try:
        conn = db_config.sqlalchemy_conn()
        db = pd.read_sql("select productname,db_type,alias,connection_string,db,ip,note,id from source_db_info order by productname,db_type", con=conn)
        
        db['connection_string'] = db['connection_string'].apply(encrypy_password)
        
        data = {
            'productname': db['productname'].values.tolist(),
            'db_type': db['db_type'].values.tolist(),
            'alias': db['alias'].values.tolist(),
            'connection_string': db['connection_string'].values.tolist(),
            'db': db['db'].values.tolist(),
            'ip': db['ip'].values.tolist(),
            'note': db['note'].values.tolist(),
            'rowid': db['id'].values.tolist()
        }
        return JsonResponse({'data': data, 'code': 1000})
    except Exception as e:
        return HttpResponseBadRequest(content=e)
    finally:
        conn.dispose()


@require_http_methods(['POST'])
def db_update(request):
    id = request.POST.get('id')
    ip = request.POST.get('ip')
    # alias = request.POST.get('alias')
    user = request.POST.get('user')
    password = request.POST.get('password')
    db = request.POST.get('db')
    port = request.POST.get('port')
    db_type = request.POST.get('db_type')
    note = request.POST.get('note')
    charset = request.POST.get('charset')
    if not charset:
        charset = CHARSET
    if db_type == 'mysql':
        connection_string = f'mysql+mysqldb://{user}:{password}@{ip}:{port}/{db}?charset={charset}'
    elif db_type == 'oracle':
        connection_string = f'oracle://{user}:{password}@{ip}:{port}/?service_name={db}'
    elif db_type == 'sqlserver':
        connection_string = f'mssql+pymssql://{user}:{password}@{ip}:{port}/{db}?charset={charset}'
    elif db_type == 'postgresql':
        connection_string = f'postgresql://{user}:{password}@{ip}:{port}/{db}'
    
    try:
        conn = db_config.mysql_connect()
        with conn.cursor() as curs:
            # alias='{alias}',
            sql = f"""update source_db_info
                        set 
                        connection_string='{connection_string}',
                        ip='{ip}',
                        passwd='{password}',
                        db='{db}',
                        port={port},
                        db_type='{db_type}',
                        note='{note}'
                        where id={id}"""
            curs.execute(sql)
        conn.commit()
        return JsonResponse({'data': '修改成功', 'code': 1000})
    except Exception as e:
        conn.rollback()
        return HttpResponseBadRequest(content=e)
    finally:
        conn.close()

@require_http_methods(['POST'])
def server_update(request):
    id = request.POST.get('id')
    ip = request.POST.get('ip')
    user = request.POST.get('user')
    password = request.POST.get('password')
    port = request.POST.get('port')
    note = request.POST.get('note')

    try:
        conn = db_config.mysql_connect()
        with conn.cursor() as curs:
            # alias='{alias}',
            sql = f"""update server_info
                        set 
                        ip='{ip}',
                        password='{password}',
                        port={port},
                        note='{note}'
                        where id={id}"""
            curs.execute(sql)
        conn.commit()
        return JsonResponse({'data': '修改成功', 'code': 1000})
    except Exception as e:
        conn.rollback()
        return HttpResponseBadRequest(content=e)
    finally:
        conn.close()

@require_http_methods(['POST'])
def server_insert(request):
    productname = request.POST.get('productname')
    server_name = request.POST.get('server_name')
    ip = request.POST.get('ip')
    user = request.POST.get('user')
    password = request.POST.get('password')
    port = request.POST.get('port')
    note = request.POST.get('note')
    try:
        conn = db_config.mysql_connect()
        with conn.cursor() as curs:
            if curs.execute(f"""select count(*) from server_info where server_name='{server_name}'"""):
                result = curs.fetchall()
                if result and result[0][0] >0: #如果服务器存在数据源
                    return  HttpResponseBadRequest('此服务器或产品线已存在数据')
                else:
                    sql = f"""insert into server_info(productname, server_name,ip,user,password,port,note)
                                values('{productname}','{server_name}','{ip}','{user}','{password}','{port}','{note}')"""
                    curs.execute(sql)
        conn.commit()
        return JsonResponse({'data': '新增成功', 'code': 1000})
    except Exception as e:
        conn.rollback()
        return HttpResponseBadRequest(content=e)
    finally:
        conn.close()

@require_http_methods(['POST'])
def db_insert(request):
    productname = request.POST.get('productname')
    # name = request.POST.get('name')
    # alias = request.POST.get('alias')
    ip = request.POST.get('ip')
    user = request.POST.get('user')
    password = request.POST.get('password')
    db = request.POST.get('db')
    port = request.POST.get('port')
    db_type = request.POST.get('db_type')
    charset = request.POST.get('charset')
    if not charset:
        charset = CHARSET
    note = request.POST.get('note')
    
    if db_type == 'mysql':
        connection_string = f'mysql+mysqldb://{user}:{password}@{ip}:{port}/{db}?charset={charset}'
    elif db_type == 'oracle':
        connection_string = f'oracle://{user}:{password}@{ip}:{port}/?service_name={db}'
    elif db_type == 'sqlserver':
        connection_string = f'mssql+pymssql://{user}:{password}@{ip}:{port}/{db}?charset={charset}'
    elif db_type == 'postgresql':
        connection_string = f'postgresql://{user}:{password}@{ip}:{port}/{db}'
        
    try:
        resultstr = ''
        conn = db_config.mysql_connect()
        with conn.cursor() as curs:
            if curs.execute(f"""select count(*) from source_db_info where productname='{productname}'"""):
                result = curs.fetchall()
                if result and result[0][0] >0: #如果此产品线已存在数据源
                    # return  HttpResponseBadRequest('此产品线已存在数据源')
                    resultstr = '新增失败, 此产品线已存在数据源'
                else:
                    sql = f"""insert into source_db_info(productname,connection_string,ip,user,passwd,db,port,db_type,note)
                                values('{productname}','{connection_string}','{ip}','{user}','{password}','{db}',{port},'{db_type}','{note}')"""
                    curs.execute(sql)
        conn.commit()
        resultstr = '新增成功'
    except Exception as e:
        conn.rollback()
        return HttpResponseBadRequest(content=e)
    finally:
        conn.close()
    return JsonResponse({'data': resultstr, 'code': 1000})


@require_http_methods(['POST'])
def crontab_enable(request):
    """
    启用/禁用自动检核的crontab任务
    :param request:
    :return:
    """
    enable = request.POST.get('enable')
    job_name = request.POST.get('job_name')

    cron = CronTab(user=True)
    job = list(cron.find_comment(job_name))[0]

    if enable == 'false':
        # job.enable(False)
        # cron.write()
        return JsonResponse({"msg": "success"})
    elif enable == 'true':
        # job.enable()
        # cron.write()
        return JsonResponse({"msg": "success"})
    else:
        return JsonResponse({"msg": "failed"})
    

@require_http_methods(['POST'])
def crontab_run(request):
    job_name = request.POST.get('job_name')
    
    try:
        cron =  CronTab(user=True)
        job = list(cron.find_comment(job_name))[0]
        job.run()
        return JsonResponse({"msg": "success"})
    except Exception as e:
        return HttpResponseBadRequest(content=e)