import logging
import re
import sys
import threading

from check.result_check import Result_Check

try:
    import MySQLdb
except:
    print('import MySQLdb error')

from utils.dingding_msg import dingding_notice
from utils.remote_execute import execute_by_agent

sys.path.insert(0, '..')

import utils.date_utils

from mysite import db_config
from sqlalchemy import create_engine
from utils import  excute_shell, ssh_shell, strutil


# 暂不考虑ORCALE
# os.environ['NLS_LANG']    = 'AMERICAN_AMERICA.UTF8'
# os.environ['ORACLE_HOME'] = '/data/oracle/app/11.2.4'
try:
    logger = logging.getLogger()
except:
    print('logging.getLogger error')

class Check(object):

    def __init__(self, productname=''):
        self.productname = productname
        self.db = ''
        self.username = ''
        self.conn = None
        self.curs = None

    def init_table(self, productname=''):
        """
        类实例化所需参数
        :param productname:
        :return:
        初始化检核表
            如果check_result_{0}表存在，则从check_result_template表中插入对应产品线检核项和逻辑
            如果check_result_{0}表不存在，则使用check_result_template表作为模板新建
        """
        if not productname:
            productname = self.productname

        logger.info('*' * 50)
        logger.info(f'开始初始化检核结果表...check_result_{productname}')
        
        conn = db_config.mysql_connect()
        curs = conn.cursor()
        curs.execute('set autocommit=0')
        
        sql = f"select table_name from information_schema.tables where table_schema='{db_config.database}' and table_name='check_result_{productname}'"
        table_count = curs.execute(sql)
        try:
            if table_count == 0:    # 表不存在则新建
                sql = f"""create table check_result_{productname} as select * from check_result_template
                                                                where productname='{productname}' order by id,productname"""
                curs.execute(sql)
            else:                   # 表存在则插入
                # 获取检核版本号
                sql = f"select count(*) from check_execute_log where productname='{productname}'"
                curs.execute(sql)
                result = curs.fetchone()
                if( result):
                    version = result[0] + 1
                else:
                    version = 1
                # 可能存在了初始化完检核表，但是检核失败导致事务回滚，检核表check_version={version}数据项为空的情况，因此需要处理这种情况
                for sql in (
                    f"delete from check_result_{productname} where check_version={version}",
                    f"insert into check_result_{productname} select * from check_result_template where productname='{productname}' order by id,productname",
                    f"update check_result_{productname} set check_version={version} where check_version is null",
                ):
                    curs.execute(sql)
                
            conn.commit()
            logger.info('*' * 50 +  f"初始化 check_result_{productname}表 ...完成" + '*' * 50)
            return True
        except Exception as e:
            conn.rollback()
            logger.error('!' * 50 + f'初始化 check_result_{productname}表 ...失败,错误信息：{str(e)}' + '!' * 50)
            return False
        finally:
            curs.close()
            conn.close()

    def excute_rule(self, checkrule, source_result, productname, version=''):
        """
        执行检核
        类实例化所需参数
        :param productname: 产品线简称
        :return:
        """
        logger.info('start excute_rule')
        bverify_result = False #检验结果
        excute_operator = '' #目前定义 = > < 或includ,缺省为
        excute_code = ''
        expect = ''
        check_result = ''
        check_result_list = None
        msg = '"rule check success"' #msg格式定义必须如此
        remote_ip_config = None
        try:
            # 连接源系统数据库
            if source_result:
                connection_string = source_result[0]
                db = source_result[1]
                self.db =db
            else:
                connection_string = ''
            engine = create_engine(
                connection_string,
                echo=False,                     # 打印sql语句
                max_overflow=0,                 # 超过连接池大小外最多创建的连接
                pool_size=5,                    # 连接池大小
                pool_timeout=30,                # 池中没有线程最多等待的时间，否则报错
                pool_recycle=-1,                # 多久之后对线程池中的线程进行一次连接的回收（重置）
            )
            conn_source = engine.raw_connection()
            curs_source = conn_source.cursor()
            #with conn_source.cursor() as curs_source:
                # curs_source = conn_source.cursor()
            # 执行检核
            id = checkrule[0]
            check_sql  = checkrule[1]
            check_name = checkrule[5]
            remote_ip  = checkrule[6]
            note       = checkrule[7]
            if remote_ip:
                remote_ip_config = self.getRemoteInfo(remote_ip)
            err = ''
            logger.info('excute sql or shell')
            if check_sql and check_sql.strip():
                check_sql = check_sql.strip()
                check_sql = self.set_var(check_sql) #读取动态变量
                check_sql_list = check_sql.split(';')
                sql = check_sql_list[0]
                #如果checksql中不存在目标表，则用target_table
                try:
                    if sql :
                        sql = sql.strip()
                        if (sql.find('FROM') > -1 and not sql.split("FROM")[1]) or (sql.find('from') > -1 and not sql.split("from")[1]):
                            sql = sql + " "+ checkrule[3]
                except Exception as e:
                    logger.error(e)
                    logger.info("check_sql error" + sql)
                logger.info(f'{productname}, db={db}, id={checkrule[0]} >>>开始检核')
                try:
                    try:
                        if sql.find('select') > -1 or sql.find('SELECT') > -1:
                            curs_source.execute(sql)
                            check_result_list = curs_source.fetchall()  # sql检核结果
                        else: # 脚本
                            check_result_list = [[self.excute_shell(sql, remote_ip_config)]]
                    except Exception as e:
                        logger.error(' curs_source.execute sql or invoke shell  error')
                        logger.error(e)
                        check_result_list = [[' curs_source.execute sql or invoke shell exception']]
                    if check_result_list and check_result_list[0]: #(())第一行数据的第一个数据
                        check_result = check_result_list[0][0]
                        logger.info("acutal result")
                        logger.info(check_result)
                    if len(check_sql_list) > 1:
                        excute_operator = check_sql_list[1]
                    if len(check_sql_list) > 2:
                        excute_code = check_sql_list[2]
                        if excute_code and excute_code.lower().find('select') > -1:
                            # logger.info("do't implement")
                            curs_source.execute(excute_code)
                            check_result_list = curs_source.fetchall()  # sql检核结果
                            expect = self.getExpect(check_result_list)
                        else:
                            expect = self.excute_shell(excute_code, remote_ip_config)
                    bsave_success = False
                    if excute_operator:
                        excute_operator = strutil.strip(excute_operator)
                        if excute_operator == 'alert':
                            msg = '"读取datacenter错误日志表,表中存在异常记录 "'
                            if check_result_list:
                                bsave_success = self.save_alert_info(check_result_list)
                            elif check_result:
                                bsave_success = self.save_alert_info([[check_result]])
                            else:
                                bverify_result = True
                                msg = '"datacenter错误日志表无异常信息 "'
                            if not bsave_success: #如果未保存新的异常信息，则可能已经预警
                                bverify_result = True
                                msg = '"读取datacenter错误日志表,表中无异常记录 "'
                        elif expect or expect == 0 or expect == '0':
                            logger.info('start verify_result')
                            bverify_result = self.verify_result(check_result, excute_operator, expect)
                        else:
                            bverify_result = False
                            err = 'expect is null!maybe excute shell or sql fail!!!'
                    else:
                        if check_result:
                            bverify_result  = True
                except Exception as e:
                    logger.error("excute error!" + e)
                    err += e
            else:
                check_code = checkrule[2]
                if check_code and check_code.strip():
                    excute_shell.excute(check_code)
            if not err:
                err = ' is null '
            if not check_result: check_result = 0
            if excute_operator != 'alert':
                msg = f'''"the rule is {check_name}, the check_sql is {check_sql} the acutal checkresult is {check_result} and the expect value is {excute_operator} {expect} the err {err}"'''
            if   not bverify_result:
                subject = f'game {productname} excute fail'
                is_notice_success = 0
                if self.notice("检核预警",check_name, note, msg):
                    logger.info('send notice success!!')
                    is_notice_success = 1
                info = [check_name, msg, note, is_notice_success]
                self.save_abnormal_info(info)
            else:
                info = ['excute pass', msg]
            logger.info(f'{productname}, db={db}, id={checkrule[0]} <<<完成')
            if bverify_result:
                result = 'success'
            else:
                result = 'fail'
            self.save_excute_log(check_name, result, msg)
            self.update_check_result_template(result, check_name)
            conn_source.close()
            # # 根据检核结果明细计算问题占比，后期扩展
            # self.calc_result(version)
            logger.info("-" * 25 + f'{productname}, db={db} 检核完成' +  "-" * 25)
            logger.info('end excute_rule')
            return bverify_result
        except Exception as e:
            print(e)
            logger.error(e)
            #id={ruleid}
            logger.error("!" * 25 + f'{productname}, db={db}, 检核出错,错误信息：{str(e)}' + "!" * 25)
            self.save_abnormal_info(['exception error', e, 'exception'])
            #需要保存异常到日志表
            return False

    def getExpect(self, check_result_list):
        try:
            return check_result_list[0][0]
        except Exception as e:
            logger.error("getExpect error!" + e)
            return "getExpect err" + e


    def get_project_version(self, project_code):
        version = get_version_from_ops();
        if not version:
            version= get_version_from_dataplatform(project_code)
        return version

    def run_rule(self, ruleid, productname='', db='', username = ''):
        """
        执行检核
        类实例化所需参数
        :param productname: 产品线简称
        :param db:      检核的数据库
        :return:
        """
        logger.info('start run_rule')
        if not productname:
            productname = self.productname
        else:
            self.productname = productname
        if username:
            self.username = username
        err = ''
        bverify_result = False #检验结果
        try:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
            # curs.execute('set autocommit=0')
            self.conn = conn
            self.curs = curs
            # 从规则库表中取出检核项和检核sql，只运行“已启用”状态的SQL STATUS = 1[已启用]  check_code后续扩展支持python、shell脚本运行    and db='{db}'
            sql = f"""select id,check_sql,check_code,target_table,db,check_item, remote_ip, note from check_result_template
                    where id ='{ruleid}'
                    and ((check_sql is not null
                    and check_sql != '') or (check_code is not null and check_code != ''))
                    and status=1
                    order by id"""
            curs.execute(sql)
            checkrule = curs.fetchone()
            # 获取检核版本号
            curs.execute(f"select count(*) from check_execute_log where productname='{productname}'")
            result = curs.fetchone()
            if( result):
                version = result[0] + 1
            else:
                version = 1
            # 连接源系统数据库
            curs.execute(f"select connection_string,db from source_db_info where db='{checkrule[4]}'")
            source_result = curs.fetchone()
            bverify_result = self.excute_rule(checkrule, source_result, productname, version)
        except Exception as e:
            conn.rollback()
        finally:
            curs.close()
            conn.close()
        logger.info('end run_rule')
        return bverify_result


    #批量执行，通过定时器跑
    def run_check(self, productname='', username = ''):
        """
        执行检核
        # 类实例化所需参数
        # :param productname: 产品线简称
        # :param db:      检核的数据库
        # :return:
        # """
        logger.info('start run_check')
        if not productname:
            productname = self.productname
        else:
            self.productname = productname
        if username:
            self.username = username
        logger.info('-' * 50)
        logger.info("正在检核" + productname + "数据...")
        try:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
            self.conn = conn
            self.curs = curs
            curs.execute('set autocommit=0')
            # 从规则库表中取出检核项和检核sql，只运行“已启用”状态的SQL STATUS = 1[已启用]  check_code后续扩展支持python、shell脚本运行    and db='{db}'
            sql = f"""select id,check_sql,check_code,target_table,db,check_item, remote_ip, note from check_result_template
                    where productname='{productname}'
                    and ((check_sql is not null
                    and check_sql != '') or (check_code is not null and check_code != ''))
                    and status=1
                    order by id"""
            curs.execute(sql)
            check_list = curs.fetchall()
            curs.execute(f"select count(*) from check_execute_log where productname='{productname}'")
            result = curs.fetchone()
            if( result):
                version = result[0] + 1
            else:
                version = 1
            for checkrule in check_list:
                # 连接源系统数据库 checkrule
                #
                curs.execute(f"select connection_string,db from source_db_info where db='{checkrule[4]}' ")
                source_result = curs.fetchone()
                self.excute_rule(checkrule, source_result, productname, version)
        except Exception as e:
            conn.rollback()
        finally:
            curs.close()
            conn.close()
        logger.info('end run_check')

    def run_allcheck(self, username):
        logger.info('start run_allcheck')
        productname_list = self.get_productname()
        threads = []
        for product in productname_list:
            self.init_table(product[0])
            threads.append(MyThread(func=self.run_check,
                     args=(product[0], username)))
        for thread in threads:
            thread.start()
            thread.join()
        logger.info('end run_allcheck')

    def getRemoteInfo(self, remote_ip):
        remote_ip_conf = ()
        try:
            sql = f"""select ip,port,user,password from server_info where ip = '{remote_ip}'"""
            self.curs.execute(sql)
            remote_ip_conf = self.curs.fetchone()
        except Exception as e:
            logger.error('getRemoteInfo error')
            logger.error(e)
        return remote_ip_conf

    def get_productname(self):
        productname_list = []
        try:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
            sql = """select productname from check_result_template GROUP BY productname"""
            curs.execute(sql)
            productname_list = curs.fetchall()
        finally:
            curs.close()
            conn.close()
        return productname_list

    def getcheck_excute_log_ver(self):
        pass

    def notice(self, subject, check_name, note, msg):
        logger.info("send dingding notice!!!")
        return dingding_notice(subject, check_name, note, msg)


    def set_var(self, sql):
        vars = re.findall(r"##[^#]+##", sql, flags=0) #[{]*\w+[()+-/*%{}&.]*\d*[}]*
        valuelist = []
        logger.info('start set_var')
        for var in vars:
            varname = var[2:-2] #remove ##
            print("varname is ", varname)
            try:
                if varname.find('{') > -1: #执行{}表示为一段python语句
                    varname = varname[1:-1]
                    if varname.find('&') > -1: #若需要导入包，在执行语句前加&
                        varnames = varname.split('&')
                        exec(varnames[0])
                        exec(f"self.value = {varnames[1]}")
                    else:
                        exec(f"self.value = {varname}")
                else:
                    #兼容老的包含time的变量动态获取变量
                    if varname.find('(') < 0 and varname.find('time') > -1:
                        varname = 'get_' + varname + '()'
                    exec("from utils import  var_setting")
                    exec(f"self.value = var_setting.{varname}") #读取var_setting中变量的值
                logger.info('value:')
                logger.info(self.value)
                sql = sql.replace(var, str(self.value))
            except Exception as e:
                logger.error(e)
                logger.error(f"excute value = var_setting.{varname} fail")
        logger.info('end set_var')
        return sql

    def save_excute_log(self, check_item, result, execute_result_info=''):
        logger.info('start save_excute_log')
        if not self.conn or not self.curs:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
        else:
            conn = self.conn
            curs = self.curs
        #insert into check_execute_log(productname, execute_date, execute_user, db, status) values('game1',now(),'admin','', 'sucess')
        try:
            try:
                execute_result_info = self.transferContent(execute_result_info)
            except:
                logger.error('self.transferContent(execute_result_info) error')
            sql = f"insert into check_execute_log(productname, check_item, execute_date, execute_user, status, execute_result_info) " \
                  f"values('{self.productname}','{check_item}',now(),'{self.username}','{result}',{execute_result_info})"
            logger.info(sql)
            curs.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(e)
        logger.info('end save_excute_log')

    def update_check_result_template(self, result, check_item):
        logger.info('start update_check_result_template')
        try:
            #update check_result_template
            check_result_template_sql = f"update check_result_template set check_date=now(), check_result='{result}' where check_item='{check_item}' and productname='{self.productname}'"
            rr = self.curs.execute(check_result_template_sql)
            self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.info('end update_check_result_template')

    def excute_shell(self, excute_code, remote_ip_config=None):
        logger.info('start excute_shell')
        expect = ''
        # 若是sh文件的，则为linux执行脚本 linux shell
        if excute_code:
            if isinstance(excute_code, str):
                excute_code = excute_code.strip()
            if (excute_code and isinstance(excute_code, str) and excute_code.isdigit()) or isinstance(excute_code, int):
                expect = int(excute_code)
            elif excute_code.find("http") > -1: #通过http agent的方式调用linux命令或shell脚本
                try:
                    excute_list = excute_code.split(',')
                    expect = execute_by_agent(excute_list[0].strip(), excute_list[1].strip())
                    logger.info("invoke http result")
                    logger.info(expect)
                except Exception as e:
                    logger.error("http agent invokde fail")
                    logger.error(e)
            elif excute_code.find("sh:") > -1:
                try:
                    excute_code = excute_code[3:-1]
                    expect = ssh_shell.excute_shell_by_db(excute_code, remote_ip_config)[0]
                except Exception as e:
                    logger.error("ssh_shell excute fail")
                    logger.error(e)
            else: #default linux cmd
                try:
                    expect = excute_code
                    #logger.info("exute linux cmd shell")
                    # expect = ssh_shell.excute_shell_by_db(excute_code, remote_ip_config)[0]
                except Exception as e:
                    logger.error("linux cmd excute fail")
                    logger.error(e)
        logger.info('end excute_shell')
        return expect


    def save_abnormal_info(self, info):
        '''
        将异常信息保存到预警表中
        :param conn:
        :param curs:
        :param info:
        :return:
        '''
        try:
            subject = info[0]
            abnormal_info = info[1]
            note = info[2]
            severity_level = 1
            is_read = 0
            if len(info) > 3:
                is_read = info[3]
            try:
                abnormal_info = self.transferContent(abnormal_info)
            except:
                logger.error('MySQLdb.escape_string(abnormal_info)')
            sql = f"insert into prewarning_info(subject,abnormal_info,severity_level,abnormal_date, note, is_read, created_by)" \
                  f"values('{subject}',{abnormal_info},'{severity_level}',current_timestamp, '{note}',{is_read} , '{self.username}')"
            logger.info(sql)
            self.curs.execute(sql)
            self.conn.commit()
        except Exception as e:
            logger.error(e)
            logger.error(sql)

    #插入MySQL ，转后期优化
    def transferContent(self, content):
        result = content
        try:
            if content :
                content = content.strip()
                contents = content[1:-2]
                # contents = content.split('where')
                # if len(contents) > 1:
                string = ''
                #     wheresql = contents[1].split(';')
                for c in contents:
                    if c == '"':
                        string += '\\\"'
                    # elif c == "'":
                    #     string += "\\\'"
                    # elif c == "\\":
                    #     string += "\\\\"
                    else:
                        string += c
                result = '"' + string + '"'
                # for i in range(1, len(wheresql)):
                #     result = result + wheresql[i]
        except Exception as e:
            logger.error(e)
            logger.error('transferContent error')
        return result

    def verify_result(self, origin, operator, expect, check_result_list=None):
        return  Result_Check().verify_result(origin, operator, expect, check_result_list)

    #create by = alerter
    def save_alert_info(self, origin):
        abnormal_date = None
        bsave_success = False
        try:
            sql = f"""select CAST(MAX(abnormal_date) as CHAR) from prewarning_info
                            where created_by = 'alerter'
                              and is_read = 0
                              and is_deleted = 0"""
            # CAST(check_date as char),
            self.curs.execute(sql)
            abnormal_dates = self.curs.fetchone()
            if abnormal_dates:
                abnormal_date = abnormal_dates[0]
            #保存信息到预警表
            for abnormal in origin:
                subject = "check base_error_log_info table!"
                abnormal_info = f"the content is {abnormal[1]} , the file is {abnormal[2]}, the line is {abnormal[3]}"
                logger.info(abnormal_date)
                # current_timestamp, 错误日志表的时间>异常记录表的时间或是异常记录表中无错误信息日志表中的则入库
                if not abnormal_date or (abnormal_date and utils.date_utils.compare_time(abnormal_date, abnormal[5])> 0):
                    sql = f"""insert into prewarning_info(subject,severity_level, abnormal_info,abnormal_date, created_by)
                            values('{subject}',1,'{abnormal_info}', '{abnormal[5]}', 'alerter')"""
                    self.curs.execute(sql)
                    bsave_success = True
            self.conn.commit()
            return bsave_success
        except Exception as e:
            logger.error('save_alert_info save err info fail!!!')

    @staticmethod
    def abnormal_notice(version=''):
        try:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
            sql = f"""select id,subject,abnormal_info,severity_level,abnormal_date,note from prewarning_info
                        where  is_read = 0
                          and is_deleted = 0
                          order by id"""
            curs.execute(sql)
            abnormal_info_list = curs.fetchall()
            for abnormal_info in  abnormal_info_list:
                dingding_notice('检核预警--异常通知', abnormal_info[1], abnormal_info[5], abnormal_info[2])
                #sendmail()
                update_sql = f"""update prewarning_info set is_read=1
                              where is_read = 0
                              and id = {abnormal_info[0]}
                              """
                curs.execute(update_sql)
            conn.commit()
        finally:
            curs.close()
            conn.close()

    def calc_result(self, version):
        """根据检核结果明细计算问题占比
        1. 填充空值的问题占比
        2. 计算正常的问题占比
        
        :param version:     要进行计算的版本号
        :return:            检核成功返回True，失败返回False
        """
        productname = self.productname
        try:
            conn = db_config.mysql_connect()
            curs = conn.cursor()
            curs.execute('set autocommit=0')
            # 计算问题占比
            # 处理item_count和problem_count都是null或=0的行
            sql = f"""update check_result_{productname}
                        set problem_per=100
                        where (item_count is null or item_count=0)
                        and check_version={version}"""
            curs.execute(sql)

            # 计算正常的问题占比
            sql = f"""update check_result_{productname} set problem_per=problem_count/item_count*100\
                        where problem_per is null
                        and check_version={version}"""
            curs.execute(sql)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            return False
        finally:
            curs.close()
            conn.close()
    

class MyThread(threading.Thread):
    """重新定义带返回值的线程类"""
    def __init__(self,func,args=()):
        super(MyThread,self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


if __name__ == '__main__':
    ss = "dfhdkjfhkd'##get_prev_10m_datetime()##'dfdfdf,##prev_10m_datetime##"
    check = Check('product1')
    s = check.set_var(ss)
    print(s)
