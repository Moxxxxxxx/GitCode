import datetime
import pendulum
import random
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.contrib.operators.dingding_operator import DingdingOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook




default_args = {
    'owner': 'wangziming',  # 拥有者名称
    'depends_on_past': False,   # 是否依赖上一个自己的执行状态
    'execution_timeout': None,
    'retries': 1,  # 失败重试次数
    'retry_delay': datetime.timedelta(seconds=30)  # 失败重试间隔
    # 'on_failure_callback': compass_utils.ding_failure_callback('dingding_bigdata'),
    # 'on_success_callback': compass_utils.ding_success_callback('dingding_bigdata')，
    # 'trigger_rule':'all_success'(默认),
  # 'end_date': datetime(2020, 1, 1),   # 结束时间，注释掉就会一直执行下去
}


def failure_callback(context):
    message = """### <font color=#FF0000> {0} airflow task 告警 </font> ###\n- **【DAG】** <font color=#808080>{1}</font>\n- **【任务】** <font color=#808080>{2}</font>\n- **【原因】** <font color=#808080>{3}</font>\n
              """.format(datetime.date.today(),
                     context['task_instance'].dag_id,
                     context['task_instance'].task_id,
                     context['exception'])

    return DingdingOperator(
        task_id='dingding_success_callback',
        dingding_conn_id='dingding_test',
        message_type='markdown',
        message={
            'title':'Airflow dingding Error',
            'text': message
        },
        at_all=True
        ).execute(context)
default_args['on_failure_callback'] = failure_callback



with DAG(
    dag_id='quicktron_edw',
    schedule_interval='10 12 * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="Asia/Shanghai"),
    default_args=default_args,
    catchup=False, # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
    #dagrun_timeout=datetime.timedelta(minutes=60), # 改dags最大的运行时长，超时则会报错退出
    tags=['quicktron_offline'],
    params={"pre1_date": "test1"},
) as dag:

################################################################################## init ##################################################################################

    mysqlConn = BaseHook.get_connection('qkt_mysql008') #根据hook获取mysql的链接参数
    host = mysqlConn.host
    user = mysqlConn.login
    password = mysqlConn.password
    database = mysqlConn.schema
    port = mysqlConn.port


    # 昨日日期
    yesterday = "{{ yesterday_ds }}"

    #  datax-(mysqlreader-hivewriter)
    mr_hw_json=f'''start-datax.sh "\--readerPlugin mysqlreader 
    \--ipAddress 008.bg.qkt
    \--port {port} 
    \--dataBase (mysql_db) 
    \--userName {user} 
    \--passWord {password} 
    \--querySql (mysql_query)
    \--separator 
    \--writerPlugin hivewriter 
    \--dataBase (hive_db) 
    \--table (hive_table) 
    \--defaultFs hdfs://001.bg.qkt:8020 
    \--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
    \--writeMode overwrite 
    \--tmpDataBase tmp 
    \--tmpPath /user/hive/warehouse/tmp.db/ 
    \--partition (hive_partition)
    \--column (hive_column)" "(datax_json_file)" "{yesterday}"
    '''

    # datax-(hivereader-mysqlwriter)
    hr_mw_json=f'''start-datax.sh "\--readerPlugin hivereader 
    \--hiveSql (hive_query) 
    \--defaultFs hdfs://001.bg.qkt:8020  
    \--tmpDataBase tmp 
    \--tmpPath /user/hive/warehouse/tmp.db/ 
    \--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive; 
    \--separator 
    \--writerPlugin mysqlwriter 
    \--column (mysql_column) 
    \--ipAddress 007.bg.qkt 
    \--port {port} 
    \--dataBase (mysql_db)  
    \--table (mysql_table)  
    \--preSql (mysql_presql)
    \--passWord {password}
    \--userName {user}" "(datax_json_file)" "{yesterday}"
    '''


    # 随机选择ssh 链接
    ssh_list=["qkt_ssh001","qkt_ssh002","qkt_ssh003"] 


    root = BashOperator(
        task_id='root',
        bash_command='echo "----------开始运行root根任务----------"',
        retries=1,
        dag=dag
    )    


    dic_root = BashOperator(
        task_id='dic_root',
        bash_command='echo "----------开始运行dic根任务----------"',
        retries=1,
        dag=dag
    )

    live_root = BashOperator(
        task_id='live_root',
        bash_command='echo "----------开始运行live根任务----------"',
        retries=1,
        dag=dag
    )


################################################################################## ods层 ##################################################################################



################################################################################## (dic) ################################################################################

    # mysqlreader——hivewriter的替换
    ods_qkt_bpm_app_k3flow_df_json = mr_hw_json.replace("(mysql_db)","bpm").replace("(mysql_query)","select id,FlowID,FlowStatus,FlowModelID,ApplyID,ApplyName,DeptID,DeptName,OrgID,OrgName,FlowName,StartDate,EndDate,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12,date13,date14,date15,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,Number1,Number2,Number3,Number4,Number5,Number6,Number7,Number8,Number9,Number10,Number11,Number12,Number13,Number14,Number15,Number16,Number17,Number18,Number19,Number20,Number21,Number22,Number23,Number24,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,Description,BackSucess,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string51,string52,string53,string54,string55,string56,string57,string58,string59,string60,string61,string62,string63,string64,string65,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,string91,string92,string93,string94,string95,string96,string97,string98,string99,string100,bool11,bool12,bool13,bool14,bool15,bool16,bool17,bool18,bool19,bool20,text1,oFlowModelID,ApplyAcc,ErpMsgID,VoucherID,CheckID,text2,text3,text4,text5,cash,budget,cashflow,zdbh,cashflow2,cashflow1,GUID,PrintCount,Office1,FileType,FileSize,Number25,Number26,Number27,Number28,Number29,Number30,Number31,Number32,Number33,Number34,Number35,Number36,Number37,Number38,Number39,Number40,Number41,Number42,Number43,Number44,Number45,backSql,'\${pre1_date}' as d from App_K3Flow").replace("(hive_db)","ods").replace("(hive_table)","ods_qkt_bpm_app_k3flow_df").replace("(hive_partition)","d").replace("(hive_column)","id,FlowID,FlowStatus,FlowModelID,ApplyID,ApplyName,DeptID,DeptName,OrgID,OrgName,FlowName,StartDate,EndDate,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12,date13,date14,date15,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,Number1,Number2,Number3,Number4,Number5,Number6,Number7,Number8,Number9,Number10,Number11,Number12,Number13,Number14,Number15,Number16,Number17,Number18,Number19,Number20,Number21,Number22,Number23,Number24,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,Description,BackSucess,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string51,string52,string53,string54,string55,string56,string57,string58,string59,string60,string61,string62,string63,string64,string65,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,string91,string92,string93,string94,string95,string96,string97,string98,string99,string100,bool11,bool12,bool13,bool14,bool15,bool16,bool17,bool18,bool19,bool20,text1,oFlowModelID,ApplyAcc,ErpMsgID,VoucherID,CheckID,text2,text3,text4,text5,cash,budget,cashflow,zdbh,cashflow2,cashflow1,GUID,PrintCount,Office1,FileType,FileSize,Number25,Number26,Number27,Number28,Number29,Number30,Number31,Number32,Number33,Number34,Number35,Number36,Number37,Number38,Number39,Number40,Number41,Number42,Number43,Number44,Number45,backSql,d").replace("(datax_json_file)","ods_qkt_bpm_app_k3flow_df")




    ods_qkt_bpm_app_k3flow_df = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='ods_qkt_bpm_app_k3flow_df',
        command=ods_qkt_bpm_app_k3flow_df_json,
        retries=1,
        dag=dag
    )



################################################################################## (live) ################################################################################

    
################################################################################## task_t3 ################################################################################
    

    # hivereader-mysqlwriter的替换
    hive_mysql_json = hr_mw_json.replace("(hive_query)","").replace("(mysql_column)","").replace("(mysql_db)","").replace("(mysql_table)","").replace("(mysql_presql)","").replace("(datax_json_file)","")



     
    # t3 = BashOperator(
    #     task_id='t3',
    #     bash_command='eyiwg',
    #     dag=dag
    # )
     

################################################################################## dim层 ##################################################################################


################################################################################## dwd层 ##################################################################################


################################################################################## dws层 ##################################################################################


################################################################################## pre层 ##################################################################################


################################################################################## ads层 ##################################################################################


################################################################################## 依赖任务 ##################################################################################
    

    root >> [dic_root,live_root]

    
    dic_root >> ods_qkt_bpm_app_k3flow_df



    # live_root >> t3


   
