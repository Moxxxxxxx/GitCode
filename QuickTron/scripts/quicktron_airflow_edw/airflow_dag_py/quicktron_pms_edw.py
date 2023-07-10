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
    'depends_on_past': False,  # 是否依赖上一个自己的执行状态
    'execution_timeout': None,
    'retries': 1,  # 失败重试次数
    'retry_delay': datetime.timedelta(seconds=10)  # 失败重试间隔
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
            'title': 'Airflow dingding Error',
            'text': message
        },
        at_all=True
    ).execute(context)


default_args['on_failure_callback'] = failure_callback

with DAG(
        dag_id='quicktron_pms_edw',
        schedule_interval='55 02 * * *',
        start_date=pendulum.datetime(2022, 10, 31, tz="Asia/Shanghai"),
        default_args=default_args,
        catchup=False,  # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
        concurrency=12,  # 设置DAG的最大运行 task任务数
        # dagrun_timeout=datetime.timedelta(minutes=60), # 改dags最大的运行时长，超时则会报错退出
        tags=['quicktron_offline'],
) as dag:
    ################################################################################## init ##################################################################################

    # task触发任务以及任务依赖路径
    airflow_task_path = '/usr/src/app/data/task_depend/'

    # 脚本路径
    share_quick_airflow_path = '/data/quick_airflow/sql'

    live2_quick_airflow_ods = '/data/quick_airflow/sql/live2/ods/'

    live3_quick_airflow_ods = '/data/quick_airflow/sql/live3/ods/'

    dic_quick_airflow_ods = '/data/quick_airflow/sql/dic/ods/'

    # 昨日日期
    yesterday = '{{ logical_date.in_timezone("Asia/Shanghai").strftime("%Y-%m-%d") }}'

    # 随机选择ssh 链接
    ssh_list = ["qkt_ssh002", "qkt_ssh003"]


    ## 公共的task需要触发的任务函数
    def readerFileList(yesterday, file_path):
        '''
        yesterday 日期
        share_task_map = {} #key:脚本文件路径，value:脚本的名称列表（不带sh）
        file_path  需要读取的文件名

        '''

        share_list = []

        file_strs = open(airflow_task_path + file_path, 'r')

        for item in file_strs:
            share_list.append(item.strip())

        file_strs.close()
        return share_list


    # 公共任务层的脚本触发函数
    def shareTaskOperator(list_1, task_str1, list_2, map_1):
        for share_table in list_1:
            share_task_job = task_str1.replace("task", share_table)

            share_table_instance = SSHOperator(
                ssh_conn_id=random.choice(ssh_list),
                task_id=share_table,
                command=share_task_job,
                dag=dag
            )
            list_2.append(share_table_instance)

            map_1[share_table] = share_table_instance


    root = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='root',
        command='echo "----------开始运行dag根任务----------"',
        dag=dag
    )
    end = BashOperator(
        task_id='end',
        bash_command='echo "---ok--" ',
        dag=dag
    )

    # 创建dic-ods表数据源
    dic_ods_list = readerFileList(None, 'dic_ods_pms_task.txt')

    dic_ods_task = f'''sh {dic_quick_airflow_ods}task.sh {yesterday} '''

    dic_ods_task_scheduling = []

    dic_ods_task_map = {}

    shareTaskOperator(dic_ods_list, dic_ods_task, dic_ods_task_scheduling, dic_ods_task_map)

    root >> dic_ods_task_scheduling >> end


