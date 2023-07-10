import datetime
import pendulum
import random
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.contrib.operators.dingding_operator import DingdingOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
        dag_id='quicktron_main_edw',
        schedule_interval='00 03 * * *',
        start_date=pendulum.datetime(2022, 10, 31, tz="Asia/Shanghai"),
        default_args=default_args,
        catchup=False,  # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
        concurrency=12,  # 设置DAG的最大运行 task任务数
        # dagrun_timeout=datetime.timedelta(minutes=60), # 改dags最大的运行时长，超时则会报错退出
        tags=['quicktron_offline'],
        params={"pre1_date": "test1"},
) as dag:
    ################################################################################## init ##################################################################################
    # task触发任务以及任务依赖路径
    airflow_task_path = '/usr/src/app/data/task_depend/'

    # 脚本路径
    share_quick_airflow_path = '/data/quick_airflow/sql'

    # 昨日日期
    yesterday = '{{ logical_date.in_timezone("Asia/Shanghai").strftime("%Y-%m-%d") }}'

    # 随机选择ssh 链接
    ssh_list = ["qkt_ssh002", "qkt_ssh003"]


    ## 公共的task需要触发的任务函数
    def readerFileList(yesterday, *file_paths):
        '''
        yesterday 日期
        share_task_map = {} #key:脚本文件路径，value:脚本的名称列表（不带sh）
        *file_paths  需要读取的文件名

        '''

        share_task_map = {}
        for file_path in file_paths:

            share_list = []

            hive_sh = ''
            file_name = os.path.basename(file_path)

            if file_name.startswith('live2_ods'):
                hive_sh = '/live2/ods/'
            elif file_name.startswith('live2_dim'):
                hive_sh = '/live2/dim/'
            elif file_name.startswith('live2_dwd'):
                hive_sh = '/live2/dwd/'
            elif file_name.startswith('live2_ads'):
                hive_sh = '/live2/ads/'
            elif file_name.startswith('live3_ods'):
                hive_sh = '/live3/ods/'
            elif file_name.startswith('live3_dim'):
                hive_sh = '/live3/dim/'
            elif file_name.startswith('live3_dwd'):
                hive_sh = '/live3/dwd/'
            elif file_name.startswith('live3_ads'):
                hive_sh = '/live3/ads/'
            elif file_name.startswith('dic_ods'):
                hive_sh = '/dic/ods/'
            elif file_name.startswith('dic_dim'):
                hive_sh = '/dic/dim/'
            elif file_name.startswith('dic_dwd'):
                hive_sh = '/dic/dwd/'
            elif file_name.startswith('dic_dws'):
                hive_sh = '/dic/dws/'
            elif file_name.startswith('dic_ads'):
                hive_sh = '/dic/ads/'

            file_strs = open(airflow_task_path + file_path, 'r', errors='ignore')

            for item in file_strs:
                if not item.startswith("reflow_"):
                    share_list.append(item.strip())
            share_task_map[hive_sh] = share_list

            file_strs.close()

        return share_task_map

        ## 公共的task需要触发的任务依赖函数


    def readerFlieMap(file_path):
        file_strs = open(airflow_task_path + file_path, 'r', errors='ignore')
        share_depend_map = {}
        for item in file_strs:
            if not item.startswith("reflow_"):
                item_kv = item.strip()
                item_list = item_kv.split(':')
                key = item_list[0]
                value = item_list[1]
                share_depend_map[key] = value
        file_strs.close()
        return share_depend_map


    # 公共任务层的脚本触发函数
    def shareTaskOperator(share_task_map):

        share_task_entriy_map = {}
        for share_table_path in share_task_map:

            share_script_path = f'''sh {share_quick_airflow_path}{share_table_path}task.sh {yesterday} '''

            for share_table in share_task_map[share_table_path]:
                share_task_job = share_script_path.replace("task", share_table)

                share_table_instance = SSHOperator(
                    ssh_conn_id=random.choice(ssh_list),
                    task_id=share_table,
                    command=share_task_job,
                    dag=dag
                )
                share_task_entriy_map[share_table] = share_table_instance
        return share_task_entriy_map


    # 公共的依赖脚本触发函数
    def shareTaskDepend(map_depend_str, map_task_entity, *args_entity_list):

        for map_key in map_depend_str:

            depend_table_list = map_depend_str[map_key].split('&')  # 依赖表的字符串列表

            if map_key in map_task_entity.keys():

                task_table_entity = map_task_entity[map_key]  # 任务表的实体对象

                depend_task_up_list = []  # 创建任务依赖表的空列表

                if len(args_entity_list) > 0:

                    for depend_table in depend_table_list:

                        for entity_map in args_entity_list:

                            if depend_table in entity_map.keys():
                                depend_task_up_list.append(entity_map[depend_table])

                depend_task_up_list >> task_table_entity


    # 公共依赖map函数
    def shareTaskOneMap(*args_entity_list):

        share_entity_map = {}
        if len(args_entity_list) > 0:
            for entity_map in args_entity_list:
                share_entity_map = share_entity_map | entity_map

        return share_entity_map


    # ExternalTaskSensor被动触发公共函数
    def shareExternalTask(list_1, list_task_scheduling, map_1):

        for share_dag in list_1:

            share_task = ExternalTaskSensor(
                task_id=share_dag,
                external_dag_id=share_dag,  # 需要等待的外部DAG id
                external_task_id='end',  # 需要等待的外部Task id
                execution_delta=datetime.timedelta(minutes=5),
                # 执行时间差，这里指定5分钟，那么当前ExternalTaskSensor会基于当前执行时间（1:05）往前倒5分钟（1:00）寻找在这个时间点已经成功执行完毕的**init.common.1d**的save_env_conf
                ## 假如「**init.common.1d**」的执行规则是「10 1 * * *」也就是每天凌晨1点10分，
                ## 那么这里可以使用「execution_date_fn」，让当前DAG等待至1点10分，
                ## 直到「**init.common.1d**」的「save_env_conf」成功执行完
                # execution_date_fn=lambda dt: dt + timedelta(minutes=5),
                timeout=7200,  # 超时时间，如果等待了600秒还未符合期望状态的外部Task，那么抛出异常进入重试
                allowed_states=['success'],  # Task允许的状态，这里只允许外部Task执行状态为'success'
                mode='reschedule',  # reschedule模式，在等待的时候，两次检查期间会sleep当前Task，节约系统开销
                # check_existence=True, # 校验外部Task是否存在，不存在立马结束等待
                dag=dag,
            )
            if share_dag != 'quicktron_live2_edw':
                list_task_scheduling.append(share_task)
            map_1[share_dag] = share_task


    root = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='root',
        command='echo "----------开始运行root根任务----------" ',
        dag=dag
    )

    dic_root = BashOperator(
        task_id='dic_root',
        bash_command='echo "----------开始运行dic根任务----------" ',
        dag=dag
    )

    live_root = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='live_root',
        command='echo "----------开始运行live根任务----------" && sh /data/quick_airflow/script/collection_dtk_monitor.sh ',
        dag=dag
    )

    trigger_dag_list = ['quicktron_live2_edw', 'quicktron_bpm_edw', 'quicktron_ctrip_edw', 'quicktron_devops_edw',
                        'quicktron_dtk_edw', 'quicktron_kde_edw', 'quicktron_ones_edw', 'quicktron_pms_edw',
                        'quicktron_report_edw', 'quicktron_sonar_edw', 'quicktron_git_edw']
    trigger_task_scheduling = []
    trigger_task_map = {}
    shareExternalTask(trigger_dag_list, trigger_task_scheduling, trigger_task_map)
    ####
    share_map_list_task = readerFileList(None, 'dic_dim_task.txt', 'dic_dwd_task.txt', 'dic_dws_task.txt',
                                         'dic_ads_task.txt', 'live2_dim_task.txt', 'live2_dwd_task.txt',
                                         'live2_ads_task.txt')  # 获取需要执行的脚本以及脚本所在的路径

    ####
    share_depend_map = readerFlieMap('dic_task_schedule.txt')  # 获取task任务之间依赖关系map字典

    ####
    share_task_entriy_map = shareTaskOperator(share_map_list_task)  # 执行脚本的sshOperator算子，以及返回算子的实体对象map字典

    ####
    share_task_map = shareTaskOneMap(share_task_entriy_map,
                                     trigger_task_map)  # ExternalTaskSensor算子实体对象与share_task_entriy_map算子实体对象合为新的公共share实体对象
    ####

    ##############################################################################################
    root >> [dic_root, live_root]

    dic_root >> trigger_task_scheduling

    live_root >> trigger_task_map['quicktron_live2_edw']

    shareTaskDepend(share_depend_map, share_task_entriy_map, share_task_map)


