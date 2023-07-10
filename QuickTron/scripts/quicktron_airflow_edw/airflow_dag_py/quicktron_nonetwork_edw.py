import datetime
import pendulum
import subprocess
import random
from airflow import DAG
import subprocess

from airflow.operators.bash import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.dingding_operator import DingdingOperator
from airflow.operators.python import BranchPythonOperator


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
    message = """### <font color=#FF0000> {0} airflow task 告警 </font> ###\n- **【DAG】** <font color=#808080
>{1}</font>\n- **【任务】** <font color=#808080>{2}</font>\n- **【原因】** <font color=#808080>{3}</font>\n
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
        dag_id='quicktron_nonetwork_edw',
        schedule_interval='00 17 * * *',
        start_date=pendulum.datetime(2022, 12, 19, tz="Asia/Shanghai"),
        default_args=default_args,
        catchup=False,  # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
        concurrency=12,  # 设置DAG的最大运行 task任务数
        # dagrun_timeout=datetime.timedelta(minutes=60), # 改dags最大的运行时长，超时则会报错退出
        tags=['quicktron_offline'],
) as dag:


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


    # 脚本路径
    live_quick_airflow_ods = '/data/quick_airflow/sql/live/ods/'

    live_quick_airflow_dwd = '/data/quick_airflow/sql/live/dwd/'

    live_quick_airflow_ads = '/data/quick_airflow/sql/live/ads/'


    # 随机选择ssh 链接
    ssh_list = ["qkt_ssh002", "qkt_ssh003"]

    # 昨日日期
    yesterday = '{{ logical_date.in_timezone("Asia/Shanghai").strftime("%Y-%m-%d") }}'
    #delta = datetime.timedelta(days=1)
    #dayb = yesterday1 - delt
    #yesterday = '{{ yesterday_ds }}'


    def sshExecution():
        (exitcode, output) = subprocess.getstatusoutput("ssh hadoop@192.168.1.80 '/opt/docker/offline-script/start-nonetwork.sh'")
        if str(output) == "1":
            return "end"
        else:
            return "next_sucess"


    root = BranchPythonOperator(
        task_id='root',
        python_callable=sshExecution,
        dag=dag,
    )

    next_sucess = BashOperator(
        task_id='next_sucess',
        bash_command='echo "next_sucess -----------------"' + yesterday,
        dag=dag
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "end -----------------"' + yesterday,
        dag=dag
    )

    reflow_str1 = f"""
    /opt/module/datax/bin/datax.py -p "-Dpre1_date='{yesterday}'" /opt/module/datax/tmp/hive_ads_lite_amr_breakdown_ck.json
    """
    reflow_ads_lite_amr_breakdown = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='reflow_ads_lite_amr_breakdown',
        command=reflow_str1,
        dag=dag
    )

    cmd_dim = f'''sh /data/quick_airflow/sql/dic/dim/dim_collection_project_record_ful.sh {yesterday} '''
    dim_collection_project_record_ful = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='dim_collection_project_record_ful',
        command=cmd_dim,
        dag=dag
    )



    ods_list_1 = ['ods_qkt_rcs_basic_agv_df', 'ods_qkt_rcs_basic_agv_type_df', 'ods_qkt_rcs_agv_job_history_di', 'ods_qkt_rcs_agv_history_job_di', 'ods_qkt_rcs_basic_charger_df', 'ods_qkt_rcs_notification_message_di', 'ods_qkt_notification_message_di', 'ods_qkt_g2p_bucket_robot_job_di', 'ods_qkt_g2p_bucket_move_job_di', 'ods_qkt_g2p_si_qp_move_job_di', 'ods_qkt_g2p_si_qp_extend_df', 'ods_qkt_g2p_si_qp_transfer_job_di', 'ods_qkt_g2p_job_state_change_da']

    # 创建ods的任务源
    live_ods_task = f'''sh {live_quick_airflow_ods}task.sh {yesterday} '''

    ods_list_scheduling = []
    ods_map_scheduling = {}


    # ods
    shareTaskOperator(ods_list_1, live_ods_task, ods_list_scheduling, ods_map_scheduling)


    dwd_list_1 = ['dwd_notification_message_info_di','dwd_rcs_basic_agv_info_df', 'dwd_rcs_basic_agv_type_info_df', 'dwd_rcs_agv_job_history_info_di', 'dwd_rcs_basic_charger_info_df', 'dwd_g2p_bucket_move_job_info_di', 'dwd_g2p_bucket_robot_job_info_di', 'dwd_g2p_si_qp_move_job_info_di', 'dwd_g2p_si_qp_transfer_job_info_di', 'dwd_agv_breakdown_astringe_v4_di', 'dwd_agv_breakdown_detail_incre_dt']

    # 创建dwd的任务源
    live_dwd_task = f'''sh {live_quick_airflow_dwd}task.sh {yesterday} '''

    dwd_list_scheduling = []

    dwd_map_scheduling = {}



    # dwd
    shareTaskOperator(dwd_list_1, live_dwd_task, dwd_list_scheduling, dwd_map_scheduling)



    ads_list_1 = ['ads_lite_amr_breakdown', 'ads_project_view_lite_amr_status', 'ads_project_view_lite_charge_pile', 'ads_project_view_lite_carry_order_count', 'reflow_ads_project_view_lite_amr_status', 'reflow_ads_project_view_lite_charge_pile', 'reflow_ads_project_view_lite_carry_order_count']

    # 创建ads的任务源
    live_ads_task = f'''sh {live_quick_airflow_ads}task.sh {yesterday} '''

    ads_list_scheduling = []

    ads_map_scheduling = {}

    # ads
    shareTaskOperator(ads_list_1, live_ads_task, ads_list_scheduling, ads_map_scheduling)




    # 依赖
    root >> [next_sucess, end]

    next_sucess >> ods_list_scheduling
    next_sucess >> dim_collection_project_record_ful

    # dwd

    ods_map_scheduling['ods_qkt_rcs_basic_agv_df'] >> dwd_map_scheduling['dwd_rcs_basic_agv_info_df']

    ods_map_scheduling['ods_qkt_rcs_basic_agv_type_df'] >> dwd_map_scheduling['dwd_rcs_basic_agv_type_info_df']


    ods_map_scheduling['ods_qkt_rcs_basic_charger_df'] >> dwd_map_scheduling['dwd_rcs_basic_charger_info_df']
    [ods_map_scheduling['ods_qkt_rcs_agv_job_history_di'], ods_map_scheduling['ods_qkt_rcs_agv_history_job_di']] >> dwd_map_scheduling['dwd_rcs_agv_job_history_info_di']

    ods_map_scheduling['ods_qkt_g2p_bucket_move_job_di'] >> dwd_map_scheduling['dwd_g2p_bucket_move_job_info_di']
    ods_map_scheduling['ods_qkt_g2p_bucket_robot_job_di'] >> dwd_map_scheduling['dwd_g2p_bucket_robot_job_info_di']
    ods_map_scheduling['ods_qkt_g2p_si_qp_move_job_di'] >> dwd_map_scheduling['dwd_g2p_si_qp_move_job_info_di']
    ods_map_scheduling['ods_qkt_g2p_si_qp_transfer_job_di'] >> dwd_map_scheduling['dwd_g2p_si_qp_transfer_job_info_di']

    dwd_map_scheduling['dwd_agv_breakdown_detail_incre_dt'] >> dwd_map_scheduling['dwd_agv_breakdown_astringe_v4_di']

    [ods_map_scheduling['ods_qkt_rcs_notification_message_di'], ods_map_scheduling['ods_qkt_notification_message_di']] >> dwd_map_scheduling['dwd_notification_message_info_di']
	
    [dwd_map_scheduling['dwd_notification_message_info_di'],dim_collection_project_record_ful] >> dwd_map_scheduling['dwd_agv_breakdown_detail_incre_dt']


    # ads
    [dim_collection_project_record_ful, dwd_map_scheduling['dwd_rcs_basic_agv_info_df'], dwd_map_scheduling['dwd_rcs_basic_agv_type_info_df'], dwd_map_scheduling['dwd_rcs_agv_job_history_info_di']] >> ads_map_scheduling['ads_project_view_lite_amr_status']

    [dim_collection_project_record_ful, dwd_map_scheduling['dwd_rcs_basic_charger_info_df'], dwd_map_scheduling['dwd_rcs_agv_job_history_info_di']] >> ads_map_scheduling['ads_project_view_lite_charge_pile']

    [dim_collection_project_record_ful, dwd_map_scheduling['dwd_g2p_bucket_move_job_info_di'], dwd_map_scheduling['dwd_g2p_bucket_robot_job_info_di'], dwd_map_scheduling['dwd_g2p_si_qp_move_job_info_di'], dwd_map_scheduling['dwd_g2p_si_qp_transfer_job_info_di']] >> ads_map_scheduling['ads_project_view_lite_carry_order_count']

    [dim_collection_project_record_ful, dwd_map_scheduling['dwd_g2p_bucket_move_job_info_di'], dwd_map_scheduling['dwd_g2p_bucket_robot_job_info_di'], dwd_map_scheduling['dwd_g2p_si_qp_move_job_info_di'], dwd_map_scheduling['dwd_g2p_si_qp_transfer_job_info_di'], dwd_map_scheduling['dwd_agv_breakdown_astringe_v4_di'], dwd_map_scheduling['dwd_rcs_agv_job_history_info_di'], dwd_map_scheduling['dwd_rcs_basic_agv_info_df'], dwd_map_scheduling['dwd_rcs_basic_agv_type_info_df']] >> ads_map_scheduling['ads_lite_amr_breakdown']

    #reflow
    ads_map_scheduling['ads_project_view_lite_amr_status'] >> ads_map_scheduling['reflow_ads_project_view_lite_amr_status']
    ads_map_scheduling['ads_project_view_lite_charge_pile'] >> ads_map_scheduling['reflow_ads_project_view_lite_charge_pile']
    ads_map_scheduling['ads_project_view_lite_carry_order_count'] >> ads_map_scheduling['reflow_ads_project_view_lite_carry_order_count']
    ads_map_scheduling['ads_lite_amr_breakdown'] >> reflow_ads_lite_amr_breakdown

