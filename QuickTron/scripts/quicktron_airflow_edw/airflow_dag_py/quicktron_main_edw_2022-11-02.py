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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor




default_args = {
    'owner': 'wangziming',  # 拥有者名称
    'depends_on_past': False,   # 是否依赖上一个自己的执行状态
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
            'title':'Airflow dingding Error',
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
    catchup=False, # 执行DAG时，将开始时间到目前所有该执行的任务都执行，默认为True
    concurrency=12, #设置DAG的最大运行 task任务数
    #dagrun_timeout=datetime.timedelta(minutes=60), # 改dags最大的运行时长，超时则会报错退出
    tags=['quicktron_offline'],
    params={"pre1_date": "test1"},
) as dag:

################################################################################## init ##################################################################################

    # 公共任务层的脚本触发函数
    def shareTaskOperator(list_1,task_str1,list_2,map_1):
        for share_table in list_1 :

            share_task_job = task_str1.replace("task",share_table)

            share_table_instance = SSHOperator(
                    ssh_conn_id=random.choice(ssh_list),
                    task_id=share_table,
                    command=share_task_job,
                    dag=dag
                )
            list_2.append(share_table_instance)  

            map_1[share_table] = share_table_instance


    # 公共的依赖脚本触发函数
    def shareTaskDepend(map_depend_str,map_task_entity,*args_entity_list) :

        for map_key in map_depend_str : 

            depend_table_list = map_depend_str[map_key].split('&') # 依赖表的字符串列表

            task_table_entity = map_task_entity[map_key]  # 任务表的实体对象

            depend_task_up_list = [] # 创建任务依赖表的空列表


            if len(args_entity_list) > 0 :

                for depend_table in depend_table_list :
                    
                    for entity_map in args_entity_list :

                        if depend_table in entity_map.keys() :
                            depend_task_up_list.append(entity_map[depend_table])
                            
            depend_task_up_list >> task_table_entity

    # 公共依赖map函数
    def shareTaskOneMap(*args_entity_list) :

        share_entity_map = {}
        if len(args_entity_list) > 0 :
            for entity_map in args_entity_list :
                share_entity_map = share_entity_map | entity_map

        return share_entity_map

    # ExternalTaskSensor被动触发公共函数
    def shareExternalTask(list_1,list_task_scheduling,map_1) : 

        for share_dag in list_1 :

            share_task = ExternalTaskSensor(
                                        task_id=share_dag,
                                        external_dag_id=share_dag, # 需要等待的外部DAG id
                                        external_task_id='end', # 需要等待的外部Task id
                                        execution_delta=datetime.timedelta(minutes=5), # 执行时间差，这里指定5分钟，那么当前ExternalTaskSensor会基于当前执行时间（1:05）往前倒5分钟（1:00）寻找在这个时间点已经成功执行完毕的**init.common.1d**的save_env_conf
                                        ## 假如「**init.common.1d**」的执行规则是「10 1 * * *」也就是每天凌晨1点10分，
                                        ## 那么这里可以使用「execution_date_fn」，让当前DAG等待至1点10分，
                                        ## 直到「**init.common.1d**」的「save_env_conf」成功执行完
                                        # execution_date_fn=lambda dt: dt + timedelta(minutes=5),
                                        timeout=7200, # 超时时间，如果等待了600秒还未符合期望状态的外部Task，那么抛出异常进入重试
                                        allowed_states=['success'], # Task允许的状态，这里只允许外部Task执行状态为'success'
                                        mode='reschedule', # reschedule模式，在等待的时候，两次检查期间会sleep当前Task，节约系统开销
                                        #check_existence=True, # 校验外部Task是否存在，不存在立马结束等待
                                        dag=dag,
                                        )
            if share_dag != 'quicktron_live2_edw' :
                list_task_scheduling.append(share_task)
            map_1[share_dag] = share_task



    # 脚本路径
    #quick_airflow_shell=/data/quick_airflow/shell
    live_quick_airflow_ods = '/data/quick_airflow/sql/live/ods/'

    live_quick_airflow_dim = '/data/quick_airflow/sql/live/dim/'

    live_quick_airflow_dwd = '/data/quick_airflow/sql/live/dwd/'

    live_quick_airflow_ads = '/data/quick_airflow/sql/live/ads/'


    dic_quick_airflow_ods = '/data/quick_airflow/sql/dic/ods/'

    dic_quick_airflow_dim = '/data/quick_airflow/sql/dic/dim/'

    dic_quick_airflow_dwd = '/data/quick_airflow/sql/dic/dwd/'

    dic_quick_airflow_dws = '/data/quick_airflow/sql/dic/dws/'

    dic_quick_airflow_ads = '/data/quick_airflow/sql/dic/ads/'



    # 昨日日期
    #yesterday = "{{ yesterday_ds }}"
    yesterday = '{{ logical_date.in_timezone("Asia/Shanghai").strftime("%Y-%m-%d") }}'


    # 随机选择ssh 链接
    ssh_list=["qkt_ssh002","qkt_ssh003"] 



    root = SSHOperator(
        ssh_conn_id=random.choice(ssh_list),
        task_id='root',
        command='echo "----------开始运行root根任务----------" && sh /data/quick_airflow/script/qucikflow.sh ',
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


    trigger_dag_list = ['quicktron_live2_edw','quicktron_bpm_edw','quicktron_ctrip_edw','quicktron_devops_edw','quicktron_dtk_edw','quicktron_kde_edw','quicktron_ones_edw','quicktron_pms_edw','quicktron_report_edw','quicktron_sonar_edw','quicktron_git_edw']

    trigger_task_scheduling = []

    trigger_task_map = {}


    shareExternalTask(trigger_dag_list,trigger_task_scheduling,trigger_task_map)

################################################################################## (dic)-开始 ############################################################################
################################################################################## dim层 ##################################################################################
    #创建dic-dim的依赖关系（key->dim表，value->依赖表）
    dic_dim_map = {'dim_collection_project_record_ful':'quicktron_dtk_edw','dim_bpm_ud_spm_mapping_info_ful':'quicktron_bpm_edw','dim_dtk_emp_org_mapping_info':'quicktron_dtk_edw&dim_dtk_org_role_info_offline&dim_dtk_org_level_info','dim_dtk_org_history_info_df':'quicktron_dtk_edw&dim_dtk_org_level_info','dim_dtk_org_info':'quicktron_dtk_edw','dim_dtk_org_level_info':'quicktron_dtk_edw&dim_dtk_org_role_info_offline','dim_git_auth_user':'dim_git_used_author_offine&quicktron_ones_edw&quicktron_git_edw','dim_kde_bd_material_group_info_ful':'quicktron_kde_edw','dim_kde_bd_project_info':'quicktron_kde_edw','dim_kde_bd_rate_info_ful':'quicktron_kde_edw','dim_ones_field_info':'quicktron_ones_edw','dim_ones_issue_type':'quicktron_ones_edw','dim_ones_org_info':'quicktron_ones_edw','dim_ones_org_ralation_info':'quicktron_ones_edw','dim_ones_project_field_info':'quicktron_ones_edw','dim_ones_project_field_option_value_info':'quicktron_ones_edw','dim_ones_sprint_info':'quicktron_ones_edw&ods_qkt_ones_sprint_df','dim_ones_task_status':'quicktron_ones_edw','dim_ones_user_org_info':'quicktron_ones_edw','dim_report_dashboard_slices_info':'quicktron_report_edw','dim_report_dashboard_user_info':'quicktron_report_edw'}    
    #创建live-dim表数据源
    dic_dim_list = ['dim_collection_project_record_ful','dim_bpm_ud_spm_mapping_info_ful','dim_dtk_emp_org_mapping_info','dim_dtk_org_history_info_df','dim_dtk_org_info','dim_dtk_org_level_info','dim_git_auth_user','dim_kde_bd_material_group_info_ful','dim_kde_bd_project_info','dim_kde_bd_rate_info_ful','dim_ones_field_info','dim_ones_issue_type','dim_ones_org_info','dim_ones_org_ralation_info','dim_ones_project_field_info','dim_ones_project_field_option_value_info','dim_ones_sprint_info','dim_ones_task_status','dim_ones_user_org_info','dim_report_dashboard_slices_info','dim_report_dashboard_user_info']

    dic_dim_task =f'''sh {dic_quick_airflow_dim}task.sh '''

    dic_dim_task_scheduling=[] 

    dic_dim_task_map={} 

    shareTaskOperator(dic_dim_list,dic_dim_task,dic_dim_task_scheduling,dic_dim_task_map)


################################################################################## dwd层 ##################################################################################
    #创建dic_dwd的依赖关系（key->dwd表，value->依赖表）
    dic_dwd_map = {'dwd_kde_material_final_balance_info_ful':'quicktron_kde_edw','dwd_bpm_app_k3flow_info_ful':'quicktron_bpm_edw','dwd_bpm_online_report_milestone_info_ful':'quicktron_bpm_edw','dwd_bpm_final_verification_report_milestone_info_ful':'quicktron_bpm_edw','dwd_bpm_equipment_arrival_confirmation_milestone_info_ful':'quicktron_bpm_edw','dwd_bpm_external_project_handover_info_ful':'quicktron_bpm_edw','dwd_bpm_technical_scheme_review_info_ful':'quicktron_bpm_edw','dwd_bpm_contract_review_info_ful':'quicktron_bpm_edw','dwd_bpm_supplementary_contract_review_info_ful':'quicktron_bpm_edw','dwd_bpm_external_project_pre_apply_info_ful':'quicktron_bpm_edw','dwd_bpm_project_delivery_approval_info_ful':'quicktron_bpm_edw','dwd_bpm_materials_purchase_request_info_ful':'quicktron_bpm_edw','dwd_bpm_purchase_request_change_info_ful':'quicktron_bpm_edw','dwd_bpm_personal_expense_account_info_ful':'quicktron_bpm_edw','dwd_bpm_project_suspend_apply_info_ful':'quicktron_bpm_edw','dwd_bpm_personal_expense_account_item_info_ful':'quicktron_bpm_edw','dwd_bpm_app_K3flowentry_info_ful':'quicktron_bpm_edw','dwd_bpm_es_flow_info_ful':'quicktron_bpm_edw','dwd_bpm_es_ganttchart_info_df':'quicktron_bpm_edw','dwd_bpm_project_info_ful':'quicktron_bpm_edw','dwd_bpm_ud_former_project_info_ful':'quicktron_bpm_edw','dwd_ctrip_flight_account_check_info_di':'quicktron_ctrip_edw','dwd_ctrip_hotel_account_check_info_di':'quicktron_ctrip_edw','dwd_devops_asset_record_info_df':'quicktron_devops_edw','dwd_devops_env_deploy_record_info_df':'quicktron_devops_edw','dwd_devops_env_update_record_info_di':'quicktron_devops_edw&dwd_devops_env_deploy_record_info_df&dwd_devops_user_info_df','dwd_devops_project_deploy_version_info_df':'quicktron_devops_edw','dwd_devops_project_info_df':'quicktron_devops_edw','dwd_devops_scenario_record_info_di':'quicktron_devops_edw','dwd_devops_user_info_df':'quicktron_devops_edw','dwd_devops_user_login_record_info_di':'quicktron_devops_edw','dwd_dtk_attendance_info_di':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_emp_attendance_checkin_day_info_di':'dwd_dtk_group_day_checkin_info_di&dwd_dtk_emp_attendance_day_info_di','dwd_dtk_emp_attendance_day_info_di':'dim_dtk_emp_job_number_mapping_info&quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_emp_info_df':'quicktron_dtk_edw&dim_dtk_org_level_info&dim_dtk_org_info','dwd_dtk_emp_org_change_info_df':'dwd_dtk_emp_info_df&dim_dtk_org_level_info','dwd_dtk_emp_org_history_mapping_info_df':'dim_dtk_emp_org_mapping_info&dim_dtk_org_history_info_df&quicktron_dtk_edw','dwd_dtk_group_day_checkin_info_di':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_implementers_attendamce_di':'quicktron_dtk_edw&dwd_share_project_base_info_df&dwd_dtk_emp_info_df','dwd_dtk_org_change_info_df':'dim_dtk_org_history_info_df','dwd_dtk_process_attendance_business_dayily_info_df':'dwd_dtk_process_attendance_business_info_df','dwd_dtk_process_attendance_business_info_df':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_process_business_travel_dayily_info_df':'dwd_dtk_process_business_travel_df','dwd_dtk_process_business_travel_df':'quicktron_dtk_edw&dwd_share_project_base_info_df&dwd_dtk_emp_info_df','dwd_dtk_process_leave_dayily_info_df':'dwd_dtk_process_leave_info_df','dwd_dtk_process_leave_info_df':'quicktron_dtk_edw','dwd_dtk_process_maintenance_log_info_df':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_process_operation_record_info_df':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_process_pe_log_info_df':'quicktron_dtk_edw&dim_dtk_org_level_info&dwd_dtk_emp_info_df','dwd_dtk_process_work_for_home_dayily_info_df':'dwd_dtk_process_work_for_home_info_df','dwd_dtk_process_work_for_home_info_df':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_process_work_overtime_dayily_info_df':'dwd_dtk_process_work_overtime_info_df','dwd_dtk_process_work_overtime_info_df':'quicktron_dtk_edw','dwd_dtk_process_work_request_info_df':'quicktron_dtk_edw&dwd_dtk_emp_info_df','dwd_dtk_special_labor_approval_manhour_info_ful':'quicktron_dtk_edw','dwd_dtk_special_labor_approval_process_info_ful':'quicktron_dtk_edw','dwd_dtk_version_evaluation_info_df':'quicktron_dtk_edw','dwd_git_app_git_stats_info_da':'quicktron_git_edw&dim_git_used_author_offine','dwd_git_commit_detail_info_da':'quicktron_git_edw&dim_git_used_author_offine','dwd_kde_bd_material_info_df':'quicktron_kde_edw','dwd_kde_pur_mrb_entry_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_kde_pur_mrb_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_kde_pur_poorder_entry_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_kde_pur_poorder_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_kde_sal_outstock_entry_info_df':'quicktron_kde_edw&dwd_kde_sal_outstock_info_df','dwd_kde_sal_outstock_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_kde_sal_returnstock_entry_info_df':'quicktron_kde_edw&dwd_kde_sal_returnstock_info_df','dwd_kde_sal_returnstock_info_df':'quicktron_kde_edw&dim_kde_bd_project_info','dwd_one_task_process_change_info_his':'dwd_ones_task_message_info_di&dwd_ones_org_user_info_ful&','dwd_ones_bug_detail_info_df':'quicktron_ones_edw','dwd_ones_org_user_info_ful':'quicktron_ones_edw','dwd_ones_project_classify_info_ful':'quicktron_ones_edw&dim_ones_project_field_info&dim_ones_project_field_option_value_info&dim_dtk_org_level_info','dwd_ones_project_info_df':'quicktron_ones_edw','dwd_ones_task_field_value_info_ful':'quicktron_ones_edw','dwd_ones_task_info_ful':'quicktron_ones_edw&dim_ones_sprint_info&dwd_ones_project_classify_info_ful&dim_dtk_org_level_info&dwd_one_task_process_change_info_his&dim_ones_issue_type&dim_ones_task_status&dwd_ones_org_user_info_ful&','dwd_ones_task_manhour_info_ful':'quicktron_ones_edw&dwd_ones_task_info_ful&dwd_ones_org_user_info_ful','dwd_ones_task_message_info_di':'quicktron_ones_edw','dwd_ones_task_process_comments_change_info_his':'dwd_ones_task_message_info_di&dwd_ones_org_user_info_ful&quicktron_ones_edw','dwd_ones_testcase_field_value_info_ful':'quicktron_ones_edw','dwd_ones_testcase_info_ful':'quicktron_ones_edw','dwd_ones_work_order_change_record_df':'quicktron_ones_edw','dwd_ones_work_order_info_df':'quicktron_ones_edw','dwd_pms_project_emp_log_info_df':'quicktron_dtk_edw&dim_bpm_ud_spm_mapping_info_ful&quicktron_pms_edw&dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df','dwd_pms_share_project_base_info_df':'quicktron_pms_edw&quicktron_dtk_edw&dwd_bpm_equipment_arrival_confirmation_milestone_info_ful&dwd_share_project_base_info_df&dim_bpm_ud_spm_mapping_info_ful&quicktron_bpm_edw&dwd_dtk_version_evaluation_info_df','dwd_report_action_log_info_da':'quicktron_report_edw&dim_report_dashboard_user_info&dim_report_dashboard_slices_info','dwd_share_project_base_info_df':'quicktron_ones_edw&quicktron_dtk_edw&quicktron_bpm_edw&dwd_bpm_es_ganttchart_info_df&quicktron_live2_edw&dwd_bpm_final_verification_report_milestone_info_ful&dwd_bpm_online_report_milestone_info_ful'}

    #创建live-dwd表数据源
    dic_dwd_list = ['dwd_kde_material_final_balance_info_ful','dwd_bpm_app_k3flow_info_ful','dwd_bpm_online_report_milestone_info_ful','dwd_bpm_final_verification_report_milestone_info_ful','dwd_bpm_equipment_arrival_confirmation_milestone_info_ful','dwd_bpm_external_project_handover_info_ful','dwd_bpm_technical_scheme_review_info_ful','dwd_bpm_contract_review_info_ful','dwd_bpm_supplementary_contract_review_info_ful','dwd_bpm_external_project_pre_apply_info_ful','dwd_bpm_project_delivery_approval_info_ful','dwd_bpm_materials_purchase_request_info_ful','dwd_bpm_purchase_request_change_info_ful','dwd_bpm_personal_expense_account_info_ful','dwd_bpm_project_suspend_apply_info_ful','dwd_bpm_personal_expense_account_item_info_ful','dwd_bpm_app_K3flowentry_info_ful','dwd_bpm_es_flow_info_ful','dwd_bpm_es_ganttchart_info_df','dwd_bpm_project_info_ful','dwd_bpm_ud_former_project_info_ful','dwd_ctrip_flight_account_check_info_di','dwd_ctrip_hotel_account_check_info_di','dwd_devops_asset_record_info_df','dwd_devops_env_deploy_record_info_df','dwd_devops_env_update_record_info_di','dwd_devops_project_deploy_version_info_df','dwd_devops_project_info_df','dwd_devops_scenario_record_info_di','dwd_devops_user_info_df','dwd_devops_user_login_record_info_di','dwd_dtk_attendance_info_di','dwd_dtk_emp_attendance_checkin_day_info_di','dwd_dtk_emp_attendance_day_info_di','dwd_dtk_emp_info_df','dwd_dtk_emp_org_change_info_df','dwd_dtk_emp_org_history_mapping_info_df','dwd_dtk_group_day_checkin_info_di','dwd_dtk_implementers_attendamce_di','dwd_dtk_org_change_info_df','dwd_dtk_process_attendance_business_dayily_info_df','dwd_dtk_process_attendance_business_info_df','dwd_dtk_process_business_travel_dayily_info_df','dwd_dtk_process_business_travel_df','dwd_dtk_process_leave_dayily_info_df','dwd_dtk_process_leave_info_df','dwd_dtk_process_maintenance_log_info_df','dwd_dtk_process_operation_record_info_df','dwd_dtk_process_pe_log_info_df','dwd_dtk_process_work_for_home_dayily_info_df','dwd_dtk_process_work_for_home_info_df','dwd_dtk_process_work_overtime_dayily_info_df','dwd_dtk_process_work_overtime_info_df','dwd_dtk_process_work_request_info_df','dwd_dtk_special_labor_approval_manhour_info_ful','dwd_dtk_special_labor_approval_process_info_ful','dwd_dtk_version_evaluation_info_df','dwd_git_app_git_stats_info_da','dwd_git_commit_detail_info_da','dwd_kde_bd_material_info_df','dwd_kde_pur_mrb_entry_info_df','dwd_kde_pur_mrb_info_df','dwd_kde_pur_poorder_entry_info_df','dwd_kde_pur_poorder_info_df','dwd_kde_sal_outstock_entry_info_df','dwd_kde_sal_outstock_info_df','dwd_kde_sal_returnstock_entry_info_df','dwd_kde_sal_returnstock_info_df','dwd_one_task_process_change_info_his','dwd_ones_bug_detail_info_df','dwd_ones_org_user_info_ful','dwd_ones_project_classify_info_ful','dwd_ones_project_info_df','dwd_ones_task_field_value_info_ful','dwd_ones_task_info_ful','dwd_ones_task_manhour_info_ful','dwd_ones_task_message_info_di','dwd_ones_task_process_comments_change_info_his','dwd_ones_testcase_field_value_info_ful','dwd_ones_testcase_info_ful','dwd_ones_work_order_change_record_df','dwd_ones_work_order_info_df','dwd_pms_project_emp_log_info_df','dwd_pms_share_project_base_info_df','dwd_report_action_log_info_da','dwd_share_project_base_info_df']

    dic_dwd_task = f'''sh {dic_quick_airflow_dwd}task.sh '''

    dic_dwd_task_scheduling=[]

    dic_dwd_task_map={}

    shareTaskOperator(dic_dwd_list,dic_dwd_task,dic_dwd_task_scheduling,dic_dwd_task_map)


################################################################################## dws层 ##################################################################################
    #创建dic_dws的依赖关系（key->dws表，value->依赖表）
    dic_dws_map = {'dws_report_user_login_daycount':'dwd_report_action_log_info_da','dws_monitor_platform_auto_work_order':'dwd_ones_work_order_info_df','dws_report_dashboard_daycount':'dwd_report_action_log_info_da&dim_report_dashboard_slices_info','dws_report_slice_daycount':'dwd_report_action_log_info_da','dws_report_sql_edit_info_daycount':'dwd_report_action_log_info_da'}

    #创建dic-dws表数据源
    dic_dws_list = ['dws_report_user_login_daycount','dws_monitor_platform_auto_work_order','dws_report_dashboard_daycount','dws_report_slice_daycount','dws_report_sql_edit_info_daycount']

    dic_dws_task = f'''sh {dic_quick_airflow_dws}task.sh '''

    dic_dws_task_scheduling=[]

    dic_dws_task_map={}

    shareTaskOperator(dic_dws_list,dic_dws_task,dic_dws_task_scheduling,dic_dws_task_map)

################################################################################## ads层 ##################################################################################
    #创建dic_ads的依赖关系（key->ads表，value->依赖表）
    dic_ads_map = {'ads_project_member_effcive':'dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_dtk_org_history_info_df&dim_day_date&dwd_pms_project_emp_log_info_df&tmp_pms_project_general_view_detail&dwd_dtk_process_leave_dayily_info_df&dwd_bpm_project_suspend_apply_info_ful','tmp_pms_project_general_view_detail':'dwd_bpm_ud_former_project_info_ful&dwd_bpm_app_k3flow_info_ful&dwd_bpm_es_flow_info_ful&dwd_bpm_contract_amount_offline_info_ful&dwd_pmo_project_plan_offline_info_df&dwd_bpm_equipment_arrival_confirmation_milestone_info_ful&dwd_bpm_materials_purchase_request_info_ful&dwd_bpm_purchase_request_change_info_ful&dwd_bpm_project_delivery_approval_info_ful&dwd_bpm_app_k3flowentry_info_ful&dwd_ones_work_order_info_df&dwd_pms_share_project_base_info_df','ads_team_ft_member_issue_type':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dwd_ones_task_info_ful&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df&dwd_one_task_process_change_info_his&dim_day_date','ads_team_ft_member_issue_status':'dim_dtk_emp_org_mapping_info&dwd_dtk_emp_info_df&dim_dtk_org_level_info&dim_day_date&dwd_ones_task_info_ful&dwd_one_task_process_change_info_his','ads_ft_work_order_detail':'dim_ft_team_info_offline&dwd_ones_work_order_info_df&dwd_ones_work_order_change_record_df&dwd_pms_share_project_base_info_df&dwd_ones_task_info_ful&dwd_dtk_emp_info_df&dwd_ones_task_field_value_info_ful&dwd_one_task_process_change_info_his&dwd_dtk_emp_org_change_info_df','ads_project_healthy_info':'tmp_pms_project_general_view_detail&dwd_dtk_version_evaluation_info_df&dwd_ones_work_order_info_df&dwd_g2p_job_state_change_info&dwd_rcs_basic_agv_info&dwd_agv_working_status_incre_dt&dwd_agv_breakdown_astringe_v4_di&dwd_agv_breakdown_detail_incre_dt&dwd_notification_message_info_di&dim_sys_error_info_offline&dim_day_date&dim_day_of_second&dwd_sys_breakdown_info_df','ads_project_service_check':'dwd_dtk_implementers_attendamce_di&dwd_share_project_base_info_df','ads_project_git_detail':'dim_day_date&dwd_git_app_git_stats_info_da','ads_project_work_order_daily':'dim_ft_team_info_offline&dwd_ones_work_order_info_df&dwd_pms_share_project_base_info_df&dwd_ones_task_info_ful&dwd_ones_task_field_value_info_ful','ads_dtk_implementers_attendamce':'dwd_dtk_implementers_attendamce_di&tmp_pms_project_general_view_detail&','ads_dtk_process_business_travel':'dwd_dtk_process_business_travel_df&dwd_dtk_emp_info_df&dwd_pms_share_project_base_info_df&','ads_team_ft_virtual_ones_work_detail':'dwd_ones_task_info_ful&dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_virtual_org_emp_info_offline&dim_dtk_org_level_info&','ads_team_ft_virtual_member_git_detail':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_virtual_org_emp_info_offline&dim_dtk_org_level_info&dim_day_date&dwd_git_commit_detail_info_da&dim_git_auth_user&','ads_team_ft_virtual_member_work_efficiency':'dim_virtual_org_emp_info_offline&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dim_day_date&dwd_git_commit_detail_info_da&dwd_dtk_emp_info_df&dim_git_auth_user&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_ones_task_info_ful&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df','ads_team_ft_virtual_member_manhour_detail':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_virtual_org_emp_info_offline&dim_dtk_org_level_info&dim_day_date&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_ones_task_info_ful&dwd_pms_share_project_base_info_df&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df&dwd_dtk_process_business_travel_dayily_info_df','ads_team_ft_virtual_member_count_info':'dim_virtual_org_emp_info_offline','ads_project_service_cost':'dwd_bpm_project_info_ful&dim_bpm_material_mapping_info_offline&dwd_dtk_special_labor_approval_process_info_ful&dwd_dtk_implementers_attendamce_di&dwd_dtk_process_business_travel_df&dwd_dtk_emp_info_df&dwd_share_project_base_info_df&dwd_ones_task_info_ful&dwd_ones_org_user_info_ful&dim_dtk_emp_org_mapping_info&dwd_bpm_app_k3flow_info_ful&dwd_bpm_app_k3flowentry_info_ful&dwd_bpm_es_flow_info_ful&dwd_ones_work_order_info_df','ads_team_ft_standard_reaching_detail':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dim_day_date&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_git_app_git_stats_info_da&dim_git_auth_user','ads_team_ft_member_standard_reaching_rate':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dim_day_date&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_git_app_git_stats_info_da&dim_git_auth_user','ads_dtk_implementers_attendamce_error':'dwd_pms_share_project_base_info_df&dwd_dtk_implementers_attendamce_di','ads_dtk_process_business_travel_error':'dwd_dtk_process_business_travel_df&dwd_dtk_emp_info_df','ads_team_ft_role_member_work_efficiency':'dwd_dtk_process_business_travel_dayily_info_df&dwd_dtk_process_work_for_home_dayily_info_df&dwd_dtk_process_attendance_business_dayily_info_df&dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dim_day_date&dwd_git_commit_detail_info_da&dwd_dtk_emp_info_df&dim_git_auth_user&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_ones_task_info_ful&dwd_dtk_emp_attendance_checkin_day_info_di&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_process_work_overtime_dayily_info_df','ads_bpm_znby_ft_project_detail':'dwd_share_project_base_info_df&dwd_bpm_contract_review_info_ful&dwd_bpm_supplementary_contract_review_info_ful&dwd_bpm_external_project_handover_info_ful&dwd_bpm_external_project_pre_apply_info_ful','ads_project_work_order_new':'dim_day_date&tmp_pms_project_general_view_detail&dwd_ones_work_order_info_df','ads_project_stage_change_info':'dim_day_date&tmp_pms_project_general_view_detail','ads_ones_unusual_workhour_daily':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dim_day_date&dwd_ones_task_manhour_info_ful&dwd_ones_task_info_ful&dwd_ones_org_user_info_ful','ads_project_pe_day_month':'dim_day_date&dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_dtk_org_history_info_df&dwd_dtk_process_pe_log_info_df&dwd_bpm_project_info_ful&dwd_dtk_process_leave_dayily_info_df&dwd_share_project_base_info_df&dwd_bpm_external_project_handover_info_ful','ads_project_service_day_month':'dim_day_date&dwd_dtk_implementers_attendamce_di&dwd_share_project_base_info_df&dwd_bpm_external_project_handover_info_ful','ads_project_stage_input_person_count':'dwd_share_project_base_info_df&dwd_bpm_ud_former_project_info_ful&dwd_bpm_project_delivery_approval_info_ful&dwd_bpm_app_k3flowentry_info_ful&dwd_bpm_equipment_arrival_confirmation_milestone_info_ful&dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_dtk_org_history_info_df&dim_day_date&dwd_dtk_process_pe_log_info_df&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_implementers_attendamce_di&dwd_bpm_external_project_handover_info_ful','ads_project_ctrip_travel_detail':'dwd_ctrip_car_account_check_info_di&dwd_ctrip_flight_account_check_info_di&dwd_ctrip_hotel_account_check_info_di&dim_day_date&dwd_pms_share_project_base_info_df','ads_superset_project_used_count':'dim_day_date&dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dwd_report_action_log_info_da&dim_report_dashboard_slices_info&dim_report_dashboard_user_info','ads_superset_project_used_detail':'dwd_report_action_log_info_da&dim_report_dashboard_slices_info&dim_report_dashboard_user_info&dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info','ads_bpm_personal_expense_account_info_ful':'dwd_bpm_personal_expense_account_info_ful&dwd_bpm_personal_expense_account_item_info_ful&tmp_pms_project_general_view_detail','ads_ones_project_view':'dwd_ones_project_classify_info_ful&dwd_bpm_project_info_ful&dim_ones_sprint_info&dwd_ones_task_info_ful&dwd_one_task_process_change_info_his','ads_ones_demand_detail':'dwd_ones_project_classify_info_ful&dwd_bpm_project_info_ful&dim_ones_sprint_info&dwd_ones_task_info_ful&dwd_dtk_emp_org_history_mapping_info_df&dwd_one_task_process_change_info_his','ads_ones_bug_detail':'dwd_ones_project_classify_info_ful&dwd_bpm_project_info_ful&dim_ones_sprint_info&dwd_dtk_emp_org_history_mapping_info_df&dwd_ones_task_info_ful&dwd_one_task_process_change_info_his','ads_ones_task_process_change_detail':'dwd_ones_project_classify_info_ful&dwd_one_task_process_change_info_his&dwd_bpm_project_info_ful&dim_ones_sprint_info&dwd_ones_task_info_ful&dwd_ones_task_process_comments_change_info_his','ads_ones_bug_detail_of_member':'dwd_ones_project_classify_info_ful&dwd_bpm_project_info_ful&dim_ones_sprint_info&dwd_dtk_emp_org_history_mapping_info_df&dwd_ones_task_info_ful&dwd_one_task_process_change_info_his','ads_ones_bug_total_info':'dwd_ones_project_classify_info_ful&dwd_bpm_project_info_ful&dim_ones_sprint_info&dim_day_date&dwd_ones_task_info_ful&dwd_one_task_process_change_info_his','ads_member_work_detail':'dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_day_date&dwd_dtk_emp_attendance_day_info_di&dwd_dtk_process_business_travel_dayily_info_df&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_group_day_checkin_info_di&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_pms_share_project_base_info_df&dwd_ones_task_info_ful','ads_member_work_detail_report':'dwd_dtk_process_business_travel_dayily_info_df&dwd_dtk_process_work_for_home_dayily_info_df&dwd_dtk_process_attendance_business_dayily_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_day_date&dwd_dtk_emp_attendance_checkin_day_info_di&dwd_dtk_process_leave_dayily_info_df&dwd_ones_task_info_ful&dwd_git_commit_detail_info_da&dwd_dtk_emp_info_df&dim_git_auth_user&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful','ads_pms_project_general_view_detail':'tmp_pms_project_general_view_detail&dim_collection_project_record_ful','ads_pms_project_profit_detail':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_virtual_org_emp_info_offline&dim_dtk_org_level_info&dwd_ones_task_manhour_info_ful&dwd_ones_org_user_info_ful&dwd_ones_task_info_ful&dwd_pms_share_project_base_info_df&dwd_dtk_process_work_overtime_dayily_info_df&dwd_dtk_process_business_travel_dayily_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_dtk_org_history_info_df&dwd_dtk_process_leave_dayily_info_df&dwd_dtk_implementers_attendamce_di&dwd_pms_project_emp_log_info_df&dwd_bpm_personal_expense_account_info_ful&dwd_bpm_personal_expense_account_item_info_ful&dwd_kde_material_final_balance_info_ful&dwd_kde_sal_outstock_entry_info_df&dwd_kde_sal_returnstock_entry_info_df&dim_kde_bd_material_group_info_ful&dwd_kde_pur_poorder_entry_info_dfdwd_kde_pur_poorder_info_df&dwd_kde_pur_mrb_entry_info_df&dwd_kde_pur_poorder_entry_info_df&dwd_kde_pur_poorder_info_df&dwd_kde_bd_material_info_df&dwd_kde_pur_mrb_info_df&dwd_dtk_process_maintenance_log_info_df&dwd_ctrip_car_account_check_info_di&dwd_ctrip_flight_account_check_info_di&dwd_ctrip_hotel_account_check_info_di&dim_day_date&tmp_pms_project_general_view_detail','ads_pms_process_pe_log_detail':'dwd_dtk_emp_info_df&dwd_dtk_emp_org_history_mapping_info_df&dim_dtk_org_history_info_df&dim_day_date&dwd_pms_project_emp_log_info_df&tmp_pms_project_general_view_detail&dwd_dtk_process_leave_dayily_info_df','ads_pms_process_service_log_detail':'dwd_dtk_implementers_attendamce_di&dwd_pms_project_emp_log_info_df&tmp_pms_project_general_view_detail','ads_superset_activity_occupation':'dws_report_dashboard_daycount&dws_report_slice_daycount','ads_superset_dashboards_usage_total':'dws_report_dashboard_daycount','ads_superset_dashboards_user_usage_total':'dws_report_dashboard_daycount','ads_superset_login_activity_daily':'dws_report_user_login_daycount','ads_superset_sql_lab_activity_detail':'dwd_report_action_log_info_da','ads_superset_sql_lab_excute_trend':'dws_report_sql_edit_info_daycount','ads_superset_user_activity_detail':'dwd_report_action_log_info_da&dim_report_dashboard_slices_info&dim_report_dashboard_user_info','ads_superset_dashboards_usage_daily':'dws_report_dashboard_daycount','ads_monitor_platform_auto_work_order':'dwd_ones_work_order_info_df','ads_devops_scenario_record_detail':'dwd_devops_scenario_record_info_di','ads_devops_login_detail':'dwd_devops_user_login_record_info_di','ads_monitor_platform_error_perday':'dws_monitor_platform_auto_work_order','ads_devops_env_penetration':'dwd_devops_user_info_df&dwd_devops_env_deploy_record_info_df&dwd_devops_asset_record_info_df','ads_devops_envdeploy_duration':'dwd_devops_env_deploy_record_info_df','ads_devops_product_obtain':'dwd_devops_project_deploy_version_info_df','ads_devops_envdeploy_detail':'dwd_devops_env_deploy_record_info_df','ads_ones_manhour_dismemberment_detail':'ads_team_ft_virtual_member_manhour_detail','ads_devops_env_update':'dwd_dtk_emp_info_df&dim_dtk_emp_org_mapping_info&dim_dtk_org_level_info&dwd_dtk_emp_org_history_mapping_info_df&dwd_devops_env_update_record_info_di','ads_devops_dtk_user_info':'dwd_dtk_emp_info_df','ads_devops_dtk_org_mapping_info':'dim_dtk_org_level_info','ads_devops_project_base_detail':'dwd_bpm_app_k3flow_info_ful&dwd_bpm_app_K3flowentry_info_ful&dwd_bpm_technical_scheme_review_info_ful&dwd_bpm_project_info_ful'}

    #创建dic-ads表数据源
    dic_ads_list = ['ads_project_member_effcive','tmp_pms_project_general_view_detail','ads_team_ft_member_issue_type','ads_team_ft_member_issue_status','ads_ft_work_order_detail','ads_project_healthy_info','ads_project_service_check','ads_project_git_detail','ads_project_work_order_daily','ads_dtk_implementers_attendamce','ads_dtk_process_business_travel','ads_team_ft_virtual_ones_work_detail','ads_team_ft_virtual_member_git_detail','ads_team_ft_virtual_member_work_efficiency','ads_team_ft_virtual_member_manhour_detail','ads_team_ft_virtual_member_count_info','ads_project_service_cost','ads_team_ft_standard_reaching_detail','ads_team_ft_member_standard_reaching_rate','ads_dtk_implementers_attendamce_error','ads_dtk_process_business_travel_error','ads_team_ft_role_member_work_efficiency','ads_bpm_znby_ft_project_detail','ads_project_work_order_new','ads_project_stage_change_info','ads_ones_unusual_workhour_daily','ads_project_pe_day_month','ads_project_service_day_month','ads_project_stage_input_person_count','ads_project_ctrip_travel_detail','ads_superset_project_used_count','ads_superset_project_used_detail','ads_bpm_personal_expense_account_info_ful','ads_ones_project_view','ads_ones_demand_detail','ads_ones_bug_detail','ads_ones_task_process_change_detail','ads_ones_bug_detail_of_member','ads_ones_bug_total_info','ads_member_work_detail','ads_member_work_detail_report','ads_pms_project_general_view_detail','ads_pms_project_profit_detail','ads_pms_process_pe_log_detail','ads_pms_process_service_log_detail','ads_superset_activity_occupation','ads_superset_dashboards_usage_total','ads_superset_dashboards_user_usage_total','ads_superset_login_activity_daily','ads_superset_sql_lab_activity_detail','ads_superset_sql_lab_excute_trend','ads_superset_user_activity_detail','ads_superset_dashboards_usage_daily','ads_monitor_platform_auto_work_order','ads_devops_scenario_record_detail','ads_devops_login_detail','ads_monitor_platform_error_perday','ads_devops_env_penetration','ads_devops_envdeploy_duration','ads_devops_product_obtain','ads_devops_envdeploy_detail','ads_ones_manhour_dismemberment_detail','ads_devops_env_update','ads_devops_dtk_user_info','ads_devops_dtk_org_mapping_info','ads_devops_project_base_detail']

    dic_ads_task = f'''sh {dic_quick_airflow_ads}task.sh '''

    dic_ads_task_scheduling=[]

    dic_ads_task_map={}

    shareTaskOperator(dic_ads_list,dic_ads_task,dic_ads_task_scheduling,dic_ads_task_map)
################################################################################## (dic)-结束 #############################################################################




################################################################################## (live)-开始 ############################################################################
################################################################################## dim层 ##################################################################################
    #创建live-dim的依赖关系（key->dim表，value->依赖表）
    live_dim_map = {'dim_dsp_error_dict_v2':'quicktron_live2_edw','dim_basic_station_point_info_df':'quicktron_live2_edw','dim_basic_station_info_df':'quicktron_live2_edw','dim_rcs_agv_type':'quicktron_live2_edw','dim_cke_map_point_info_df':'quicktron_live2_edw'}

    #创建live-dim表数据源
    live_dim_list = ['dim_dsp_error_dict_v2','dim_basic_station_info_df','dim_cke_map_point_info_df','dim_rcs_agv_type','dim_basic_station_point_info_df']

    live_dim_task =f'''sh {live_quick_airflow_dim}task.sh '''

    live_dim_task_scheduling=[] 

    live_dim_task_map={} 

    shareTaskOperator(live_dim_list,live_dim_task,live_dim_task_scheduling,live_dim_task_map)

################################################################################## dwd层 ##################################################################################
    #创建live_dwd的依赖关系（key->dwd表，value->依赖表）
    live_dwd_map = {'dwd_g2p_si_qp_move_job_info': 'quicktron_live2_edw', 'dwd_basic_slot_base_info_df': 'quicktron_live2_edw', 'dwd_basic_bucket_info': 'quicktron_live2_edw', 'dwd_picking_order_detail_info': 'quicktron_live2_edw', 'dwd_g2p_picking_job_info': 'quicktron_live2_edw', 'dwd_inventory_transaction_info': 'quicktron_live2_edw', 'dwd_rcs_agv_base_info_df': 'quicktron_live2_edw', 'dwd_g2p_picking_work_detail_info': 'quicktron_live2_edw', 'dwd_agv_breakdown_astringe_v4_di': 'dwd_agv_breakdown_detail_incre_dt', 'dwd_rcs_basic_agv_type_info': 'quicktron_live2_edw', 'dwd_rcs_basic_area_info': 'quicktron_live2_edw', 'dwd_agv_breakdown_detail_incre_dt': 'quicktron_live2_edw&dim_dsp_error_dict', 'dwd_wes_basic_sku_info_df': 'quicktron_live2_edw', 'dwd_g2p_countcheck_job_info': 'ods_qkt_g2p_countcheck_job_di&ods_qkt_g2p_w2p_countcheck_job_di&ods_qkt_countcheck_w2p_countcheck_job_di', 'dwd_inventory_level3_inventory_info_df': 'quicktron_live2_edw', 'dwd_g2p_si_qp_extend_info': 'quicktron_live2_edw', 'dwd_g2p_guided_putaway_job_info': 'quicktron_live2_edw', 'dwd_agv_working_status_incre_dt': 'quicktron_live2_edw', 'dwd_picking_work_detail_info': 'quicktron_live2_edw', 'dwd_picking_order_info': 'quicktron_live2_edw', 'dwd_replenish_order_info': 'quicktron_live2_edw', 'dwd_basic_bucket_type_info': 'quicktron_live2_edw', 'dwd_rcs_basic_charger_info_df': 'quicktron_live2_edw', 'dwd_g2p_si_qp_transfer_job_info': 'quicktron_live2_edw', 'dwd_station_station_login_info': 'quicktron_live2_edw', 'dwd_sys_breakdown_info_df': 'quicktron_live2_edw', 'dwd_g2p_putaway_job_info': 'quicktron_live2_edw', 'dwd_g2p_picking_work_info': 'quicktron_live2_edw', 'dwd_basic_bucket_base_info_df': 'quicktron_live2_edw', 'dwd_g2p_bucket_robot_job_info': 'quicktron_live2_edw', 'dwd_g2p_bucket_move_job_info': 'quicktron_live2_edw', 'dwd_rcs_agv_job_history_info_di': 'quicktron_live2_edw', 'dwd_replenish_work_detail_info': 'quicktron_live2_edw', 'dwd_cyclecount_cycle_count_info': 'quicktron_live2_edw', 'dwd_station_station_entry_info': 'quicktron_live2_edw', 'dwd_g2p_countcheck_work_detail_info': 'quicktron_live2_edw', 'dwd_g2p_job_state_change_info': 'quicktron_live2_edw', 'dwd_g2p_putaway_work_detail_info': 'quicktron_live2_edw', 'dwd_rcs_basic_agv_info': 'quicktron_live2_edw', 'dwd_cyclecount_cycle_count_work_info': 'quicktron_live2_edw','dwd_notification_message_info_di':'quicktron_live2_edw'}

    #创建live-dwd表数据源
    live_dwd_list = ['dwd_rcs_agv_base_info_df','dwd_sys_breakdown_info_df','dwd_g2p_countcheck_work_detail_info','dwd_g2p_picking_work_info','dwd_g2p_putaway_work_detail_info','dwd_rcs_basic_charger_info_df','dwd_station_station_login_info','dwd_basic_bucket_type_info','dwd_agv_working_status_incre_dt','dwd_agv_breakdown_astringe_v4_di','dwd_agv_breakdown_detail_incre_dt','dwd_g2p_bucket_robot_job_info','dwd_g2p_bucket_move_job_info','dwd_g2p_guided_putaway_job_info','dwd_g2p_putaway_job_info','dwd_g2p_si_qp_move_job_info','dwd_rcs_basic_agv_type_info','dwd_rcs_basic_agv_info','dwd_inventory_transaction_info','dwd_basic_bucket_info','dwd_picking_order_detail_info','dwd_g2p_picking_work_detail_info','dwd_picking_work_detail_info','dwd_replenish_work_detail_info','dwd_basic_bucket_base_info_df','dwd_basic_slot_base_info_df','dwd_inventory_level3_inventory_info_df','dwd_wes_basic_sku_info_df','dwd_station_station_entry_info','dwd_rcs_agv_job_history_info_di','dwd_rcs_basic_area_info','dwd_picking_order_info','dwd_g2p_picking_job_info','dwd_cyclecount_cycle_count_info','dwd_cyclecount_cycle_count_work_info','dwd_g2p_countcheck_job_info','dwd_replenish_order_info','dwd_g2p_si_qp_extend_info','dwd_g2p_si_qp_transfer_job_info','dwd_g2p_job_state_change_info','dwd_notification_message_info_di']

    live_dwd_task = f'''sh {live_quick_airflow_dwd}task.sh '''

    live_dwd_task_scheduling=[]

    live_dwd_task_map={}

    shareTaskOperator(live_dwd_list,live_dwd_task,live_dwd_task_scheduling,live_dwd_task_map)

################################################################################## ads层 ##################################################################################
    #创建live_ads的依赖关系（key->ads表，value->依赖表）
    live_ads_map = {'ads_amr_breakdown':'dwd_agv_breakdown_astringe_v4_di&dwd_agv_working_status_incre_dt&dwd_rcs_agv_base_info_df&tmp_basic_agv_data_offline_info&dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_cyclecount_cycle_count_info&dwd_cyclecount_cycle_count_work_info&dwd_g2p_countcheck_job_info&dwd_replenish_order_info&dwd_g2p_guided_putaway_job_info&dwd_picking_order_info&dwd_g2p_picking_job_info&dwd_rcs_agv_job_history_info_di&dwd_g2p_bucket_robot_job_info&dwd_g2p_si_qp_extend_info&dwd_g2p_si_qp_move_job_info&dwd_g2p_si_qp_transfer_job_info&tmp_amr_mtbf_breakdown_add','tmp_amr_mtbf_breakdown_add':'dwd_agv_breakdown_astringe_v4_di&dwd_agv_working_status_incre_dt&dwd_rcs_agv_base_info_df&tmp_basic_agv_data_offline_info&dim_day_date&dim_day_of_hour&dim_collection_project_record_ful','ads_amr_breakdown_detail':'dim_collection_project_record_ful&dwd_agv_breakdown_astringe_v4_di&dwd_agv_working_status_incre_dt','ads_carry_work_reflow':'ads_carry_work_analyse_count&ads_carry_work_analyse_detail','ads_avg_type_proportion':'dwd_rcs_agv_base_info_df','ads_agv_breakdown':'dwd_agv_breakdown_astringe_v4_di&dwd_rcs_agv_base_info_df','ads_agv_breakdown_rate':'dwd_agv_breakdown_astringe_v4_di&dwd_rcs_agv_base_info_df','ads_agv_breakdown_where':'dwd_agv_breakdown_astringe_v4_di','ads_sku_ABC_checkout_distribution':'dwd_picking_order_detail_info&dwd_inventory_transaction_info&dwd_basic_bucket_info','ads_sku_ABC_linenum_distribution':'&dwd_picking_order_detail_info&dwd_inventory_transaction_info&dwd_basic_bucket_info','ads_sku_abc_bucket_rate':'dwd_picking_order_detail_info&dwd_inventory_transaction_info&dwd_basic_bucket_info','ads_bucket_used_situation':'dwd_inventory_transaction_info&dwd_basic_bucket_info&dwd_basic_slot_base_info_df','ads_single_project_abc_count_info':'dwd_picking_order_detail_info&dim_collection_project_record_ful&dwd_basic_bucket_base_info_df&dwd_basic_slot_base_info_df&dwd_inventory_level3_inventory_info_df','ads_single_project_order_statistics':'dwd_picking_order_info&dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_g2p_picking_work_detail_info&dwd_picking_work_detail_info&dwd_replenish_order_info&dwd_replenish_work_detail_info','ads_single_project_synthesis_target':'dim_collection_project_record_ful&dwd_picking_order_info&dwd_g2p_job_state_change_info&dwd_station_station_login_info&dim_day_date&dwd_basic_bucket_base_info_df&dwd_basic_slot_base_info_df&dwd_inventory_level3_inventory_info_df&dwd_wes_basic_sku_info_df&dwd_station_station_entry_info&dwd_g2p_picking_job_info&dwd_ones_work_order_info_df&dwd_ones_task_field_value_info_ful&dwd_ones_task_info_ful&tmp_basic_agv_inspection_data_offline_info&dwd_rcs_agv_job_history_info_di&dwd_rcs_basic_agv_info&tmp_basic_agv_data_offline_info&dwd_rcs_basic_charger_info_df&tmp_basic_live_data_offline_info','ads_single_project_classify_target':'dim_collection_project_record_ful&dwd_agv_breakdown_astringe_v4_di&dwd_ones_work_order_info_df&dwd_ones_task_field_value_info_ful&dwd_ones_task_info_ful','ads_single_project_agv_type_info':'dwd_agv_breakdown_astringe_v4_di&dwd_agv_working_status_incre_dt&dwd_cyclecount_cycle_count_info&dwd_cyclecount_cycle_count_work_info&dwd_g2p_countcheck_job_info&dwd_replenish_order_info&dwd_g2p_guided_putaway_job_info&dwd_picking_order_info&dwd_g2p_picking_job_info&dim_day_date&dim_collection_project_record_ful&dwd_g2p_si_qp_move_job_info&dwd_g2p_bucket_robot_job_info&dwd_g2p_si_qp_extend_info&dwd_g2p_si_qp_transfer_job_info&dwd_rcs_agv_job_history_info_di&dwd_rcs_agv_base_info_df&tmp_basic_agv_inspection_data_offline_info&tmp_basic_agv_data_offline_info&tmp_basic_live_data_offline_info','ads_single_project_agv_fix_deatail':'tmp_basic_agv_inspection_data_offline_info&tmp_basic_agv_data_offline_info','ads_single_project_equipment_detail':'tmp_basic_live_data_offline_info','ads_single_project_intelligent_handling':'dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_g2p_bucket_robot_job_info&dwd_g2p_si_qp_extend_info&dwd_g2p_si_qp_move_job_info&dwd_g2p_job_state_change_info','ads_rcs_basic_area_info':'dwd_rcs_basic_area_info','ads_carry_work_analyse_detail':'dwd_picking_order_info&dwd_g2p_picking_job_info&dwd_cyclecount_cycle_count_info&dwd_cyclecount_cycle_count_work_info&dwd_g2p_countcheck_job_info&dwd_replenish_order_info&dwd_g2p_guided_putaway_job_info&dwd_g2p_putaway_job_info&dim_collection_project_record_ful&dwd_g2p_bucket_robot_job_info&dwd_g2p_si_qp_move_job_info&dwd_g2p_si_qp_extend_info&dwd_g2p_si_qp_transfer_job_info&dwd_g2p_job_state_change_info&dwd_rcs_basic_agv_info&dwd_rcs_basic_agv_type_info','ads_carry_work_analyse_count':'dwd_g2p_bucket_move_job_info&dwd_g2p_bucket_robot_job_info&dwd_picking_order_info&dwd_g2p_picking_job_info&dwd_cyclecount_cycle_count_info&dwd_cyclecount_cycle_count_work_info&dwd_g2p_countcheck_job_info&dwd_replenish_order_info&dwd_g2p_guided_putaway_job_info&dwd_g2p_putaway_job_info&dim_collection_project_record_ful&dwd_g2p_si_qp_move_job_info&dwd_g2p_si_qp_extend_info&dwd_g2p_si_qp_transfer_job_info&dwd_rcs_basic_agv_info&dwd_rcs_basic_agv_type_info&dwd_g2p_job_state_change_info','ads_carry_order_point':'dwd_rcs_basic_area_info','ads_carry_order_agv_type':'dwd_rcs_basic_agv_info&dwd_rcs_basic_agv_type_info'}

    #创建live_ads表数据源
    live_ads_list = ['ads_amr_breakdown','tmp_amr_mtbf_breakdown_add','ads_amr_breakdown_detail','ads_carry_work_reflow','ads_avg_type_proportion','ads_agv_breakdown','ads_agv_breakdown_rate','ads_agv_breakdown_where','ads_sku_ABC_checkout_distribution','ads_sku_ABC_linenum_distribution','ads_sku_abc_bucket_rate','ads_bucket_used_situation','ads_single_project_abc_count_info','ads_single_project_order_statistics','ads_single_project_synthesis_target','ads_single_project_classify_target','ads_single_project_agv_type_info','ads_single_project_agv_fix_deatail','ads_single_project_equipment_detail','ads_single_project_intelligent_handling','ads_rcs_basic_area_info','ads_carry_work_analyse_detail','ads_carry_work_analyse_count','ads_carry_order_point','ads_carry_order_agv_type']

    live_ads_task = f'''sh {live_quick_airflow_ads}task.sh '''

    live_ads_task_scheduling=[]

    live_ads_task_map={}

    shareTaskOperator(live_ads_list,live_ads_task,live_ads_task_scheduling,live_ads_task_map)




################################################################################## (live)-结束 ############################################################################



################################################################################## 依赖任务开始 ##################################################################################

    share_task_map = shareTaskOneMap(live_dim_task_map,live_dwd_task_map,live_ads_task_map,dic_dim_task_map,dic_dwd_task_map,dic_dws_task_map,dic_ads_task_map,trigger_task_map)
    
    
    root >> [dic_root,live_root]

################################################################################## (dic-ods依赖) ##################################################################################

    dic_root >> trigger_task_scheduling

################################################################################## (dic-dim依赖) ##################################################################################
    
    shareTaskDepend(dic_dim_map,dic_dim_task_map,share_task_map)

################################################################################## (dic-dwd依赖) ##################################################################################
    shareTaskDepend(dic_dwd_map,dic_dwd_task_map,share_task_map)

################################################################################## (dic-dws依赖) ##################################################################################
    shareTaskDepend(dic_dws_map,dic_dws_task_map,share_task_map)

################################################################################## (dic-ads依赖) ##################################################################################
    shareTaskDepend(dic_ads_map,dic_ads_task_map,share_task_map)

##################################################################################---------------##################################################################################



################################################################################## (live-ods依赖) ##################################################################################

    live_root >> trigger_task_map['quicktron_live2_edw']

################################################################################## (live-dim依赖) ##################################################################################

    shareTaskDepend(live_dim_map,live_dim_task_map,share_task_map)

################################################################################## (live-dwd依赖) ##################################################################################
    
    shareTaskDepend(live_dwd_map,live_dwd_task_map,share_task_map)

################################################################################## (live-ads依赖) ##################################################################################
    shareTaskDepend(live_ads_map,live_ads_task_map,share_task_map)

################################################################################## 依赖任务结束 ##################################################################################

