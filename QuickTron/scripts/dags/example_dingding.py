import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.dingding_operator import DingdingOperator

with DAG(
    dag_id='test2',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2022, 7, 1, tz="Asia/Shanghai"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['test2'],
    params={"example_key": "example_value"},
) as dag:
    # shell
    s1 = BashOperator(
        task_id='s1',
        bash_command='echo 1',
    )

    # shell
    s2 = BashOperator(
        task_id='s2',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    # shell
    s3 = BashOperator(
        task_id='s3',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    # shell
    end_shell = BashOperator(
        task_id='end_shell',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    # 发送钉钉
    dd = DingdingOperator(
        task_id='dingding',
        dingding_conn_id='dingding_default',
        message_type='text',
        message='QuickFlow告警 这是测试钉钉发送',
        at_mobiles=['15083337012'],
        dag=dag,
    )
   
    # 设置流程
    s1 >> [s2,s3] >> end_shell >> dd
