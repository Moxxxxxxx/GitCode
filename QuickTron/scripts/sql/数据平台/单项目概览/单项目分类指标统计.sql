#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
project_code=A51118


# �������������ڰ���ȡ�������ڣ����û��������ȡ��ǰʱ���ǰһ��
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- ����Ŀ����ָ��ͳ�� ads_single_project_classify_target 

with t1 as 
(
  SELECT TO_DATE(m.d) as cur_date,
         m.project_code,
         '�����˹���' as classify,
         m.agv_code as classify_value,
         COUNT(*) as num_of_times,
         row_number()over(PARTITION by TO_DATE(m.d),m.project_code order by COUNT(*) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di m
  ON c.project_code = m.pt AND m.d = '${pre1_date}'
  WHERE m.pt is not null
  GROUP BY TO_DATE(m.d),m.project_code,m.agv_code
),
t2 as 
(
  SELECT TO_DATE(m.d) as cur_date,
         m.project_code,
         '�����˹�����' as classify,
         m.error_display_name as classify_value,
         COUNT(*) as num_of_times,
         row_number()over(PARTITION by TO_DATE(m.d),m.project_code order by COUNT(*) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di m
  ON c.project_code = m.pt AND m.d = '${pre1_date}'
  WHERE m.pt is not null
  GROUP BY TO_DATE(m.d),m.project_code,m.error_display_name
),
t3 as 
(
  SELECT w.cur_date,
         w.project_code,
         'ϵͳ����' as classify,
         IF(t.property_value_map['second_category'] is null OR t.property_value_map['third_category'] is null,CONCAT(w.second_category,' : ',w.third_category),CONCAT(t.property_value_map['second_category'],' : ',t.property_value_map['third_category'])) as classify_value,
         COUNT(DISTINCT w.ticket_id) as num_of_times,
         row_number()over(PARTITION by w.cur_date,w.project_code order by COUNT(DISTINCT w.ticket_id) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  JOIN 
  (
    SELECT TO_DATE(w.d) as cur_date,
           w.project_code,
           w.second_category,
           w.third_category, 
           w.ticket_id
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
    WHERE w.d = '${pre1_date}' AND w.work_order_status != '�Ѳ���' AND w.first_category = 'ϵͳ'
  )w
  ON c.project_code = w.project_code
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v 
  ON w.ticket_id = v.field_value
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t 
  ON v.task_uuid = t.uuid
  GROUP BY w.cur_date,w.project_code,IF(t.property_value_map['second_category'] is null OR t.property_value_map['third_category'] is null,CONCAT(w.second_category,' : ',w.third_category),CONCAT(t.property_value_map['second_category'],' : ',t.property_value_map['third_category']))
)/*,
-- mock ϵͳ��������
t4 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ϵͳ����' as classify,
         '������' as classify_value,
         18 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ϵͳ����' as classify,
         '����' as classify_value,
         14 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ϵͳ����' as classify,
         '�ӿ�' as classify_value,
         10 as num_of_times,
         3 as sort
  LIMIT 5
),
-- mock ������ʱ������
t5 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '������ʱ��' as classify,
         '< 5s' as classify_value,
         6 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '������ʱ��' as classify,
         '5s - 1min' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '������ʱ��' as classify,
         '1min - 5min' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '������ʱ��' as classify,
         '> 5min' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
),
-- mock ӵ��ʱ������
t6 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ӵ��ʱ��' as classify,
         '< 5s' as classify_value,
         6 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ӵ��ʱ��' as classify,
         '5s - 1min' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ӵ��ʱ��' as classify,
         '1min - 5min' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         'ӵ��ʱ��' as classify,
         '> 5min' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
),
-- mock �˹�����ָ���ʽ�ֲ�����
t7 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '�˹�����' as classify,
         '�ļ�ͣ' as classify_value,
         7 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '�˹�����' as classify,
         'QS�ָ�' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '�˹�����' as classify,
         '���������˻ָ�' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '�˹�����' as classify,
         'others' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
)
*/

INSERT overwrite table ${ads_dbname}.ads_single_project_classify_target
SELECT '' as id, -- ����
       t1.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t1
WHERE t1.sort <= 5
UNION ALL
SELECT '' as id, -- ����
       t2.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t2
WHERE t2.sort <= 5
UNION ALL
SELECT '' as id, -- ����
       t3.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t3
WHERE t3.sort <= 5
/*
UNION ALL
SELECT '' as id, -- ����
       t4.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t4
WHERE t4.sort <= 5
UNION ALL
SELECT '' as id, -- ����
       t5.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t5
WHERE t5.sort <= 5
UNION ALL
SELECT '' as id, -- ����
       t6.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t6
WHERE t6.sort <= 5
UNION ALL
SELECT '' as id, -- ����
       t7.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t7
WHERE t7.sort <= 5
*/;
-----------------------------------------------------------------------------------------------------------------------------00

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash



#����datax����
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_project_view_breakdown_top5.json ads_project_view_error_code_top5.json ads_project_view_sys_order_ansys.json ads_project_view_sys_order_ansys_rank.json ads_project_view_manual_recovery.json ads_project_view_dead_lock_num_dis.json ads_project_view_traffic_jam_num_dis.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo