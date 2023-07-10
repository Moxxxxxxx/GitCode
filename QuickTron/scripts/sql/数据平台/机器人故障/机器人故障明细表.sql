#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# �������������ڰ���ȡ�������ڣ����û��������ȡ��ǰʱ���ǰһ��
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- �����˹�����ϸ ads_amr_breakdown_detail 

INSERT overwrite table ${ads_dbname}.ads_amr_breakdown_detail partition(d,pt)
SELECT '' as id, -- ����
       NULL as data_time, -- ͳ��Сʱ
       bd.project_code, -- ��Ŀ����
       bd.error_time as happen_time, -- ���ϴ���ʱ��
       bd.first_classification_desc as carr_type_des, -- �����˴���
       nvl(a.agv_type,bd.agv_type_code) as amr_type, -- ���������ͱ���
       nvl(a.agv_type_name,bd.agv_type_name) as amr_type_des, -- ��������������
       bd.agv_code as amr_code, -- �����˱���
       bd.error_level, -- ���ϵȼ�
       bd.error_display_name as error_des, -- ��������
       bd.error_name as error_code, -- ���ϱ���
       NULL as error_module, -- ����ģ��
       bd.error_end_time as end_time, -- ���Ͻ���ʱ��
       bd.breakdown_duration as error_duration, -- ����ʱ��
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(bd.error_time,1,10) as d,
       bd.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
)pt
JOIN 
(
  SELECT t.*,
         coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2]) as error_end_time,
         unix_timestamp(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2])) - unix_timestamp(t.error_time) as breakdown_duration
  FROM 
  (
    SELECT tt1.*,
           ROW_NUMBER() over (PARTITION by tt1.project_code,tt1.agv_code,tt1.breakdown_id,tt1.d order by tt2.status_change_time asc) as rk,
           sort_array(ARRAY(tt1.next_error_time, tt2.status_change_time,concat(date_add(to_date(tt1.error_time), 1), ' ', '00:00:00'))) as sort_time,
           tt2.status_change_time
    FROM 
    (
      SELECT b.project_code,
             b.breakdown_log_time as error_time,
             lead(b.breakdown_log_time, 1) over (PARTITION by b.project_code,b.agv_code,to_date(b.breakdown_log_time) order by b.breakdown_log_time asc) as next_error_time,
             b.agv_code,
             b.agv_type_code,
             b.agv_type_name,
             b.breakdown_id,
             b.error_code,
             b.error_name,
             b.error_display_name,
             b.error_level,
             b.d,
             case when b.first_classification = 'WORKBIN' then '���䳵'
                  when b.first_classification = 'STOREFORKBIN' then '�洢һ��ʽ'
                  when b.first_classification = 'CARRIER' then 'Ǳ��ʽ������'
                  when b.first_classification = 'ROLLER' then '��Ͳ������'
                  when b.first_classification = 'FORKLIFT' then '�Ѹ�ȫ��'
                  when b.first_classification = 'DELIVER' then 'Ͷ�ݳ�'
                  when b.first_classification = 'SC'then '������' 
             end as first_classification_desc
      FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
      WHERE b.d = '${pre1_date}' AND b.error_level >= '3' 
    )tt1
    LEFT JOIN 
    (
      SELECT w.project_code,
             w.agv_code,
             w.status_log_time as status_change_time,
             w.working_status,
             w.online_status,
             w.d
      FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt w
      WHERE 1 = 1 AND w.d = '${pre1_date}' AND w.online_status = 'REGISTERED' AND w.working_status = 'BUSY' 
    ) tt2 
    ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code AND tt2.d = tt1.d
    WHERE tt2.status_change_time > tt1.error_time
  )t
  WHERE t.rk = 1
)bd
ON pt.project_code = bd.project_code
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
ON bd.project_code = a.project_code AND a.agv_code = bd.agv_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "
