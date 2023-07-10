#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- DIC项目码点 ads_carry_order_point 

INSERT overwrite table ${ads_dbname}.ads_carry_order_agv_type

SELECT '' as id, -- 主键
       t.first_classification as carry_type,
       case when t.first_classification = 'WORKBIN' then '料箱车'
            when t.first_classification = 'STOREFORKBIN' then '存储一体式'
            when t.first_classification = 'CARRIER' then '潜伏式机器人'
            when t.first_classification = 'ROLLER' then '辊筒机器人'
            when t.first_classification = 'FORKLIFT' then '堆高全向车'
            when t.first_classification = 'DELIVER' then '投递车'
            when t.first_classification = 'SC'then '四向穿梭车' 
       end as carry_type_des,
       t.project_code,
       t.agv_type_code as agv_type,
       nvl(a.agv_type,t.agv_type_code) as amr_type,
       nvl(a.agv_type_name,t.agv_type_name) as amr_type_desc,
       t.agv_code,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df t
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
ON t.project_code = a.project_code AND a.agv_code = t.agv_code
WHERE t.d = '${pre1_date}' AND (a.project_code is null OR a.active_status = '运营中')
-- 凤凰 
union all 
select 
'' as id, -- 主键
t.first_classification as carry_type,
       case when t.first_classification = 'WORKBIN' then '料箱车'
            when t.first_classification = 'STOREFORKBIN' then '存储一体式'
            when t.first_classification = 'CARRIER' then '潜伏式机器人'
            when t.first_classification = 'ROLLER' then '辊筒机器人'
            when t.first_classification = 'FORKLIFT' then '堆高全向车'
            when t.first_classification = 'DELIVER' then '投递车'
            when t.first_classification = 'SC'then '四向穿梭车' 
       end as carry_type_des,
       t.project_code,
       t.robot_type_code as agv_type,        
      t.robot_type_code as amr_type,
       t.robot_type_name as amr_type_desc,
       t.robot_code as agv_code,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
from ${dwd_dbname}.dwd_phx_basic_robot_base_info_df t
where t.d='${pre1_date}' and t.robot_usage_state ='using'

;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"