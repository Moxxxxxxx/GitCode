#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
project_code=A51118

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- 设备列表 ads_single_project_equipment_detail 

INSERT overwrite table ${ads_dbname}.ads_single_project_equipment_detail
SELECT '' as id, -- 主键
       b.project_code, -- 项目编码
       '工作站' as equiqment_name, -- 设备名称
       b.basic_units_qyt as equiqment_num, -- 设备数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_basic_live_data_offline_info b
WHERE b.basic_code = 0003 -- 工作站
UNION ALL
SELECT '' as id, -- 主键
       b.project_code, -- 项目编码
       '货架' as equiqment_name, -- 设备名称
       b.basic_units_qyt as equiqment_num, -- 设备数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_basic_live_data_offline_info b
WHERE b.basic_code = 0004 -- 货架
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"