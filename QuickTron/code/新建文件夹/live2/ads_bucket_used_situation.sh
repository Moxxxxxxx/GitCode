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
-------------------------------------------------------------------------------------------------------------00
-- 货架使用情况、货位使用情况 ads_bucket_used_situation 

INSERT overwrite table ${ads_dbname}.ads_bucket_used_situation
SELECT ''  as id,
       tt.d as cur_date, -- 统计日期
       tt.pt as project_code, -- 项目编码
       tt.bucket_num, -- 货架占用量
       tt1.bucket_total_num, -- 货架总量
       tt.bucket_num / tt1.bucket_total_num as bucket_num_rate, -- 货架占用率
       tt.bucket_slot_num, -- 货位占用量
       tt2.slot_total_num, -- 货位总量
       tt.bucket_slot_num / tt2.slot_total_num as bucket_slot_num_rate, -- 货位占用率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM (
     SELECT COUNT(DISTINCT bucket_code) as bucket_num,COUNT(DISTINCT bucket_slot_code) as bucket_slot_num,d,pt
     FROM  ${dwd_dbname}.dwd_inventory_transaction_info_di
     WHERE warehouse_id='1' and inventory_level='LEVEL_THREE' and d = '${pre1_date}' and sku_id is not null 
     GROUP BY d,pt
     )tt
LEFT JOIN (
     SELECT pt,COUNT(DISTINCT bucket_code) as bucket_total_num
     FROM ${dwd_dbname}.dwd_basic_bucket_info_df 
     WHERE d = '${pre1_date}'
     GROUP BY pt
     )tt1
ON tt.pt = tt1.pt
LEFT JOIN (
     SELECT pt,COUNT(DISTINCT slot_code) as slot_total_num
     FROM ${dwd_dbname}.dwd_basic_slot_base_info_df 
     WHERE d = '${pre1_date}'
     GROUP BY pt
     )tt2
ON tt.pt = tt2.pt;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"