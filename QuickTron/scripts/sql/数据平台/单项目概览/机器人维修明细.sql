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
-- 机器人维修明细 ads_single_project_agv_fix_deatail 

INSERT overwrite table ${ads_dbname}.ads_single_project_agv_fix_deatail
SELECT '' as id, -- 主键
       to_date(i.inspection_start_time) as cur_date, -- 统计日期
       i.project_code, -- 项目编码
       d.agv_code, -- 机器人编码
       d.agv_type as agv_type_code, -- 机器人类型
       d.agv_type_name, -- 机器人类型名称
       i.inspection_start_time as start_fix_time, -- 开始维修时间
       i.inspection_finish_time as finish_fix_time, -- 结束维修时间
       IF(i.inspection_finish_time is null,unix_timestamp(),unix_timestamp(i.inspection_finish_time)) - unix_timestamp(i.inspection_start_time) as fix_duration, -- 维修时长
       i.inspection_detail as fix_reason, -- 维修原因
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_basic_agv_inspection_data_offline_info i
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info d
ON i.agv_uuid = d.agv_uuid AND i.project_code = d.project_code
ORDER BY i.inspection_finish_time asc -- 按结束维修时间排序 未完成优先
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi


#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_project_view_amr_fix_duration.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo