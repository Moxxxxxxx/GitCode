#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 仿真环境使用分析详表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_simulation_record_info_ful
#-- 输出表 ：dws.dws_simulation_utility_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-21 CREATE
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
dws_dbname=dws

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--仿真环境使用分析详表 dws_simulation_utility_detail （仿真环境使用分析，月活，周频）

INSERT overwrite table ${dws_dbname}.dws_simulation_utility_detail
SELECT 
  simulation_id                   as simulation_id,
  simulation_type                 as simulation_type,
  simulation_sub_type             as simulation_sub_type,
  server_ip                       as server_ip,
  user_ip                         as user_ip,
  user_name						  as user_name,
  action_detail                   as action_detail,
  data_time                       as start_time,
  end_time                        as end_time,
  scene_duration_second           as duration_second
FROM 
  (
  SELECT
      uuid,
      server_ip,
      user_ip,
      CASE 
      WHEN user_name IS NULL AND data_time < '2021-12-03' AND user_ip IN ('172.31.251.251','172.31.252.251','172.31.249.172') THEN 'admin'
      ELSE user_name 
      END as user_name,
      simulation_id,
      CASE 
      WHEN simulation_type = 'MOVE' THEN '搬运业务'
      ELSE simulation_type 
      END as simulation_type,
      CASE 
      WHEN simulation_sub_type = 'CONTAINER' THEN '纯料箱搬运'
      WHEN simulation_sub_type = 'BUCKET' THEN '货架搬运(潜伏式)'
      ELSE simulation_sub_type 
      END as simulation_sub_type,
      scene_actions_concat,
      CASE 
      WHEN scene_actions_concat = 'map-create->simulation-startAndBuild' then '仿真场景参数编辑'
      WHEN scene_actions_concat IN ('simulation-startAndBuild->simulation-end','simulation-startAndBuild->simulation-finish') then '仿真场景运行'
      ELSE 'abnormal_data' END as action_detail,
      data_time,
      from_unixtime(unix_timestamp( data_time, 'yyyy-MM-dd HH:mm:ss' )+scene_duration_second,'yyyy-MM-dd HH:mm:ss') AS end_time,
      scene_duration_second 
    FROM
      ${dwd_dbname}.dwd_simulation_record_info_ful 
    WHERE
      scene_actions_concat <> 'OTHER') t
WHERE t.action_detail <> 'abnormal_data';

-----------------------------------------------------------------------------------------------------------------------------00

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"