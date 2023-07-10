#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：devops环境部署时长
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_env_deploy_record_info_df
#-- 输出表 ：ads.ads_devops_envdeploy_duration
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-06-01 CREATE
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads

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
-- ads_devops_envdeploy_duration （devops环境部署时长）

INSERT overwrite table ${ads_dbname}.ads_devops_envdeploy_duration
--部署时长指标
select 
  owner_id,owner_name,env_deploy_name,server_master_ip,server_slave_ip,
  DATE_FORMAT(create_time,'yyyy-MM-dd') as deploy_start_date,
  create_time as deploy_starttime,
  deploy_finish_time as deploy_endtime,
  (unix_timestamp(deploy_finish_time) - unix_timestamp(create_time)) as deploye_duration_s,
  (unix_timestamp(deploy_finish_time) - unix_timestamp(create_time))/60 as deploye_duration_min
from 
 ( select 
 	*
 	,rank() over(partition by id order by update_time) as operation_sequence 
 from ${dwd_dbname}.dwd_devops_env_deploy_record_info_df 
 where d = '${pre1_date}'
 order by id desc) a
where operation_sequence = 1 and deploy_progress = 100;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"