#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：devops环境部署时长
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_project_deploy_version_info_df
#-- 输出表 ：ads.ads_devops_product_obtain
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
-- ads_devops_product_obtain （devops版本获取）

INSERT overwrite table ${ads_dbname}.ads_devops_product_obtain
--部署时长指标
SELECT 
  create_time as request_time,
  update_time as last_opt_time,
  new_version,old_version,
  create_user as request_user,
  project_code,
  status 
FROM ${dwd_dbname}.dwd_devops_project_deploy_version_info_df where d = '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"