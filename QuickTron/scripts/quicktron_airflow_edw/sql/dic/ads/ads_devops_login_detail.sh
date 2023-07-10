#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops登陆表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_user_login_record_info_di
#-- 输出表 ：ads.ads_devops_login_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-15 CREATE
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--devops登录表 ads_devops_login_detail 

INSERT overwrite table ${ads_dbname}.ads_devops_login_detail
SELECT
    user_id               as user_id,
    user_login_time       as user_login_time,
    data_time             as data_time,
    d                     as d
FROM(
  SELECT 
    user_id,
    user_login_time,
    data_time,
    d
  FROM 
    ${dwd_dbname}.dwd_devops_user_login_record_info_di
  ) t;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"