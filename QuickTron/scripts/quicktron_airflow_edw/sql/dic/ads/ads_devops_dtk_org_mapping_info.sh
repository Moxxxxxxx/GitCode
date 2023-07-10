#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops用户信息表
#-- 注意 ： 
#-- 输入表 : dim.dim_dtk_org_level_info
#-- 输出表 ：ads.ads_devops_dtk_org_mapping_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-08-17 CREATE
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
--devops用户信息表 ads_devops_dtk_org_mapping_info 

INSERT overwrite table ${ads_dbname}.ads_devops_dtk_org_mapping_info
select 
  org_id
  ,org_level_num
  ,org_name
  ,split(org_path_id,'/')[size(split(org_path_id,'/'))-2] as parent_org_id
  ,split(org_path_name,'/')[size(split(org_path_name,'/'))-2] as parent_org_name
  ,org_path_id
  ,org_path_name
  ,org_id_1
  ,org_name_1
  ,org_id_2
  ,org_name_2
  ,org_id_3
  ,org_name_3
  ,org_id_4
  ,org_name_4
  ,org_id_5
  ,org_name_5
  ,org_id_6
  ,org_name_6
from 
  ${dim_dbname}.dim_dtk_org_level_info
where 
  org_company_name = '上海快仓智能科技有限公司'
  and is_valid = 1;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"