-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops渗透率指标
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_user_info_df,dwd.dwd_devops_env_deploy_record_info_df
#-- 输出表 ：ads.ads_devops_env_penetration
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-06-01 CREATE
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

-- 所有用户现环境部署情况
with a as
(
select owner_id,owner_name,server_master_ip,server_slave_ip,MAX(create_time) as last_deploy_starttime,MAX(update_time) as last_deploy_endtime,d
from ${dwd_dbname}.dwd_devops_env_deploy_record_info_df  
where deploy_status =7 
GROUP BY owner_id,owner_name,server_master_ip,server_slave_ip,d
)

-- 有效用户现环境部署情况
,b as 
(
select  a.*
from a left join ${dwd_dbname}.dwd_devops_user_info_df bb on a.d = bb.d
where owner_id = id and is_active = 1
)

-- 有效且有虚拟机的用户基础信息表
,c as 
(
select aa.* 
from ${dwd_dbname}.dwd_devops_asset_record_info_df aa left join ${dwd_dbname}.dwd_devops_user_info_df bb on aa.d = bb.d
where aa.owner_id = bb.id and is_active = 1
)
INSERT overwrite table ${ads_dbname}.ads_devops_env_penetration
SELECT 
  ttl.*,'已部署过环境' as deploy_type,dpl.env_deployed_owner_qty as type_qty,env_deployed_owner_qty/vm_owner_qty as deployement_rate
FROM 
  (SELECT d,count(distinct owner_id) as vm_owner_qty FROM c group by d) ttl 
  LEFT JOIN (SELECT d,count(distinct owner_id) as env_deployed_owner_qty FROM b group by d) dpl 
  ON ttl.d = dpl.d
UNION ALL 
SELECT 
  ttl.*,'未部署过环境' as deploy_type,vm_owner_qty-dpl.env_deployed_owner_qty as type_qty,env_deployed_owner_qty/vm_owner_qty as deployement_rate
FROM 
  (SELECT d,count(distinct owner_id) as vm_owner_qty FROM c group by d) ttl 
  LEFT JOIN (SELECT d,count(distinct owner_id) as env_deployed_owner_qty FROM b group by d) dpl 
  ON ttl.d = dpl.d
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"