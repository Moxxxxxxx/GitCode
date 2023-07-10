#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： devops版本部署历史记录信息
#-- 注意 ： 每天T-1记录所有数据
#-- 输入表 : ods.ods_qkt_devops_project_deploy_version_di、ods.ods_qkt_devops_project_df
#-- 输出表 : dwd.dwd_devops_project_deploy_version_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-01 CREATE 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
hive=/opt/module/hive-3.1.2/bin/hive


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
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_devops_project_deploy_version_info_df partition(d='${pre1_date}')
select 
a.id, 
a.create_time, 
a.update_time, 
a.remark, 
a.new_version, 
a.old_version, 
a.single_version, 
a.for_type, 
a.status, 
a.create_user, 
a.deployment_confirm_user, 
a.evaluation_user, 
a.upgrade_reason, 
a.is_deployment, 
from_unixtime(unix_timestamp(a.deployment_time,'yyyy/MM/dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as deployment_time,
evaluation_opinion, 
evaluation_point, 
project_id, 
upper(b.project_code) as project_code,
new_version_scenario, 
single_name

from 
${ods_dbname}.ods_qkt_devops_project_deploy_version_di a
left join ${ods_dbname}.ods_qkt_devops_project_df b  on a.project_id=b.id and b.d='${pre1_date}'
where a.d='${pre1_date}'

union all
select 
id, 
create_time, 
update_time, 
remark, 
new_version, 
old_version, 
single_version, 
for_type, 
status, 
create_user, 
deployment_confirm_user, 
evaluation_user, 
upgrade_reason, 
is_deployment, 
deployment_time, 
evaluation_opinion, 
evaluation_point, 
project_id, 
project_code,
new_version_scenario, 
single_name
from 
${dwd_dbname}.dwd_devops_project_deploy_version_info_df
where d='${pre2_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
