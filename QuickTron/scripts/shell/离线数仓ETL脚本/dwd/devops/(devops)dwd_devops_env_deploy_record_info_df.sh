#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： devops环境部署记录流水表
#-- 注意 ： 每天T-1记录所有数据
#-- 输入表 : ods.ods_qkt_devops_env_deploy_record_di、ods.ods_qkt_devops_user_df、ods.ods_qkt_devops_asset_record_df
#-- 输出表 : dwd.dwd_devops_env_deploy_record_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-01 CREATE 
#-- 2 wangziming 2022-06-06 modify 增加部署完成时间字段
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


insert overwrite table ${dwd_dbname}.dwd_devops_env_deploy_record_info_df partition(d='${pre1_date}')
select 
a.id, 
a.create_time, 
a.update_time, 
a.remark, 
a.name as env_deploy_name, 
a.status as deploy_status, 
a.deploy_log_path, 
a.deploy_progress, 
a.dataset_id, 
a.owner_id,
b.first_name as owner_name,
a.server_master_id, 
c.ip as server_master_ip,
a.server_slave_id, 
e.ip as server_slave_ip,
a.deploy_location, 
a.for_type, 
a.product_line, 
a.version_num, 
a.error_info,
a.deployfinish_time as deploy_finish_time
from 
${ods_dbname}.ods_qkt_devops_env_deploy_record_di a
left join ${ods_dbname}.ods_qkt_devops_user_df b on a.owner_id=b.id and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_devops_asset_record_df c on a.server_master_id=c.id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_devops_asset_record_df e on a.server_slave_id=e.id and e.d='${pre1_date}'
where a.d='${pre1_date}'

union all
select 
id, 
create_time, 
update_time, 
remark, 
env_deploy_name, 
deploy_status, 
deploy_log_path, 
deploy_progress, 
dataset_id, 
owner_id,
owner_name,
server_master_id, 
server_master_ip,
server_slave_id, 
server_slave_ip,
deploy_location, 
for_type, 
product_line, 
version_num, 
error_info,
deploy_finish_time
from 
${dwd_dbname}.dwd_devops_env_deploy_record_info_df
where d='${pre2_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
