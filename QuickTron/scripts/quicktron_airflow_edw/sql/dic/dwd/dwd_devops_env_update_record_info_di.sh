#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： devops环境更新记录流水表
#-- 注意 ： 每天T-1增量数据
#-- 输入表 : ods.ods_qkt_devops_env_update_record_di、dwd_devops_env_deploy_record_info_df、dwd_devops_user_info_df
#-- 输出表 : dwd.dwd_devops_env_update_record_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-03 CREATE 
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


insert overwrite table ${dwd_dbname}.dwd_devops_env_update_record_info_di partition(d='${pre1_date}')
select 
a.user_id,
c.user_cname,
c.dingding_id,
a.env_id,
b.env_deploy_name,
b.server_master_ip,
b.server_slave_ip,
a.status as env_update_status,
a.submit_time,
if(a.error_info='null',null,a.error_info) as error_info
from 
${ods_dbname}.ods_qkt_devops_env_update_record_di a 
left join ${dwd_dbname}.dwd_devops_env_deploy_record_info_df b on a.env_id=b.id and b.d='${pre1_date}'
left join ${dwd_dbname}.dwd_devops_user_info_df c on a.user_id=c.id and c.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

