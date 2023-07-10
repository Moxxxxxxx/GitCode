#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉人员请假信息记录
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_leave_di
#-- 输出表 ：dwd.dwd_dtk_process_leave_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-21 CREATE 

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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_dtk_process_leave_info_df partition(d='${pre1_date}')
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid, 
result as process_result, 
status as process_status, 
title as process_title, 
applicant_name, 
leave_dept_name, 
start_time, 
end_time, 
regexp_replace(duration,'[\\\"\\s+]','') as leave_days,
leave_type, 
leave_reasons
from 
(
select
*,
row_number() over(partition by org_name,process_instance_id order by d desc) as rn
from 
${ods_dbname}.ods_qkt_dtk_process_leave_di
) t
where t.rn=1
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_dtk_process_leave_info_df partition(d='${pre1_date}')
select 
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
applicant_name, 
leave_dept_name, 
start_time, 
end_time, 
leave_days,
leave_type, 
leave_reasons
from 
(
select 
*,
row_number() over(partition by org_name,process_instance_id order by flag desc) as rn
from 
(
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid as originator_user_id, 
result as process_result, 
status as process_status, 
title as process_title, 
applicant_name, 
leave_dept_name, 
start_time, 
end_time, 
regexp_replace(duration,'[\\\"\\s+]','') as leave_days,
leave_type, 
leave_reasons,
2 as flag
from 
${ods_dbname}.ods_qkt_dtk_process_leave_di
where d='${pre1_date}'

union all
select 
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
applicant_name, 
leave_dept_name, 
start_time, 
end_time, 
leave_days,
leave_type, 
leave_reasons,
1 as flag
from 
${dwd_dbname}.dwd_dtk_process_leave_info_df
where d='${pre2_date}'
) t
) rt
where rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

