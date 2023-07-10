#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 钉钉的快仓实施运维人员考勤记录信息
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : ods.ods_qkt_dtk_implementers_attendamce_di、dwd.dwd_share_project_base_info_df、dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_implementers_attendamce_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-30 CREATE 
#-- 2 wangziming 2022-01-07 modify 增加字段、project_code 做规则判断
#-- 3 wangziming 2022-02-10 modify 增加project_code 清洗判断逻辑
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
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

insert overwrite table ${dwd_dbname}.dwd_dtk_implementers_attendamce_di partition(d)
select 
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids, 
a.create_time,
a.finish_time, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
c.emp_name as originator_user_name, 
a.result as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
a.new_project_code as project_code, 
a.service_type, 
a.task_type_implementation, 
a.task_type_after_sales, 
a.task_type_devops, 
a.service_org, 
a.checkin, 
a.checkin_time, 
a.checkin_point, 
split(a.checkin_point,',')[0] as checkin_lon_point, 
split(a.checkin_point,',')[1] as checkin_lat_point, 
a.checkin_address, 
a.checkout, 
split(a.checkout_point,',')[0] as checkout_lon_point, 
split(a.checkout_point,',')[1] as checkout_lat_point, 
a.checkout_time,
a.checkout_point, 
a.checkout_address,
a.remarks, 
if(b.project_code is not null,'1','0') as is_project_matching,
a.project_code as old_project_code,
substr(a.create_time,1,10) as d
from 
(select *,regexp_replace(regexp_replace(regexp_replace(upper(project_code),'－|_|一|−','-'),'PCB|HJT|AGV|[^A-Z-0-9-]',''),'-$','') as new_project_code from ${ods_dbname}.ods_qkt_dtk_implementers_attendamce_di) a
left join ${dwd_dbname}.dwd_share_project_base_info_df b on a.new_project_code=b.project_code and b.d=date_sub(current_date(),1)
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and c.org_company_name='快仓实施运维平台' and c.d='${pre1_date}'
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with new_dtk_implementers_attendamce as (
select 
*,
regexp_replace(regexp_replace(regexp_replace(upper(project_code),'－|_|一|−','-'),'PCB|HJT|AGV|[^A-Z-0-9-]',''),'-$','') as new_project_code
from 
${ods_dbname}.ods_qkt_dtk_implementers_attendamce_di
where d='${pre1_date}'
),
tmp_dtk_implementers_attendamce as (
select 
*
from 
${dwd_dbname}.dwd_dtk_implementers_attendamce_di
where d in (select distinct substr(create_time,1,10) as d from new_dtk_implementers_attendamce)
)
insert overwrite table ${dwd_dbname}.dwd_dtk_implementers_attendamce_di partition(d)
select 
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
originator_user_name, 
approval_result, 
approval_status, 
approval_title, 
project_code, 
service_type, 
task_type_implementation, 
task_type_after_sales, 
task_type_devops, 
service_org, 
checkin, 
checkin_time, 
checkin_point, 
checkin_lon_point, 
checkin_lat_point, 
checkin_address, 
checkout, 
checkout_lon_point, 
checkout_lat_point,
checkout_time, 
checkout_point, 
checkout_address,
remarks, 
is_project_matching,
old_project_code,
d
from 
(
select 
*,
row_number() over(partition by process_instance_id order by flag asc) as rn
from 
(
select 
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids, 
a.create_time,
a.finish_time, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
c.emp_name as originator_user_name, 
a.result as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
a.new_project_code as project_code, 
a.service_type, 
a.task_type_implementation, 
a.task_type_after_sales, 
a.task_type_devops, 
a.service_org, 
a.checkin, 
a.checkin_time, 
a.checkin_point, 
split(a.checkin_point,',')[0] as checkin_lon_point, 
split(a.checkin_point,',')[1] as checkin_lat_point, 
a.checkin_address, 
a.checkout, 
split(a.checkout_point,',')[0] as checkout_lon_point, 
split(a.checkout_point,',')[1] as checkout_lat_point, 
a.checkout_time,
a.checkout_point, 
a.checkout_address,
a.remarks, 
if(b.project_code is not null,'1','0') as is_project_matching,
a.project_code as old_project_code,
substr(a.create_time,1,10) as d,
1 as flag
from 
new_dtk_implementers_attendamce a
left join ${dwd_dbname}.dwd_share_project_base_info_df b on a.new_project_code=b.project_code and b.d=date_sub(current_date(),1)
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and c.org_company_name='快仓实施运维平台' and c.d='${pre1_date}'

union all
select 
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
originator_user_name, 
approval_result, 
approval_status, 
approval_title, 
project_code, 
service_type, 
task_type_implementation, 
task_type_after_sales, 
task_type_devops, 
service_org, 
checkin, 
checkin_time, 
checkin_point, 
checkin_lon_point, 
checkin_lat_point, 
checkin_address, 
checkout, 
checkout_lon_point, 
checkout_lat_point,
checkout_time, 
checkout_point, 
checkout_address,
remarks, 
is_project_matching,
old_project_code,
d,
2 as flag
from 
tmp_dtk_implementers_attendamce
) t
) rt 
where rt.rn=1
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
