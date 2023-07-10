#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： ones工单系统数据
#-- 注意 ： 每日t-1全量分区
#-- 输入表 : ods.ods_qkt_ones_work_order_info
#-- 输出表 ：dwd.dwd_ones_work_order_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-10-10 CREATE 
#-- 2 wangziming 2021-12-01 modify 增加过滤逻辑（被删除，或者测试数据）
#-- 3 wangziming 2022-01-05 modify 增加字段
#-- 4 wangziming 2022-01-14 modify case_status字段增加清洗逻辑
#-- 5 wangziming 2022-03-30 modify 增加字段 project_sys_version
#-- 6 wangziming 2022-04-20 modify 项目编码全部变成大写 

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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_ones_work_order_info_df partition(d='$pre1_date')
select 
ones_work_order_uuid,
ticket_id,
upper(project_code) as project_code,
case_origin_code,
problem_type,
case when case_status in('未响应') then concat('0-',case_status)
	 when case_status in('已响应') then concat('1-',case_status)
	 when case_status in('处理中','转研发') then concat('2-',case_status)
	 when case_status in('需复现','硬件研发：待复现') then concat('3-',case_status)
	 when case_status in('研发已处理','研发已解决','研发：恢复','研发：解决','研发：任务','硬件研发：恢复','硬件研发：解决','硬件研发：任务','硬件研发：版本升级') then concat('4-',case_status)
	 when case_status in('工单：恢复','工单：解决','工单：任务','已关闭','已驳回') then concat('6-',case_status)
	else case_status end as case_status,
status_time,
owner_name,
created_time,
close_name,
first_category,
second_category,
memo,
supplement,
dealsteps,
work_order_status,
duty_type,
feedback_tel,
contact_user,
create_user,
third_category,
second_class,
third_class,
REGEXP_EXTRACT(project_sys_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as project_sys_version
from 
(
select 
ones_work_order_uuid,
ticket_id,
project_code,
case_origin_code,
problem_type,
case_status,
status_time,
owner_name,
created_time,
close_name,
first_category,
second_category,
regexp_replace (memo,'\t|\n','') as memo,
regexp_replace(supplement,'\t|\n','') as supplement,
regexp_replace(dealsteps,'\t|\n','') as dealsteps,
work_order_status,
duty_type,
feedback_tel,
sign_delete,
contact_user,
create_user,
third_category,
second_class,
third_class,
case_sys_version as project_sys_version
from 
${ods_dbname}.ods_qkt_ones_work_order_info
where d='$pre1_date' and case_origin_code='钉钉后台' and create_user='普勇军' and  ((project_code='A51256' and to_date(created_time)>='2021-10-01') or project_code not in ('TEST001','TE-tese2','TE-test','测试','test'))

union all
select 
ones_work_order_uuid,
ticket_id,
project_code,
case_origin_code,
problem_type,
case_status,
status_time,
owner_name,
created_time,
close_name,
first_category,
second_category,
regexp_replace (memo,'\t|\n','') as memo,
regexp_replace(supplement,'\t|\n','') as supplement,
regexp_replace(dealsteps,'\t|\n','') as dealsteps,
work_order_status,
duty_type,
feedback_tel,
sign_delete,
contact_user,
create_user,
third_category,
second_class,
third_class,
case_sys_version as project_sys_version
from 
${ods_dbname}.ods_qkt_ones_work_order_info
where d='$pre1_date' and (case_origin_code<>'钉钉后台' or create_user<>'普勇军')
) t
where sign_delete=0 and project_code!='A66666'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

