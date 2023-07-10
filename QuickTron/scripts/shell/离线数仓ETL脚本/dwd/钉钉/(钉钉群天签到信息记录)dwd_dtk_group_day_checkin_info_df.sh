#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉群签到信息表
#-- 注意 ： 每天全量分区
#-- 输入表 : ods.ods_qkt_dtk_group_checkin_df,dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_group_day_checkin_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-07-14 CREATE 

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
tmp_dbname=tmp
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

with tmp_dtk_group_day_checkin_str1 as (
select 
user_id as emp_id,
substr(checkin_time,1,10) as checkin_date,
collect_list(checkin_time) as checkin_times,
collect_list(detail_place) as checkin_detail_places,
collect_list(REGEXP_REPLACE(remark,'\r\n|\t','')) as remarks,
cast(count(1) as int) as days_checkin_number
from 
(
select
*
from
${ods_dbname}.ods_qkt_dtk_group_checkin_df
where d='${pre1_date}'
order by checkin_time asc
) t
group by user_id,substr(checkin_time,1,10)
)
insert overwrite table ${dwd_dbname}.dwd_dtk_group_day_checkin_info_df partition(d='${pre1_date}')
select 
a.emp_id,
b.emp_name,
a.checkin_date,
a.checkin_times[0] as first_checkin_time,
a.checkin_detail_places[0] as first_checkin_detail_place,
a.remarks[0] as first_checkin_remark,
if(a.days_checkin_number>1,a.checkin_times[a.days_checkin_number-1],null) as last_checkin_time,
if(a.days_checkin_number>1,a.checkin_detail_places[a.days_checkin_number-1],null) as last_checkin_detail_place,
if(a.days_checkin_number>1,a.remarks[a.days_checkin_number-1],null) as last_checkin_remark,
a.days_checkin_number
from 
tmp_dtk_group_day_checkin_str1 a 
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.emp_id=b.emp_id and b.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
