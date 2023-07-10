#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉群签到信息表
#-- 注意 ： 每天全量分区
#-- 输入表 : ods.ods_qkt_dtk_group_checkin_di,dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_group_day_checkin_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-07-14 CREATE 
#-- 2 wangziming 2022-08-31 modify 修改签到逻辑（凌晨七点之前都属于前一天的签到记录，签到日也属于前一天的分区内），故回流两天数据，并重新初始化数据
#-- 3 wangziming 2022-08-31 modify 修改全量分区为增量分区
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
checkin_date,
collect_list(checkin_time) as checkin_times,
collect_list(detail_place) as checkin_detail_places,
collect_list(remark) as remarks,
max(rn) as days_checkin_number
from 
(
select 
*
from 
(
select 
if(hour(checkin_time)<7 or (hour(checkin_time)=7 and minute(checkin_time)=0 and second(checkin_time)=0) ,date_sub(substr(checkin_time,1,10),1),substr(checkin_time,1,10)) as checkin_date,
checkin_time, 
image_list,
detail_place,
REGEXP_REPLACE(remark,'\r|\n|\t','') as remark, 
user_id, 
place, 
longitude, 
latitude, 
visit_user,
row_number() over(partition by user_id,if(hour(checkin_time)<7 or (hour(checkin_time)=7 and minute(checkin_time)=0 and second(checkin_time)=0) ,date_sub(substr(checkin_time,1,10),1),substr(checkin_time,1,10)) order by checkin_time asc) as rn,
row_number() over(partition by user_id,if(hour(checkin_time)<7 or (hour(checkin_time)=7 and minute(checkin_time)=0 and second(checkin_time)=0) ,date_sub(substr(checkin_time,1,10),1),substr(checkin_time,1,10)) order by checkin_time desc) as rn1
from 
${ods_dbname}.ods_qkt_dtk_group_checkin_di
where d='${pre1_date}'
order by user_id,checkin_time asc
) t
where rn=1 or rn1=1
) t1
group by user_id,checkin_date
),

tmp_dtk_group_day_checkin_str2 as (
select 
*
from 
${dwd_dbname}.dwd_dtk_group_day_checkin_info_di
where d in (select distinct if(hour(checkin_time)<7 or (hour(checkin_time)=7 and minute(checkin_time)=0 and second(checkin_time)=0) ,date_sub(substr(checkin_time,1,10),1),substr(checkin_time,1,10)) from ${ods_dbname}.ods_qkt_dtk_group_checkin_di where d='${pre1_date}')
and d<>'${pre1_date}'
),
tmp_dtk_group_day_checkin_str3 as (
select 
*
from 
tmp_dtk_group_day_checkin_str1
where checkin_date<>substr(checkin_times[0],1,10)
)

insert overwrite table ${dwd_dbname}.dwd_dtk_group_day_checkin_info_di partition(d)
select 
a.emp_id,
b.emp_name,
a.checkin_date,
a.checkin_times[0] as first_checkin_time,
a.checkin_detail_places[0] as first_checkin_detail_place,
a.remarks[0] as first_checkin_remark,
a.checkin_times[1] as last_checkin_time,
a.checkin_detail_places[1] as last_checkin_detail_place,
a.remarks[1] as last_checkin_remark,
a.days_checkin_number,
a.checkin_date as d
from 
tmp_dtk_group_day_checkin_str1 a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.emp_id=b.emp_id and b.d='${pre1_date}'


union all
select 
a.emp_id,
a.emp_name,
a.checkin_date,
a.first_checkin_time,
a.first_checkin_detail_place,
a.first_checkin_remark,
if(b.emp_id is not null,coalesce(b.checkin_times[1],b.checkin_times[0],a.last_checkin_time),a.last_checkin_time) as last_checkin_time,
if(b.emp_id is not null,coalesce(b.checkin_detail_places[1],b.checkin_detail_places[0],a.last_checkin_detail_place),a.last_checkin_detail_place) as last_checkin_detail_place,
if(b.emp_id is not null,coalesce(b.remarks[1],b.remarks[0],a.last_checkin_remark),a.last_checkin_remark) as last_checkin_remark,
a.days_checkin_number,
a.checkin_date as d
from 
tmp_dtk_group_day_checkin_str2 a
left join tmp_dtk_group_day_checkin_str3 b on a.emp_id=b.emp_id and a.checkin_date=b.checkin_date
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
