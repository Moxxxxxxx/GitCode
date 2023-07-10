#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目的机器人天最后一条机器人状态记录
#-- 注意 ： 每日全量数据合并
#-- 输入表 : dwd.dwd_phx_rms_robot_state_info_di
#-- 输出表 : dwd.dwd_phx_rms_robot_state_daily_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-21 CREATE 

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


with tmp_rms_robot_state_daily_str1 as ( -- 取出前十天每日最大的agv状态值
select 
id, 
create_time, 
robot_code, 
network_state, 
online_state, 
work_state, 
cooperate_state, 
job_sn, 
is_locked, 
robot_power, 
error_codes, 
change_cause, 
duration, 
is_error, 
project_code,
d
from 
(
select 
*,row_number() over(partition by robot_code,project_code,d order by create_time desc) as rn
from 
${dwd_dbname}.dwd_phx_rms_robot_state_info_di
where d>=date_sub('${pre1_date}',9)
) t
where t.rn=1
),
tmp_rms_robot_state_daily_str2 as ( -- 获取前十天的每天的最大状态值的合并
select 
t2.id, 
t2.create_time, 
t1.robot_code, 
t2.network_state, 
t2.online_state, 
t2.work_state, 
t2.cooperate_state, 
t2.job_sn, 
t2.is_locked, 
t2.robot_power, 
t2.error_codes, 
t2.change_cause, 
t2.duration, 
t2.is_error, 
t1.project_code,
t1.d
from 
(
select
b.robot_code,
b.project_code,
a.days as d
from 
(
select 
days
from 
${dim_dbname}.dim_day_date
where days>=date_sub('${pre1_date}',9) 
and days<='${pre1_date}'
) a 
left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df b on 1=1 and b.d='${pre1_date}'
) t1
left join tmp_rms_robot_state_daily_str1 t2 on t1.robot_code=t2.robot_code and t1.project_code=t2.project_code and t1.d=t2.d
),
tmp_rms_robot_state_daily_str3 as ( --  获取到目标表前第11天的数据
select 
b.id, 
b.create_time, 
a.robot_code, 
b.network_state, 
b.online_state, 
b.work_state, 
b.cooperate_state, 
b.job_sn, 
b.is_locked, 
b.robot_power, 
b.error_codes, 
b.change_cause, 
b.duration, 
b.is_error, 
a.project_code,
coalesce(a.d,b.d) as d
from 
(
select 
robot_code,
project_code,
null as id, 
null as create_time, 
null as network_state, 
null as online_state, 
null as work_state, 
null as cooperate_state, 
null as job_sn, 
null as is_locked, 
null as robot_power, 
null as error_codes, 
null as change_cause, 
null as duration, 
null as is_error,
'${pre1_date}' as d
from 
${dwd_dbname}.dwd_phx_basic_robot_base_info_df
where d='${pre1_date}'
) a
left join 
(
select 
id, 
create_time, 
robot_code, 
network_state, 
online_state, 
work_state, 
cooperate_state, 
job_sn, 
is_locked, 
robot_power, 
error_codes, 
change_cause, 
duration, 
is_error, 
project_code,
d
from 
${dwd_dbname}.dwd_phx_rms_robot_state_daily_info_di
where d=date_sub('${pre1_date}',10)
) b on a.robot_code=b.robot_code and a.project_code=b.project_code
)
insert overwrite table ${dwd_dbname}.dwd_phx_rms_robot_state_daily_info_di partition(d,pt)
select 
coalesce(a.id,b.id) as id, 
coalesce(a.create_time,b.create_time) as create_time,
coalesce(a.robot_code,b.robot_code) as robot_code,
coalesce(a.network_state,b.network_state) as network_state,
coalesce(a.online_state,b.online_state) as online_state,
coalesce(a.work_state,b.work_state) as work_state,
coalesce(a.cooperate_state,b.cooperate_state) as cooperate_state,
coalesce(a.job_sn,b.job_sn) as job_sn,
coalesce(a.is_locked,b.is_locked) as is_locked,
coalesce(a.robot_power,b.robot_power) as robot_power,
coalesce(a.error_codes,b.error_codes) as error_codes,
coalesce(a.change_cause,b.change_cause) as change_cause,
coalesce(a.duration,b.duration) as duration,
coalesce(a.is_error,b.is_error) as is_error,
coalesce(a.project_code,b.project_code) as project_code,
a.d,
coalesce(a.project_code,b.project_code) as pt
from 
tmp_rms_robot_state_daily_str2 a
left join (
select 
*
from 
tmp_rms_robot_state_daily_str3

union all
select 
*
from 
tmp_rms_robot_state_daily_str2
) b on a.robot_code=b.robot_code and a.project_code=b.project_code and a.d=date_add(b.d,1)
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



