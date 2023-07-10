#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 小车故障收敛规则后信息表v5
#-- 注意 ： 每天增量分区，按照天和项目分区
#-- 输入表 : dwd.dwd_agv_breakdown_detail_incre_dt,dwd_agv_working_status_incre_dt,dwd_rcs_agv_job_history_info_di
#-- 输出表 ：dwd.dwd_agv_breakdown_astringe_v5_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-22 CREATE 

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


with tmp_agv_breakdown_astringe_str1 as ( -- 获取初步过滤出的故障数据
select 
agv_code,
error_code,
error_name,
error_display_name,
error_level,
breakdown_id,
speed,
bucket_id,
warehouse_id,
mileage,
point_codes,
point_x,
point_y,
breakdown_log_time,
breakdown_collect_time,
agv_type_id,
agv_type_code,
agv_type_name,
project_code,
project_name,
first_classification,
error_code_list,
error_code_position,
error_code_0_position_list
from 
${dwd_dbname}.dwd_agv_breakdown_detail_incre_dt
where d >= date_sub('${pre1_date}',7)
and nvl(error_name,'')<>'' 
and error_level >=3
),
tmp_agv_breakdown_astringe_str2 as ( -- 获取小车任务状态变更数据
select 
agv_code,
null as error_code,
'recov' as error_name,
null as error_display_name,
null as error_level,
null as breakdown_id,
null as speed,
null as bucket_id,
null as warehouse_id,
null as mileage,
null as point_codes,
null as point_x,
null as point_y,
status_log_time as breakdown_log_time,
null as breakdown_collect_time,
null as agv_type_id,
null as agv_type_code,
null as agv_type_name,
project_code,
null as project_name,
null as first_classification,
null as error_code_list,
null as error_code_position,
null as error_code_0_position_list
from 
${dwd_dbname}.dwd_agv_working_status_incre_dt
where d >= date_sub('${pre1_date}',7)
and online_status='REGISTERED' 
and working_status='BUSY'

union all
select 
agv_code,
null as error_code,
'recov' as error_name,
null as error_display_name,
null as error_level,
null as breakdown_id,
null as speed,
null as bucket_id,
null as warehouse_id,
null as mileage,
null as point_codes,
null as point_x,
null as point_y,
job_accept_time as breakdown_log_time,
null as breakdown_collect_time,
null as agv_type_id,
null as agv_type_code,
null as agv_type_name,
project_code,
null as project_name,
null as first_classification,
null as error_code_list,
null as error_code_position,
null as error_code_0_position_list
from 
${dwd_dbname}.dwd_rcs_agv_job_history_info_di 
where d >= date_sub('${pre1_date}',7) and pt in (select project_code from ${dim_dbname}.dim_collection_project_record_ful where is_nonetwork = 1)
)
-- tmp_agv_breakdown_astringe_str3 as ( -- 获取出小车错误的开始和结束时间行组数
insert overwrite table ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di partition(d,pt)
select
agv_code,
error_code,
error_name,
error_display_name,
error_level,
breakdown_id,
speed,
bucket_id,
warehouse_id,
mileage,
point_codes,
point_x,
point_y,
breakdown_log_time,
breakdown_collect_time,
agv_type_id,
agv_type_code,
agv_type_name,
project_code,
project_name,
first_classification,
error_code_list,
error_code_position,
error_code_0_position_list,
lag_breakdown_log_time as breakdown_end_time,
substr(breakdown_log_time,1,10) as d,
project_code as pt
from 
(
select 
*,
lead(breakdown_log_time,1,breakdown_log_time) over(partition by agv_code,project_code order by breakdown_log_time asc,error_code desc) as lag_breakdown_log_time
from 
(
select 
*,
lag(error_name,1) over(partition by agv_code,project_code order by breakdown_log_time asc,error_code desc) as lag_error_name
from 
(
select 
* 
from 
tmp_agv_breakdown_astringe_str1

union all
select 
*
from 
tmp_agv_breakdown_astringe_str2
) a 
order by agv_code,project_code,breakdown_log_time
) b
where 
((b.error_name='recov' and b.lag_error_name<>'recov') or (b.error_name<>'recov' and b.lag_error_name='recov')) 
--b.project_code='A51199' and agv_code='BT172161215'
) c
where c.error_name<>'recov' and c.breakdown_log_time <> c.lag_breakdown_log_time
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

