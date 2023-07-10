#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目机器人故障收敛表1
#-- 注意 ： 每日按天增量分区
#-- 输入表 : dwd.dwd_phx_basic_notification_info_di、dwd.dwd_phx_basic_robot_base_info_df、dim.dim_phx_basic_error_info_ful,dim.dim_filter_error_code_offline_ful
#-- 输出表 : dwd.dwd_phx_robot_breakdown_astringe_v1_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-07 CREATE 
#-- 2 wangziming 2023-02-13 modify 将回流调整为15天
#-- 3 wangziming 2023-02-15 modify 增加故障码dim_filter_error_code_offline_ful过滤规则
#-- 4 wangziming 2023-02-27 modify 增加新的过滤逻辑
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


with tmp_error_str1 as (
select 
error_code,
error_name,
project_code
from 
${dim_dbname}.dim_phx_basic_error_info_ful
),
tmp_basic_robot_base_str1 as (
select 
robot_code,
robot_type_code,
robot_type_name,
first_classification,
second_classification,
project_code
from 
${dwd_dbname}.dwd_phx_basic_robot_base_info_df
where d='${pre1_date}'
),
tmp_robot_breakdown_str1 as (
select 
t.id,
t.state,
t.warehouse_id,
t.error_level,
t.error_module,
t.error_service,
t.error_type,
t.error_code,
b.error_name,
t.param_value,
t.error_start_time,
t.error_end_time,
t.is_read,
t.job_order,
t.robot_code,
c.robot_type_code,
c.robot_type_name,
t.error_detail,
t.point_x,
t.point_y,
t.point_code,
t.robot_job,
t.transport_object,
t.error_spec,
c.first_classification,
c.second_classification,
t.project_code,
substr(t.error_start_time,1,10) as d,
t.project_code as pt
from 
(
select 
*,
row_number() over(partition by robot_code,if(nvl(error_end_time,'')='','UNKNOWN',date_format(error_end_time,'YYYY-MM-dd HH:mm:ss')) order by error_start_time asc) as rn
from 
${dwd_dbname}.dwd_phx_basic_notification_info_di
where d>=date_sub('${pre1_date}',15) 
and error_module='robot'
and error_level>=3
) t
left join tmp_error_str1 b on t.error_code=b.error_code and t.project_code=b.project_code
left join tmp_basic_robot_base_str1 c on t.robot_code=c.robot_code and t.project_code=c.project_code
left join ${dim_dbname}.dim_filter_error_code_offline_ful e on t.project_code=e.project_code and t.error_code=e.error_code
where t.rn=1 and e.id is null
)
insert overwrite table ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di partition(d,pt)
select 
id,
state,
warehouse_id,
error_level,
error_module,
error_service,
error_type,
error_code,
error_name,
param_value,
error_start_time,
error_end_time,
is_read,
job_order,
robot_code,
robot_type_code,
robot_type_name,
error_detail,
point_x,
point_y,
point_code,
robot_job,
transport_object,
error_spec,
first_classification,
second_classification,
project_code,
d,
pt
from 
(
select
*,
sqrt(power(abs(negative(lag_point_x)+point_x),2) + power(abs(negative(lag_point_y)+point_y),2)) as xy_length
from 
(
select 
*,
lag(point_x,1,null) over(partition by robot_code,error_code,project_code order by error_start_time asc) as lag_point_x,
lag(point_y,1,null) over(partition by robot_code,error_code,project_code order by error_start_time asc) as lag_point_y,
row_number() over(partition by robot_code,error_code,project_code order by error_start_time asc) as rn
from 
tmp_robot_breakdown_str1
) t
) rt
where rn=1 or (rn<>1 and xy_length is null) or (rn<>1 and xy_length>1000)
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



