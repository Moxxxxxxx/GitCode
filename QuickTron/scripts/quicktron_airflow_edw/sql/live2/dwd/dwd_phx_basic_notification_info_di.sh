#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目系统告警表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_basic_notification_di
#-- 输出表 : dwd.dwd_phx_basic_notification_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-03 CREATE 
#-- 2 wangziming 2023-02-13 modify 将回流调整为15天
#-- 3 wangziming 2023-02-23 modify 增加去重
#-- 4 wangziming 2023-02-27 modify 增加x,y点的清洗逻辑，并重新初始化

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


insert overwrite table ${dwd_dbname}.dwd_phx_basic_notification_info_di partition (d,pt)
select 
id,
state, 
warehouse_id,
error_level,
error_module,
error_service,
error_type,
error_code,
param_value,
error_start_time,
error_end_time,
is_read,
job_order,
robot_code,
error_detail,
point_x,
point_y,
point_code,
robot_job,
transport_object,
error_spec,
project_code,
d,
pt
from 
(
select 
id,
state, 
warehouse_id,
alarm_level as error_level,
alarm_module as error_module,
alarm_service as error_service,
alarm_type as error_type,
error_code,
param_value,
start_time as error_start_time,
end_time as error_end_time,
is_read,
job_order,
robot_code,
regexp_replace(alarm_detail,'\\\\s+','') as error_detail,
regexp_replace(if(point_location rlike 'x=',split(split(point_location,',')[0],'=')[1],null),'\\\\)|\\\\(','') as point_x,
regexp_replace(if(point_location rlike 'y=',split(split(point_location,',')[1],'=')[1],null),'\\\\)|\\\\(','') as point_y,
if(point_location rlike 'pointCode=',regexp_replace(split(split(point_location,',')[2],'=')[1],'\\\\)',''),null) as point_code,
robot_job,
transport_object,
warning_spec as error_spec,
project_code,
substr(start_time,1,10) as d,
project_code as pt,
row_number() over(partition by id,project_code order by update_time desc) as rn 
from 
${ods_dbname}.ods_qkt_phx_basic_notification_di
where d>=date_sub('${pre1_date}',15)
and substr(start_time,1,10)>=date_sub('${pre1_date}',15)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



