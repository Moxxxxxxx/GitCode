#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成仿真小车路径规划点数据
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_simulation_agv_path_plan_di、ods.ods_qkt_simulation_map_point_di、ods.ods_qkt_simulation_agv_job_history_di
#-- 输出表 ：dwd.dwd_simulation_agv_path_plan_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 20212-01-15 CREATE 


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

insert overwrite table ${dwd_dbname}.dwd_simulation_agv_path_plan_info_di partition(d='${pre1_date}')
select 
rt.job_point_order,
rt.action_id,
rt.agv_code,
rt.job_id,
rt.generator,
rt.log_type,
rt.current_version,
rt.service_ip,
rt.simulation_id,
rt.simulation_job_created_id,
rt.last_push_version,
rt.is_last_push,
rt.is_can_follow,
rt.way_point_code,
rt.way_point_code_direction,
get_json_object(rt1.point_detail,concat('$.',rt.way_point_code,'.x')) as current_point_x,
get_json_object(rt1.point_detail,concat('$.',rt.way_point_code,'.y')) as current_point_y,
rt.next_point_code,
get_json_object(rt1.point_detail,concat('$.',rt.next_point_code,'.x')) as next_point_x,
get_json_object(rt1.point_detail,concat('$.',rt.next_point_code,'.y')) as next_point_y,
rt.data_time
from 
(
select 
t1.*,
lead(t1.way_point_code,1,t1.way_point_code) over(partition by t1.agv_code,t1.job_id,t1.service_ip,t1.simulation_id,t1.simulation_job_created_id order by t1.data_time,t1.point_order) as next_point_code,
row_number() over(partition by t1.agv_code,t1.job_id,t1.service_ip,t1.simulation_id,t1.simulation_job_created_id order by t1.data_time,t1.point_order) as job_point_order
from 
(
select 
t.*,
if(size(split(t.way_points,','))=1,0,tp.index+1) as point_order,
tp.way_point as way_point_code,
t.direction_way_points[tp.way_point] as way_point_code_direction
from 
(
select 
a.action_id,
a.agv_id as agv_code,
a.job_id,
a.generator,
a.log_type,
a.version as current_version,
a.node_ip as service_ip,
b.simulation_id,
b.simulation_job_created_id,
a.mes_last_push_version as last_push_version,
a.mes_is_last_push as is_last_push,
a.mes_can_follow as is_can_follow,
regexp_replace(a.mes_way_points,'\\\\[|\\\\]|\\\\s+','') as way_points,
str_to_map(regexp_replace(a.mes_direction_way_points,'\\\\{|\\\\}|\\\\s+',''),',','=') as direction_way_points,
a.data_time
from 
${ods_dbname}.ods_qkt_simulation_agv_path_plan_di a
inner join ${ods_dbname}.ods_qkt_simulation_agv_job_history_di b on a.job_id=b.job_id 
and a.agv_id=b.agv_code 
and a.node_ip=b.service_ip and b.d='${pre1_date}'
where  a.job_id<>'' 
and a.job_id is not null 
and a.d='${pre1_date}'

) t
lateral view posexplode(split(way_points,',')) tp as index,way_point
) t1
) rt
inner join ${ods_dbname}.ods_qkt_simulation_map_point_df rt1 on rt.service_ip=rt1.service_ip 
and rt.simulation_id=rt1.simulation_id 
and rt.simulation_job_created_id=rt1.simulation_job_created_id 
and rt1.d='${pre1_date}'
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


