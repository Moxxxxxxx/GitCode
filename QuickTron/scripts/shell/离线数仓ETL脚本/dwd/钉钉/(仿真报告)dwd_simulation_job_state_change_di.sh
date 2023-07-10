#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成仿真场景任务状态数据
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_simulation_job_state_change_di、ods.ods_qkt_simulation_detail_di、ods.ods_qkt_simulation_bucket_robot_job_di、ods.ods_qkt_simulation_base_area_di、ods_qkt_simulation_bucket_move_job_di
#-- 输出表 ：dwd.dwd_simulation_job_state_change_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-09 CREATE 
#-- 2 wangziming 2021-12-21 modify 增加字段，修改逻辑规则
#-- 3 wangziming 2022-01-05 modify 增加表关联ods_qkt_simulation_bucket_move_job_di


# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
dws_dbname=dws
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
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_simulation_job_state_change_di partition(d='${pre1_date}')
select 
  a.id,
  a.warehouse_id,
  a.zone_code,
  a.job_id,
  a.job_type,
  a.agv_code,
  a.agv_type,
  a.state as job_state,
  a.created_app as job_created_app,
  a.created_date as job_created_time,
  a.updated_app as job_updated_app,
  a.updated_date as job_updated_time,
  a.service_ip,
  a.simulation_id,
  a.simulation_job_created_id,
  b.move_type as job_move_type,
  case when b.move_type='BUCKET'
       then (case a.state when 'WAITING_NEXTSTOP' then 1
                          when 'WAITING_AGV' then 2
                          when 'INIT_JOB' then 3
                          when 'LIFT_UP_DONE' then 4
                          when 'MOVE_BEGIN' then 5
                          when 'PUT_DOWN_DONE' then 6
                          when 'DONE' then 7
                          else 'UNKNOWN' end )
       when b.move_type='CONTAINER'
       then (case a.state when 'INIT' then 1
                          when 'INIT_JOB' then 2
                          when 'MOVE_BEGIN' then 3
                          when 'DONE' then 4
                          else 'UNKNOWN' end)
       else 'UNKNOWN' end as job_state_rn
from 
${ods_dbname}.ods_qkt_simulation_job_state_change_di a
left join ${ods_dbname}.ods_qkt_simulation_detail_di b on a.service_ip=b.service_ip and a.simulation_id=b.simulation_id and a.simulation_job_created_id=b.simulation_job_created_id and b.d='${pre1_date}'
where a.d='${pre1_date}'
;

with dwd_job as (
select * from ${dwd_dbname}.dwd_simulation_job_state_change_di 
where d='${pre1_date}'
order by job_id,job_created_time,job_state_rn
),
lateral_area as (
select 
a.area_code,
a.service_ip,
a.simulation_id,
a.simulation_job_created_id,
t.point_code
from ${ods_dbname}.ods_qkt_simulation_base_area_di a
lateral view explode(split(point_code,';')) t as point_code 
where a.d='${pre1_date}'
)
insert overwrite table ${dws_dbname}.dws_simulation_job_info_dscount partition(d='${pre1_date}')
select 
t1.job_id,
t1.agv_code,
t1.job_state_time_map,
t1.service_ip,
t1.simulation_id,
t1.simulation_job_created_id,
t1.job_type,
t1.job_move_type,
t1.job_allot_response_ms,
t1.job_allot_ms,
t1.job_transport_ms,
if(t1.start_point_code!='UNKNOWN' and t1.target_point_code !='UNKNOWN',split(concat_ws(',',t1.start_point_code,t1.target_point_code),','),null) as job_transport_routes_array,
if(t2.area_code is not null and t3.area_code is not null,split(concat_ws(',',t2.area_code,t3.area_code),','),null) as job_transport_areas_array
from 
(
select 
t.job_id,
t.agv_code,
t.service_ip,
t.simulation_id,
t.simulation_job_created_id,
t.job_type,
t.job_move_type,
t.job_state_time_map,
case when job_type='SI_BUCKET_MOVE' and nvl(job_state_time_map['LIFT_UP_DONE'],'')<>'' and nvl(job_state_time_map['WAITING_NEXTSTOP'],'')<>'' 
     then ((unix_timestamp(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[0])*1000)
     + case substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3)*100
            else 0 end) 
      - ((unix_timestamp(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[0])*1000)
      + case substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*100
            else 0 end)
     when job_type='SI_CARRY' and nvl(job_state_time_map['MOVE_BEGIN'],'')<>'' and nvl(job_state_time_map['INIT'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[0])*1000)
     + case substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['INIT'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3)*100
            else 0 end)
     when job_type='WORKBIN_MOVE' and nvl(job_state_time_map['LOAD_COMPLETED'],'')<>'' and nvl(job_state_time_map['WAITING_NEXTSTOP'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*100
            else 0 end)
     when job_type='G2P_BUCKET_MOVE' and nvl(job_state_time_map['GO_TARGET'],'')<>'' and nvl(job_state_time_map['WAITING_RESOURCE'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['GO_TARGET'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*100
            else 0 end)
     else null end as job_allot_response_ms,


case when job_type='SI_BUCKET_MOVE' and nvl(job_state_time_map['INIT_JOB'],'')<>'' and nvl(job_state_time_map['WAITING_NEXTSTOP'],'')<>'' 
     then ((unix_timestamp(split(job_state_time_map['INIT_JOB'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*100
            else 0 end)
     when job_type='SI_CARRY' and nvl(job_state_time_map['INIT_JOB'],'')<>'' and nvl(job_state_time_map['INIT'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['INIT_JOB'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['INIT'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT'],'\\\\.')[1],0,3)*100
            else 0 end)
    
     when job_type='WORKBIN_MOVE' and nvl(job_state_time_map['INIT_JOB'],'')<>'' and nvl(job_state_time_map['WAITING_NEXTSTOP'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['INIT_JOB'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_NEXTSTOP'],'\\\\.')[1],0,3)*100
            else 0 end)
     
     when job_type='G2P_BUCKET_MOVE' and nvl(job_state_time_map['INIT_JOB'],'')<>'' and nvl(job_state_time_map['WAITING_RESOURCE'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['INIT_JOB'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['INIT_JOB'],'\\\\.')[1],0,3)*100
            else 0 end)

     - ((unix_timestamp(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*100
            else 0 end)
     
     when job_type='G2P_BUCKET_MOVE' and nvl(job_state_time_map['WAITING_RESOURCE_BROKER'],'')<>'' and nvl(job_state_time_map['WAITING_RESOURCE'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['WAITING_RESOURCE_BROKER'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_RESOURCE_BROKER'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_RESOURCE_BROKER'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_RESOURCE_BROKER'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_RESOURCE_BROKER'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['WAITING_RESOURCE'],'\\\\.')[1],0,3)*100
            else 0 end)
     else null end as job_allot_ms,

case when job_type='SI_BUCKET_MOVE' and nvl(job_state_time_map['DONE'],'')<>'' and nvl(job_state_time_map['LIFT_UP_DONE'],'')<>'' 
     then ((unix_timestamp(split(job_state_time_map['DONE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['LIFT_UP_DONE'],'\\\\.')[1],0,3)*100
            else 0 end)
     
     when job_type='SI_CARRY' and nvl(job_state_time_map['DONE'],'')<>'' and nvl(job_state_time_map['MOVE_BEGIN'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['DONE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*100
            else 0 end)

     - ((unix_timestamp(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['MOVE_BEGIN'],'\\\\.')[1],0,3)*100
            else 0 end)
     
     when job_type='WORKBIN_MOVE' and nvl(job_state_time_map['DONE'],'')<>'' and nvl(job_state_time_map['LOAD_COMPLETED'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['DONE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*100
            else 0 end)

     - ((unix_timestamp(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['LOAD_COMPLETED'],'\\\\.')[1],0,3)*100
            else 0 end)
    
     when job_type='G2P_BUCKET_MOVE' and nvl(job_state_time_map['DONE'],'')<>'' and nvl(job_state_time_map['GO_TARGET'],'')<>''
     then ((unix_timestamp(split(job_state_time_map['DONE'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['DONE'],'\\\\.')[1],0,3)*100
            else 0 end)
     - ((unix_timestamp(split(job_state_time_map['GO_TARGET'],'\\\\.')[0])*1000)
     +case substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3) 
            when 3 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3) 
            when 2 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3)*10
            when 1 then substr(split(job_state_time_map['GO_TARGET'],'\\\\.')[1],0,3)*100
            else 0 end)
     else null end as job_transport_ms,
    case when t.job_type='SI_CARRY' and nvl(rt1.source_waypoint_code,'')<>'' then rt1.source_waypoint_code
         when t.job_type<>'SI_CARRY' and nvl(rt.start_point,'')<>'' then rt.start_point
        else 'UNKNOWN' end as start_point_code,
    case when t.job_type='SI_CARRY' and nvl(rt1.target_waypoint_code,'')<>'' then rt1.target_waypoint_code
         when t.job_type<>'SI_CARRY' and nvl(rt.target_point,'')<>'' then rt.target_point
        else 'UNKNOWN' end as target_point_code
from 
(
select 
job_id,
max(agv_code) as agv_code,
service_ip,
simulation_id,
simulation_job_created_id,
job_type,
job_move_type,
str_to_map(concat_ws(',',collect_list(concat_ws(':',job_state,job_updated_time))),',',':') as job_state_time_map
from 
dwd_job group by job_id,service_ip,simulation_id,simulation_job_created_id,job_type,job_move_type
) t
left join (select job_id,service_ip,simulation_id,simulation_job_created_id,start_point,target_point 
from ${ods_dbname}.ods_qkt_simulation_bucket_robot_job_di where d='${pre1_date}') rt on t.job_id=rt.job_id and t.service_ip=rt.service_ip and t.simulation_id=rt.simulation_id and t.simulation_job_created_id=rt.simulation_job_created_id
left join (select job_id,service_ip,simulation_id,simulation_job_created_id,source_waypoint_code,target_waypoint_code 
from ${ods_dbname}.ods_qkt_simulation_bucket_move_job_di where d='${pre1_date}') rt1 on t.job_id=rt1.job_id and t.service_ip=rt1.service_ip and t.simulation_id=rt1.simulation_id and t.simulation_job_created_id=rt1.simulation_job_created_id
) t1
left join lateral_area t2 on t1.start_point_code=t2.point_code and t1.service_ip=t2.service_ip and t1.simulation_id=t2.simulation_id and t1.simulation_job_created_id=t2.simulation_job_created_id 
left join lateral_area t3 on t1.target_point_code=t3.point_code and t1.service_ip=t3.service_ip and t1.simulation_id=t3.simulation_id and t1.simulation_job_created_id=t3.simulation_job_created_id 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


