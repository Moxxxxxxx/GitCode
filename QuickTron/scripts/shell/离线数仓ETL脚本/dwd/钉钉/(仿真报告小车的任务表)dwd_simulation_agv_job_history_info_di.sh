#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成仿真场景的小车历史任务记录数据
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_simulation_agv_job_history_di、ods.ods_qkt_simulation_job_create_di、ods.ods_qkt_simulation_job_detail_di、ods.ods_qkt_simulation_sub_tasks_df、ods.ods_qkt_simulation_job_sequence_record_di
#-- 输出表 ：dwd.dwd_simulation_agv_job_history_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-25 CREATE 
#-- 2 wangziming 2021-12-27 modify 过滤掉 小车的任务完成时间大于仿真结束时间的数据
#-- 3 wangziming 2022-01-13 modify 增加表关联，增加字段
#-- 4 zhabowen 	 2022-01-17 modify 修改dwd_simulation_agv_job_history_info_di关联关系为a.robot_job_id=c.robot_job_id

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
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_simulation_agv_job_history_info_di partition(d='${pre1_date}')
select 
a.id,
a.create_time as job_created_time,
a.warehouse_id,
a.zone_code, 
a.agv_code,
a.job_accept_time,
a.job_execute_time,
a.job_finish_time,
a.job_duration,
a.job_id,
a.robot_job_id,
a.job_type,
a.job_state,
a.job_priority,
a.job_context, 
a.job_mark,
a.own_job_type,
a.src_job_type,
a.can_interrupt,
a.is_let_down,
a.is_report_event,
a.dest_point_code,
a.top_face_list,
a.bucket_id,
a.bucket_point_code,
a.action_state,
a.action_point_code,
a.agv_job_id,
a.service_ip,
a.simulation_id,
a.simulation_job_created_id,
b.created_time as simulation_start_time,
b.end_time as simulation_end_time,
if(nvl(b.end_time,'')<>'' and  nvl(b.created_time,'')<>'',unix_timestamp(b.end_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(b.created_time,'yyyy-MM-dd HH:mm:ss'),null ) as simulation_final_second,
c.sequence_times as job_sequence_times,
c.sub_task_id as job_sub_task_id,
c.action_slot_code as job_action_slot_code,
c.dest_slot_code as job_dest_slot_code,
c.container_code as job_container_code,
c.scene_type as job_scene_type,
f.sequence_name as job_sequence_name,
e.sub_job_name as job_sub_job_name,
e.sub_job_type as  job_sub_job_type,
e.sub_source_area as job_sub_source_area,
e.sub_target_area as job_sub_target_area,
e.sub_source_area_type as job_sub_source_area_type,
e.sub_target_area_type as job_sub_target_area_type,
e.transport_count as job_transport_count
from 
${ods_dbname}.ods_qkt_simulation_agv_job_history_di a
left join ${ods_dbname}.ods_qkt_simulation_job_create_di b on a.service_ip=b.service_ip and a.simulation_id=b.simulation_id and a.simulation_job_created_id=b.simulation_job_created_id and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_simulation_job_detail_di c on a.service_ip=c.service_ip and a.simulation_id=c.simulation_id and a.simulation_job_created_id=c.simulation_job_created_id and a.robot_job_id=c.robot_job_id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_simulation_sub_tasks_df  e on c.service_ip=e.service_ip and c.simulation_id=e.simulation_id and c.simulation_job_created_id=e.simulation_job_created_id and c.sub_task_id=e.id
left join ${ods_dbname}.ods_qkt_simulation_job_sequence_record_di f on c.service_ip=f.service_ip and c.simulation_id=f.simulation_id and c.simulation_job_created_id=f.simulation_job_created_id 
and c.job_sequence_id=f.job_sequence_id and c.job_sequence_record_id=f.id and f.d='${pre1_date}'
where a.d='${pre1_date}' and a.job_finish_time <= b.end_time
;


insert overwrite table ${dws_dbname}.dws_simulation_agv_use_ratio_info_dscount partition(d='${pre1_date}')
select 
agv_code,
service_ip,
simulation_id,
simulation_job_created_id,
sum(agv_job_second) as agv_run_final_second,
simulation_start_time,
simulation_end_time,
simulation_final_second
from 
(
select 
agv_code,
service_ip,
simulation_id,
simulation_job_created_id,
unix_timestamp(job_finish_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(job_accept_time,'yyyy-MM-dd HH:mm:ss') as agv_job_second,
simulation_start_time,
simulation_end_time,
simulation_final_second
from 
${dwd_dbname}.dwd_simulation_agv_job_history_info_di
where d='${pre1_date}'
) t
group by 
agv_code,
service_ip,
simulation_id,
simulation_job_created_id,
simulation_start_time,
simulation_end_time,
simulation_final_second
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
