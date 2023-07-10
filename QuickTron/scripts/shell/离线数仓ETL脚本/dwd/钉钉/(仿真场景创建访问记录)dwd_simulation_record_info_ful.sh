#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成仿真场景测试记录
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_simulation_record_info_df
#-- 输出表 ：dwd.dwd_simulation_record_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-19 CREATE 
#-- 2 wangziming 2021-12-03 modify 修改仿真记录的逻辑
#-- 3 wangziming 2021-12-13 modify 增加字段user_name


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




sql_0="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_simulation_record_info_ful
select 
uuid,
user_ip,
simulation_id,
simulation_type,
simulation_sub_type,
server_ip,
scene_type,
map_time,
build_start_time,
build_time,
data_time,
case when scene_type ='map-create' and lead_scene_type<>scene_type then unix_timestamp(lead_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(data_time,'yyyy-MM-dd HH:mm:ss')
     when scene_type ='map-create' and lead_scene_type=scene_type then 0
     else null end as scene_duration_second,
case when scene_type ='map-create' and lead_scene_type<>scene_type then concat(lead_scene_type,'->',scene_type)
     when scene_type ='map-create' and lead_scene_type=scene_type then concat(scene_type,'->','本身场景')
     else null end as scene_actions_concat
from 
(
select 
*,
lead(data_time,1,data_time) over(partition by uuid order by data_time) as lead_time,
lead(scene_type,1,scene_type) over(partition by uuid order by data_time) as lead_scene_type
from 
(
select
uuid,
user_ip,
simulation_id,
simulation_type,
move_type as simulation_sub_type,
node_ip as server_ip,
s_type as scene_type,
map_time,
build_start_time,
build_time,
data_time,
row_number() over(partition by uuid,s_type order by data_time) as rn 
from 
${ods_dbname}.ods_qkt_simulation_record_info_df 
where d='${pre1_date}'
) t
where t.rn=1
) tn
;
"

sql="


set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


create temporary table ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0 as 
select 
uuid,
user_ip,
simulation_id,
simulation_type,
move_type,
node_ip,
s_type,
map_time,
build_start_time,
build_time,
data_time,
end_time,
kill_time,
\`user\` as user_name
from 
(
select 
*,
row_number() over(partition by data_time,node_ip,uuid) as rn
from 
${ods_dbname}.ods_qkt_simulation_record_info_df
where d='${pre1_date}' and to_date(update_time)>='2021-12-01'
) t
where t.rn=1
;


create TEMPORARY table ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg1 as 
with simulation_record_map as (
select 
*
from 
${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0
where s_type='map-create'
),
simulation_record_build as (
select 
*
from 
(
select 
*,row_number() over(partition by uuid order by data_time ) as rn 
from 
${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0
where s_type in ('simulation-build','simulation-startAndBuild')
) t
where t.rn =1
)
select 
a.uuid,
a.user_ip,
a.simulation_id,
b.simulation_type,
b.move_type,
a.node_ip,
a.s_type,
a.user_name
from 
simulation_record_build a
left join simulation_record_map b on a.uuid=b.uuid
;


create temporary table ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg2 as 
with  ods_simulation_record_map as (
select * from ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0 where s_type='map-create'
),
ods_simulation_record_build as (
select * from ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0 where s_type in ('simulation-startAndBuild','simulation-build')
),
ods_simulation_record_finsh as (
select * from ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg0 where s_type in ('simulation-end','simulation-finish')
)
select 
*
from 
(
select 
a.uuid,
a.user_ip,
b.simulation_id,
a.simulation_type,
a.move_type,
a.node_ip,
a.s_type,
a.map_time,
a.build_start_time,
a.build_time,
a.data_time,
a.end_time,
a.kill_time,
a.user_name
from
ods_simulation_record_map a
left join ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg1 b on a.uuid=b.uuid  and a.node_ip=b.node_ip

union all
select 
a.uuid,
a.user_ip,
a.simulation_id,
b.simulation_type,
b.move_type,
a.node_ip,
a.s_type,
a.map_time,
a.build_start_time,
a.build_time,
a.data_time,
a.end_time,
a.kill_time,
a.user_name
from 
ods_simulation_record_build a
left join ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg1 b on a.uuid=b.uuid and a.node_ip=b.node_ip

union all
select 
b.uuid,
b.user_ip,
a.simulation_id,
b.simulation_type,
b.move_type,
a.node_ip,
a.s_type,
a.map_time,
a.build_start_time,
a.build_time,
a.data_time,
a.end_time,
a.kill_time,
b.user_name
from 
ods_simulation_record_finsh a
left join ${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg1 b on a.simulation_id=b.simulation_id and a.node_ip=b.node_ip
) t
where t.uuid is not null
;

insert overwrite table ${dwd_dbname}.dwd_simulation_record_info_ful
select 
uuid,
user_ip,
simulation_id,
simulation_type,
simulation_sub_type,
server_ip,
scene_type,
map_time,
build_start_time,
build_time,
data_time,
case when scene_type in('map-create','simulation-startAndBuild') and lead_scene_type<>scene_type then unix_timestamp(lead_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(data_time,'yyyy-MM-dd HH:mm:ss')
     else 0  end as scene_duration_second,
case when scene_type in('map-create','simulation-startAndBuild') and lead_scene_type<>scene_type then concat(scene_type,'->',lead_scene_type)
     else 'OTHER' end  as scene_actions_concat,
end_time,
kill_time,
user_name
from 
(
select 
uuid,
user_ip,
simulation_id,
simulation_type,
move_type as simulation_sub_type,
node_ip as server_ip,
s_type as scene_type,
map_time,
build_start_time,
build_time,
data_time,
end_time,
kill_time,
user_name,
lead(data_time,1,data_time) over(partition by uuid order by data_time) as lead_time,
lead(s_type,1,s_type) over(partition by uuid order by data_time) as lead_scene_type
from 
${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg2
where s_type!='simulation-build'
) t

union all
select 
uuid,
user_ip,
simulation_id,
simulation_type,
move_type as simulation_sub_type,
node_ip as server_ip,
s_type as scene_type,
map_time,
build_start_time,
build_time,
data_time,
0  as scene_duration_second,
'OTHER' as scene_actions_concat,
end_time,
kill_time,
user_name
from 
${dwd_dbname}.tmp_dwd_simulation_record_info_ful_stg2
where s_type='simulation-build'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

