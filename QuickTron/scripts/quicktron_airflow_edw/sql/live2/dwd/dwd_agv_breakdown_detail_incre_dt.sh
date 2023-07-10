#!/bin/bash
# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 小车故障表信息
#-- 注意 ： 每天T-1增量分区
#-- 输入表 : ods.ods_agv_breakdown_detail_dt、ods.ods_qkt_rcs_basic_agv、ods.ods_qkt_rcs_basic_agv_type、dim.dim_dsp_error_dict、dim_collection_project_record_ful、dwd_notification_message_info_di
#-- 输出表 ：dwd.dwd_agv_breakdown_detail_incre_dt
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 
#-- 2 wangziming 2021-12-01 modify 修改first_classification的判断逻辑（为空则获取新的，然后计算出error_name,error_display_name,error_level）
#-- 3 wangziming 2021-12-02 modify 新增字段 error_code_list，error_code_position，error_code_0_position_list
#-- 4 wangziming 2022-12-05 modify 增加无网的dwd_notification_message_info_di 进行融合（根据dim_collection_project_record_ful 字段（is_nonetwork=1）进行融合）
#-- 5 wangziming 2023-01-12 modify 数据的通知信息回流七天的数据
# ------------------------------------------------------------------------------------------------



######### 设置表的变量
ods_dbname=ods
dwd_dbname=dwd
dim_dbname=dim

table=dwd_agv_breakdown_detail_incre_dt
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天

if [ -n "$1" ] ;then
    pre1_date=$1
    pre2_date=`date -d "-1 day $1" +%F`
    pre3_date=`date -d "-2 day $1" +%F`
else 
    pre1_date=`date -d "-1 day" +%F`
    pre2_date=`date -d "-2 day" +%F`
    pre3_date=`date -d "-3 day" +%F`
fi

#################################################################dwd###############################################################
echo "##############################################hive:{start executor dwd}####################################################################"


init_sql="
set hive.compute.query.using.stats=false;
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;


insert overwrite table ${dwd_dbname}.${table} partition(d,pt)
select 
rt.agv_code,
rt.error_code,
coalesce(coalesce(rt.error_name,rt1.error_ename),rt.error_code) as error_name,
coalesce(coalesce(rt.error_display_name,rt1.error_cname),rt.error_code) as error_display_name,
coalesce(rt.error_level,rt1.error_level) as error_level,
rt.breakdown_id,
rt.speed,
rt.bucket_id,
rt.warehouse_id,
rt.mileage,
rt.point_codes,
rt.point_x,
rt.point_y,
rt.breakdown_log_time,
rt.breakdown_collect_time,
rt.agv_type_id,
rt.agv_type_code,
rt.agv_type_name,
rt.project_code,
rt.project_name,
rt.first_classification,
rt.error_code_list,
rt.error_code_position,
rt.error_code_0_position_list,
rt.d,
rt.pt
from 
(
select 
    t.agv_code,
    t.error_code,
    t.error_name,
    t.error_display_name,
    if(t.error_level=0,1,error_level) as error_level,
    t.breakdown_id,
    t.speed,
    t.bucket_id,
    t.warehouse_id,
    t.mileage,
    t.point_codes,
    t.point_x,
    t.point_y,
    t.log_time as breakdown_log_time,
    t.collect_time as breakdown_collect_time,
    coalesce(t.agv_type_id,a1.agv_type_id) as agv_type_id,
    coalesce(t.agv_type_code,b1.agv_type_code) as agv_type_code,
    coalesce(t.agv_type_name,b1.agv_type_name) as agv_type_name,
    t.project_code,
    t.project_name,
    coalesce(t.first_classification,b1.first_classification) as first_classification,
    if(coalesce(t.first_classification,b1.first_classification) in ('WORKBIN','STOREFORKBIN'),'1','2' ) as flag,
    t.error_code_list,
    t.error_code_position,
    t.error_code_0_position_list,
    substr(t.log_time,0,10) as d,
    t.project_code as pt
from (
select 
*,row_number() over(partition by log_time,project_code,breakdown_id,error_code order by update_time desc ) as rn 
from 
${ods_dbname}.ods_agv_breakdown_detail_dt where substr(log_time,0,10)<='$pre1_date' and project_code!='test_demo' and project_code not rlike '[\u4e00-\u9fa5]'
) t
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_df a1 on t.agv_code=a1.agv_code and t.project_code=a1.project_code and a1.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_type_df b1 on a1.agv_type_id=b1.id and a1.project_code=b1.project_code and b1.d='${pre1_date}'
where t.rn=1
) rt
left join ${dim_dbname}.dim_dsp_error_dict rt1 on rt.error_code=rt1.error_code and rt.flag=rt1.error_agv_type
;
"

sql="
set hive.compute.query.using.stats=false;
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
set mapreduce.map.memory.mb=22048;
set mapreduce.reduce.memory.mb=22048; 



with tmp_notification_message_info_str1 as (
select 
r.agv_code,
r.error_code,
r1.error_ename,
r1.error_cname as error_display_name,
r1.error_level,
r.breakdown_id,
r.speed,
r.bucket_id,
r.warehouse_id,
r.mileage,
r.point_codes,
r.point_x,
r.point_y,
date_format(r.breakdown_log_time,'yyyy-MM-dd HH:mm:ss') as breakdown_log_time,
date_format(r.breakdown_collect_time,'yyyy-MM-dd HH:mm:ss') as breakdown_collect_time,
r.agv_type_id,
r.agv_type_code,
r.agv_type_name,
r.project_code,
r.project_name,
r.first_classification,
r.error_code_list,
r.error_code_position,
r.error_code_0_position_list
from 
(
select 
rt.agv_code,
rt.error_code,
rt.message_id as breakdown_id,
null as speed,
null as bucket_id,
null as warehouse_id,
null as mileage,
null as point_codes,
null as point_x,
null as point_y,
rt.notify_start_time as breakdown_log_time,
rt.notify_start_time as breakdown_collect_time,
rt1.agv_type_id,
rt2.agv_type_code,
rt2.agv_type_name,
rt.project_code,
rt.project_name,
rt2.first_classification,
rt.error_code_list,
rt.error_code_position,
rt.error_code_0_position_list,
if(rt2.first_classification in ('WORKBIN','STOREFORKBIN'),'1','2' ) as flag
from 
(
select 
t.project_code,
t.project_name,
t.agv_code,
t.message_id,
t.notify_start_time,
t.error_code_list,
regexp_replace(t1.error_code,'[\\\\[\\\\]]','') as error_code,
t1.pos as error_code_position,
if(regexp_replace(t1.error_code,'[\\\\[\\\\]]','')='0',t1.pos,null) as error_code_0_position_list
from  
(
select
t.*,t1.project_name
from 
(
select 
a.*,regexp_replace(regexp_replace(regexp_extract(message_body,'\\\\[.*\\\\]',0),'\\\\[ ','['),'\\\\s+',',') as error_code_list
-- ,b.project_name
from 
${dwd_dbname}.dwd_notification_message_info_di a
where a.d>=date_sub('${pre1_date}',7) and upper(a.message_title) in ('RCS_RBTERR_UNKONW','RCS_RBTERR_NOTONCODE')
and a.pt in (select project_code from ${dim_dbname}.dim_collection_project_record_ful where is_nonetwork='1')
) t
left join ${dim_dbname}.dim_collection_project_record_ful t1 on t.pt=t1.project_code
-- where a.d in('2022-12-05','2022-12-04') and upper(a.message_title) in ('RCS_RBTERR_UNKONW','RCS_RBTERR_NOTONCODE') and b.is_nonetwork='1'

) t
lateral view posexplode(split(t.error_code_list,',')) t1 as pos,error_code
) rt
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_df rt1 on rt.agv_code=rt1.agv_code and rt.project_code=rt1.project_code and rt1.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_type_df rt2 on rt1.agv_type_id=rt2.id and rt1.project_code=rt2.project_code and rt2.d='${pre1_date}'
) r 
left join ${dim_dbname}.dim_dsp_error_dict r1 on r.error_code=r1.error_code and r.flag=r1.error_agv_type
)
insert overwrite table ${dwd_dbname}.${table} partition(d,pt)
select 
rt.agv_code,
rt.error_code,
coalesce(coalesce(rt.error_name,rt1.error_ename),rt.error_code) as error_name,
coalesce(coalesce(rt.error_display_name,rt1.error_cname),rt.error_code) as error_display_name,
coalesce(rt.error_level,rt1.error_level) as error_level,
rt.breakdown_id,
rt.speed,
rt.bucket_id,
rt.warehouse_id,
rt.mileage,
rt.point_codes,
rt.point_x,
rt.point_y,
rt.breakdown_log_time,
rt.breakdown_collect_time,
rt.agv_type_id,
rt.agv_type_code,
rt.agv_type_name,
rt.project_code,
rt.project_name,
rt.first_classification,
rt.error_code_list,
rt.error_code_position,
rt.error_code_0_position_list,
rt.d,
rt.pt
from 
(
select 
    t.agv_code,
    t.error_code,
    t.error_name,
    t.error_display_name,
    if(t.error_level=0,1,error_level) as error_level,
    t.breakdown_id,
    t.speed,
    t.bucket_id,
    t.warehouse_id,
    t.mileage,
    t.point_codes,
    t.point_x,
    t.point_y,
    t.log_time as breakdown_log_time,
    t.collect_time as breakdown_collect_time,
    coalesce(t.agv_type_id,a1.agv_type_id) as agv_type_id,
    coalesce(t.agv_type_code,b1.agv_type_code) as agv_type_code,
    coalesce(t.agv_type_name,b1.agv_type_name) as agv_type_name,
    t.project_code,
    t.project_name,
    coalesce(t.first_classification,b1.first_classification) as first_classification,
    if(coalesce(t.first_classification,b1.first_classification) in ('WORKBIN','STOREFORKBIN'),'1','2' ) as flag,
    t.error_code_list,
    t.error_code_position,
    t.error_code_0_position_list,
    substr(t.log_time,0,10) as d,
    t.project_code as pt
from (
select 
*,row_number() over(partition by log_time,project_code,breakdown_id,error_code order by update_time desc ) as rn 
from 
${ods_dbname}.ods_agv_breakdown_detail_dt where d>='$pre3_date' and substr(log_time,0,10) between '$pre3_date' and '$pre1_date' and  project_code!='test_demo'  and project_code not rlike '[\u4e00-\u9fa5]'
) t
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_df a1 on t.agv_code=a1.agv_code and t.project_code=a1.project_code and a1.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_type_df b1 on a1.agv_type_id=b1.id and a1.project_code=b1.project_code and b1.d='${pre1_date}'
where t.rn=1
) rt
left join ${dim_dbname}.dim_dsp_error_dict rt1 on rt.error_code=rt1.error_code and rt.flag=rt1.error_agv_type

union all
select 
agv_code,
cast(error_code as int) as error_code,
error_ename,
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
cast(error_code_0_position_list as string) as error_code_0_position_list,
substr(breakdown_log_time,1,10) as d,
project_code as pt
from 
tmp_notification_message_info_str1
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

