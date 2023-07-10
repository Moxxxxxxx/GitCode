#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 任务流程历史状态数据
#-- 注意 ： 
#-- 输入表 : dwd.dwd_ones_org_user_info_ful、dwd_ones_task_message_info_di、ods.ods_qkt_ones_task_df
#-- 输出表 ：dwd.dwd_one_task_process_change_info_his
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-10 CREATE 
#-- 2 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp
#-- 3 wangziming 2022-03-25 modify 修改 dwd层任务表为ods_qkt_ones_task_df
#-- 4 wangziming 2022-06-08 modify 修改 代码的全量改为增量逻辑 ，并增加临时表 做缓存tmp_one_task_process_change_info_df
#-- 5 wangziming 2022-07-06 modify 增加 任务拥有者邮箱，任务分配者邮箱，任务变更者邮箱字段
#-- 6 wangziming 2022-07-09 modify 增加 任务变更表 reference_type<>'1'条件
#-- 7 wangziming 2022-08-30 modify ones枚举映射值变化，关联离线外部表进行重新修正逻辑
# -----------------------------------------------------------------------------------------------

ods_dbname=ods
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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


with task_message as(
select 
*
from 
${dwd_dbname}.dwd_ones_task_message_info_di
where  nvl(ext_json,'')<>'' and ext_json<>'{}' and locate('field_values',ext_json)<=0
-- and to_date(send_time)=${pre1_date} 
),
multi_option as (

select 
*
from 
task_message
where locate('new_multi_option',ext_json)>0
),
user_info as (
select uuid,user_name from ${dwd_dbname}.dwd_ones_org_user_info_ful
)
insert overwrite table ${dwd_dbname}.dwd_one_task_process_change_info_his
select
t1.uuid,
t1.task_uuid,
t3.uuid as task_owner_uuid,
t3.user_name as task_owner_name,
t4.uuid as task_assign_uuid,
t4.user_name as task_assign_name,
t2.issue_type_uuid as task_issue_type_uuid,
regexp_replace(t2.summary,'\r|\n|\t','') as task_summary,
t5.uuid as task_process_user_uuid,
t5.user_name as task_process_user,
t1.task_process_time,
t1.task_process_field,
t1.task_process_field_type,
case when t1.task_process_field in ('field020','field033','field018','P3w4xnbG','7Pkb4Apt') then round(t1.old_task_field_value/100000,1)
	 when t1.task_process_field in ('field027','V9chD9tx','field028','BhY3gxNf','D1ixREgk') then cast(to_utc_timestamp(cast(t1.old_task_field_value as bigint)*1000 ,'GMT-8') as string)
	else regexp_replace(t1.old_task_field_value,'\r|\n|\t','') end as  old_task_field_value,
case when t1.task_process_field in ('field020','field033','field018','P3w4xnbG','7Pkb4Apt') then round(t1.new_task_field_value/100000,1)
	 when t1.task_process_field in ('field027','V9chD9tx','field028','BhY3gxNf','D1ixREgk') then cast(to_utc_timestamp(cast(t1.new_task_field_value as bigint)*1000,'GMT-8') as string)
	else regexp_replace(t1.new_task_field_value,'\r|\n|\t','') end as  new_task_field_value
from 
(
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
if(get_json_object(ext_json,'$.new_option') is not null,get_json_object(get_json_object(ext_json,'$.old_option'),'$.name'),null ) as old_task_field_value,
get_json_object(get_json_object(ext_json,'$.new_option'),'$.name') as new_task_field_value
from 
task_message
where locate('new_option',ext_json)>0

union all
select 
new1.uuid,
new1.task_uuid,
new1.task_process_user_uuid,
new1.task_process_time,
new1.task_process_field,
new1.task_process_field_type,
old1.old_task_field_value,
new1.new_task_field_value
from 
(
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
concat_ws('&',collect_set(get_json_object(nv.new_value,'$.name'))) as new_task_field_value
from 
multi_option t
lateral view posexplode(split(substr(get_json_object(t.ext_json,'$.new_multi_option'),2),'(?<=\\\}),(?=\\\{)')) nv as pos,new_value
group by uuid,object_id,subject_id,send_time,get_json_object(ext_json,'$.field_uuid'),get_json_object(ext_json,'$.field_name')
) new1
left join 
(select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
concat_ws('&',collect_set(get_json_object(ov.old_value,'$.name'))) as old_task_field_value
from 
multi_option t
lateral view posexplode(split(substr(get_json_object(t.ext_json,'$.old_multi_option'),2),'(?<=\\\}),(?=\\\{)')) ov as pos,old_value
group by uuid,object_id,subject_id,send_time,get_json_object(ext_json,'$.field_uuid'),get_json_object(ext_json,'$.field_name')
) old1 on new1.uuid=old1.uuid

union all
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
get_json_object(ext_json,'$.old_value') as old_task_field_value,
get_json_object(ext_json,'$.new_value') as new_task_field_value
from 
task_message
where locate('new_option',ext_json)<=0 and locate('new_multi_option',ext_json)<=0 and get_json_object(ext_json,'$.field_uuid') not in('field009','field036','field013','field010','field019')

union all
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
if(get_json_object(ext_json,'$.field_uuid')='field019','已登记工时',get_json_object(ext_json,'$.field_name')) as task_process_field_type,
case when get_json_object(ext_json,'$.field_uuid')='field019' then cast(get_json_object(ext_json,'$.owner_old_total')/100000 as string)
	 when get_json_object(ext_json,'$.field_uuid')='field013' then if(locate('distance_time',ext_json)>0,null,cast(to_utc_timestamp(cast(get_json_object(ext_json,'$.old_value') as bigint)*1000 ,'GMT-8') as string))
	 else null end as old_task_field_value,
case when get_json_object(ext_json,'$.field_uuid')='field019' then cast(get_json_object(ext_json,'$.owner_new_total')/100000 as string)
	 when get_json_object(ext_json,'$.field_uuid')='field013' then if(locate('distance_time',ext_json)>0,(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.day')*24*60)+(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.hour')*60)+get_json_object(get_json_object(ext_json,'$.distance_time'),'$.minute'),cast(to_utc_timestamp(cast(get_json_object(ext_json,'$.old_value') as bigint)*1000 ,'GMT-8')as string))
	 else (get_json_object(get_json_object(ext_json,'$.distance_time'),'$.day')*24*60)+(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.hour')*60)+get_json_object(get_json_object(ext_json,'$.distance_time'),'$.minute') end as new_task_field_value
from 
task_message
where get_json_object(ext_json,'$.field_uuid') in ('field009','field036','field013','field010','field019')
) t1
left join (select uuid,owner as task_owner,assign as task_assign,issue_type_uuid,regexp_replace(summary,'\n|\r|\t','') as summary from ${ods_dbname}.ods_qkt_ones_task_df where d='${pre1_date}') t2 on t1.task_uuid =t2.uuid
left join user_info t3 on t2.task_owner=t3.uuid
left join user_info t4 on t2.task_assign=t4.uuid
left join user_info t5 on t1.task_process_user_uuid=t5.uuid
;
"

columns=uuid,task_uuid,task_owner_uuid,task_owner_name,task_assign_uuid,task_assign_name,task_issue_type_uuid,task_summary,task_process_user_uuid,task_process_user,task_process_time,task_process_field,task_process_field_type,old_task_field_value,new_task_field_value,task_owner_email,task_assign_email,task_process_user_email
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


drop table if exists ${tmp_dbname}.tmp_one_task_process_change_info_his;
create table ${tmp_dbname}.tmp_one_task_process_change_info_his as
with task_message as(
select 
*
from 
${dwd_dbname}.dwd_ones_task_message_info_di
where  nvl(ext_json,'')<>'' and ext_json<>'{}' 
and locate('field_values',ext_json)<=0
and d='${pre1_date}'
and to_date(send_time)='${pre1_date}' 
and reference_type<>'2'
),
multi_option as (

select 
*
from 
task_message
where locate('new_multi_option',ext_json)>0
),
user_info as (
select uuid,user_name,user_email from ${dwd_dbname}.dwd_ones_org_user_info_ful
)
select
t1.uuid,
t1.task_uuid,
t3.uuid as task_owner_uuid,
t3.user_name as task_owner_name,
t4.uuid as task_assign_uuid,
t4.user_name as task_assign_name,
t2.issue_type_uuid as task_issue_type_uuid,
regexp_replace(t2.summary,'\r|\n|\t','') as task_summary,
t5.uuid as task_process_user_uuid,
t5.user_name as task_process_user,
t1.task_process_time,
t1.task_process_field,
t1.task_process_field_type,
case when t1.task_process_field in ('field020','field033','field018','P3w4xnbG','7Pkb4Apt') then round(t1.old_task_field_value/100000,1)
	 when t1.task_process_field in ('field027','V9chD9tx','field028','BhY3gxNf','D1ixREgk') then cast(to_utc_timestamp(cast(t1.old_task_field_value as bigint)*1000,'GMT-8') as string)
	else regexp_replace(t1.old_task_field_value,'\r|\n|\t','') end as  old_task_field_value,
case when t1.task_process_field in ('field020','field033','field018','P3w4xnbG','7Pkb4Apt') then round(t1.new_task_field_value/100000,1)
	 when t1.task_process_field in ('field027','V9chD9tx','field028','BhY3gxNf','D1ixREgk') then cast(to_utc_timestamp(cast(t1.new_task_field_value as bigint)*1000,'GMT-8') as string)
	else regexp_replace(t1.new_task_field_value,'\r|\n|\t','') end as  new_task_field_value,
t3.user_email as task_owner_email,
t4.user_email as task_assign_email,
t5.user_email as task_process_user_email

from 
(
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
if(get_json_object(ext_json,'$.new_option') is not null,get_json_object(get_json_object(ext_json,'$.old_option'),'$.name'),null ) as old_task_field_value,
get_json_object(get_json_object(ext_json,'$.new_option'),'$.name') as new_task_field_value
from 
task_message
where locate('new_option',ext_json)>0

union all
select 
new1.uuid,
new1.task_uuid,
new1.task_process_user_uuid,
new1.task_process_time,
new1.task_process_field,
new1.task_process_field_type,
old1.old_task_field_value,
new1.new_task_field_value
from 
(
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
concat_ws('&',collect_set(get_json_object(nv.new_value,'$.name'))) as new_task_field_value
from 
multi_option t
lateral view posexplode(split(substr(get_json_object(t.ext_json,'$.new_multi_option'),2),'(?<=\\\}),(?=\\\{)')) nv as pos,new_value
group by uuid,object_id,subject_id,send_time,get_json_object(ext_json,'$.field_uuid'),get_json_object(ext_json,'$.field_name')
) new1
left join 
(select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
concat_ws('&',collect_set(get_json_object(ov.old_value,'$.name'))) as old_task_field_value
from 
multi_option t
lateral view posexplode(split(substr(get_json_object(t.ext_json,'$.old_multi_option'),2),'(?<=\\\}),(?=\\\{)')) ov as pos,old_value
group by uuid,object_id,subject_id,send_time,get_json_object(ext_json,'$.field_uuid'),get_json_object(ext_json,'$.field_name')
) old1 on new1.uuid=old1.uuid
 union all
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
get_json_object(ext_json,'$.field_name') as task_process_field_type,
get_json_object(ext_json,'$.old_value') as old_task_field_value,
get_json_object(ext_json,'$.new_value') as new_task_field_value
from 
task_message
where locate('new_option',ext_json)<=0 and locate('new_multi_option',ext_json)<=0 and get_json_object(ext_json,'$.field_uuid') not in('field009','field036','field013','field010','field019')

union all
select 
uuid,
object_id as task_uuid,
subject_id as task_process_user_uuid,
send_time as task_process_time,
get_json_object(ext_json,'$.field_uuid') as task_process_field,
if(get_json_object(ext_json,'$.field_uuid')='field019','已登记工时',get_json_object(ext_json,'$.field_name')) as task_process_field_type,
case when get_json_object(ext_json,'$.field_uuid')='field019' then get_json_object(ext_json,'$.owner_old_total')/100000
	 when get_json_object(ext_json,'$.field_uuid')='field013' then if(locate('distance_time',ext_json)>0,null,cast(to_utc_timestamp(cast(get_json_object(ext_json,'$.old_value')*1000 as bigint),'GMT-8') as string))
	 else null end  as old_task_field_value,
	 
case when get_json_object(ext_json,'$.field_uuid')='field019' then get_json_object(ext_json,'$.owner_new_total')/100000
	 when get_json_object(ext_json,'$.field_uuid')='field013' then if(locate('distance_time',ext_json)>0,(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.day')*24*60)+(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.hour')*60)+get_json_object(get_json_object(ext_json,'$.distance_time'),'$.minute'),cast(to_utc_timestamp(cast(get_json_object(ext_json,'$.old_value') as bigint)*1000,'GMT-8') as string))
	 else (get_json_object(get_json_object(ext_json,'$.distance_time'),'$.day')*24*60)+(get_json_object(get_json_object(ext_json,'$.distance_time'),'$.hour')*60)+get_json_object(get_json_object(ext_json,'$.distance_time'),'$.minute') end  as new_task_field_value

from 
task_message
where get_json_object(ext_json,'$.field_uuid') in ('field009','field036','field013','field010','field019')
) t1
left join (select uuid,owner as task_owner,assign as task_assign,issue_type_uuid,regexp_replace(summary,'\n|\r|\t','') as summary from ${ods_dbname}.ods_qkt_ones_task_df where d='${pre1_date}') t2 on t1.task_uuid =t2.uuid
left join user_info t3 on t2.task_owner=t3.uuid
left join user_info t4 on t2.task_assign=t4.uuid
left join user_info t5 on t1.task_process_user_uuid=t5.uuid
;

insert overwrite table ${dwd_dbname}.dwd_one_task_process_change_info_his
select 
uuid, 
task_uuid, 
task_owner_uuid, 
task_owner_name, 
task_assign_uuid, 
task_assign_name,
task_issue_type_uuid,
task_summary, 
task_process_user_uuid, 
task_process_user, 
task_process_time, 
task_process_field, 
if(b.attribute_key is not null,b.attribute_value,a.task_process_field_type) as task_process_field_type, 
if(c.attribute_key is not null,c.attribute_value,a.old_task_field_value) as old_task_field_value, 
if(d.attribute_key is not null,d.attribute_value,a.new_task_field_value) as new_task_field_value, 
task_owner_email, 
task_assign_email, 
task_process_user_email
from 
${tmp_dbname}.tmp_one_task_process_change_info_his a
left join dim.dim_ones_attribute_info_offline b on regexp_replace(a.task_process_field_type,'[\}\{]','')=b.attribute_key
left join dim.dim_ones_attribute_info_offline c on regexp_replace(a.old_task_field_value,'[\}\{]','')=c.attribute_key
left join dim.dim_ones_attribute_info_offline d on regexp_replace(a.new_task_field_value,'[\}\{]','')=d.attribute_key

union all
select
${columns}
from  
${tmp_dbname}.tmp_one_task_process_change_info_df
where d='${pre2_date}'
;


insert overwrite table ${tmp_dbname}.tmp_one_task_process_change_info_df partition(d='${pre1_date}')
select 
${columns}
from 
${dwd_dbname}.dwd_one_task_process_change_info_his
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

