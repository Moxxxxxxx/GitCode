#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉组织变更记录表
#-- 注意 ： 每天增量数据追加到昨日分区，得到最新的数据
#-- 输入表 : dim.dim_dtk_org_history_info_df 
#-- 输出表 ：dwd.dwd_dtk_org_change_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-17 CREATE 


# ------------------------------------------------------------------------------------------------


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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_dtk_org_change_info_df partition(d='${pre1_date}')
select 
null as old_org_id,
org_id as new_org_id,
null as old_org_name,
org_name as new_org_name,
null as old_parent_org_id,
parent_org_id as new_parent_org_id,
null as old_parent_org_name,
parent_org_name as new_parent_org_name,
null as old_dept_org_id,
dept_org_id as new_dept_org_id,
null as old_dept_org_name,
dept_org_name as new_dept_org_name,
null as old_org_path_id,
org_path_id as new_org_path_id,
null as old_org_path_name,
org_path_name as new_org_path_name,
'init' as org_change_type,
'append' as org_change_category,
'${pre1_date}' as org_change_date
from 
${dim_dbname}.dim_dtk_org_history_info_df
where d='${pre1_date}'
;
"




sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


with  old_dtk_org as (
select 
*
from 
${dim_dbname}.dim_dtk_org_history_info_df
where d='${pre2_date}'
),
new_dtk_org as (
select 
*
from 
${dim_dbname}.dim_dtk_org_history_info_df
where d='${pre1_date}'
),
append_dtk_org as (  -- 每日新增组织架构
select 
new.*,
'${pre1_date}' as org_change_date
from 
new_dtk_org new 
left join old_dtk_org old on new.org_id=old.org_id 
where old.org_id is null
),
remove_dtk_org as (  -- 每日减少组织架构
select 
old.*,
'${pre1_date}' as org_change_date
from 
old_dtk_org old 
left join new_dtk_org new on new.org_id=old.org_id 
where new.org_id is null
),
change_dtk_org as (
select 
old.org_id as old_org_id,
new.org_id as new_org_id,
old.org_name as old_org_name,
new.org_name as new_org_name,
old.parent_org_id as old_parent_org_id,
new.parent_org_id as new_parent_org_id,
old.parent_org_name as old_parent_org_name,
new.parent_org_name as new_parent_org_name,
old.dept_org_id as old_dept_org_id,
new.dept_org_id as new_dept_org_id,
old.dept_org_name as old_dept_org_name,
new.dept_org_name as new_dept_org_name,
old.org_path_id as old_org_path_id,
new.org_path_id as new_org_path_id,
old.org_path_name as old_org_path_name,
new.org_path_name as new_org_path_name,
case when concat(new.org_type,'-',nvl(new.org_type_level,''))<>concat(old.org_type,'-',nvl(old.org_type_level,''))
	      then concat(nvl(old.org_type_level,old.org_type),'>', nvl(new.org_type_level,new.org_type)) 
	 when new.org_name <> old.org_name  then concat(old.org_name,'->',new.org_name)
	else 'UNKNOWN' 	end as org_change_type,
'change' as org_change_category,
'${pre1_date}' as org_change_date
from 
new_dtk_org new
left join old_dtk_org old on new.org_id=old.org_id
where (new.org_name <> old.org_name 
or new.org_type <> old.org_type
or nvl(new.org_type_level,'') <> nvl(old.org_type_level,'')
) and old.org_id is not null
)
insert overwrite table ${dwd_dbname}.dwd_dtk_org_change_info_df partition (d='${pre1_date}')
select 
old_org_id,
new_org_id,
old_org_name,
new_org_name,
old_parent_org_id,
new_parent_org_id,
old_parent_org_name,
new_parent_org_name,
old_dept_org_id,
new_dept_org_id,
old_dept_org_name,
new_dept_org_name,
old_org_path_id,
new_org_path_id,
old_org_path_name,
new_org_path_name,
org_change_type,
org_change_category,
org_change_date
from 
change_dtk_org

union all
select 
null as old_org_id,
org_id as new_org_id,
null as old_org_name,
org_name as new_org_name,
null as old_parent_org_id,
parent_org_id as new_parent_org_id,
null as old_parent_org_name,
parent_org_name as new_parent_org_name,
null as old_dept_org_id,
dept_org_id as new_dept_org_id,
null as old_dept_org_name,
dept_org_name as new_dept_org_name,
null as old_org_path_id,
org_path_id as new_org_path_id,
null as old_org_path_name,
org_path_name as new_org_path_name,
concat('UNKNOWN','->',nvl(org_type_level,org_type)) as org_change_type,
'append' as org_change_category,
org_change_date
from 
append_dtk_org 

union all
select 
org_id as old_org_id,
null as new_org_id,
org_name as old_org_name,
null as new_org_name,
parent_org_id as old_parent_org_id,
null as new_parent_org_id,
parent_org_name as old_parent_org_name,
null as new_parent_org_name,
dept_org_id as old_dept_org_id,
null as new_dept_org_id,
dept_org_name as old_dept_org_name,
null as new_dept_org_name,
org_path_id as old_org_path_id,
null as new_org_path_id,
org_path_name as old_org_path_name,
null as new_org_path_name,
concat(nvl(org_type_level,org_type),'->','UNKNOWN') as org_change_type,
'remove' as org_change_category,
org_change_date
from 
remove_dtk_org

union all 
select 
old_org_id,
new_org_id,
old_org_name,
new_org_name,
old_parent_org_id,
new_parent_org_id,
old_parent_org_name,
new_parent_org_name,
old_dept_org_id,
new_dept_org_id,
old_dept_org_name,
new_dept_org_name,
old_org_path_id,
new_org_path_id,
old_org_path_name,
new_org_path_name,
org_change_type,
org_change_category,
org_change_date
from 
${dwd_dbname}.dwd_dtk_org_change_info_df
where d='${pre2_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
