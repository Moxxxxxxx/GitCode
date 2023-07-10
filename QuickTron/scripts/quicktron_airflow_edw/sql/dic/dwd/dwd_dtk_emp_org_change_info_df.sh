#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工组织变更记录表
#-- 注意 ： 每天全量覆盖
#-- 输入表 : dwd.dwd_dtk_emp_info_df、dim.dim_dtk_org_level_info
#-- 输出表 ：dwd.dwd_dtk_emp_org_change_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-18 CREATE 


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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

add jar /data/hive/jar/hie-udf-1.0-SNAPSHOT.jar;
create temporary function udf_concat_str as 'com.quicktron.controll.ConcatStrUDF';

insert overwrite table ${dim_dbname}.dim_dtk_org_level_info_test
select 
a.org_id,
a.org_name,
a.org_level_num,
a.org_id_1,
a.org_name_1,
a.org_id_2,
a.org_name_2,
a.org_id_3,
a.org_name_3,
a.org_id_4,
a.org_name_4,
a.org_id_5,
a.org_name_5,
a.org_id_6,
a.org_name_6,
a.org_id_7,
a.org_name_7,
a.org_id_8,
a.org_name_8,
a.org_id_9,
a.org_name_9,
a.org_id_10,
a.org_name_10,
a.org_path_id,
a.org_path_name,
a.org_company_name,
b.org_role_tye,
1 as is_valid
from 
(
select
id as org_id, 
name as org_name, 
level_num as org_level_num,
l1_id as org_id_1,
l1_name as org_name_1, 
l2_id as org_id_2, 
l2_name as org_name_2, 
l3_id as org_id_3, 
l3_name as org_name_3, 
l4_id as org_id_4, 
l4_name as org_name_4, 
l5_id as org_id_5, 
l5_name as org_name_5, 
l6_id as org_id_6, 
l6_name as org_name_6, 
l7_id as org_id_7, 
l7_name as org_name_7, 
l8_id as org_id_8, 
l8_name as org_name_8, 
l9_id as org_id_9, 
l9_name as org_name_9, 
l10_id as org_id_10, 
l10_name as org_name_10,
regexp_replace(udf_concat_str('/','id',l1_id,l2_id,l3_id,l4_id,l5_id,l6_id,l7_id,l8_id,l9_id,l10_id),'/id','') as org_path_id,
regexp_replace(udf_concat_str('/','name',l1_name,l2_name,l3_name,l4_name,l5_name,l6_name,l7_name,l8_name,l9_name,l10_name),'/name','') as org_path_name,
org_name as org_company_name
from 
${ods_dbname}.ods_qkt_dtk_dingtalk_department_level_df 
where d='${pre1_date}'
) a 
left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='1') b on a.org_path_name=b.org_path_name
;

insert overwrite table dwd.dwd_dtk_emp_org_change_info_df partition(d='${pre1_date}')
select 
t1.emp_id,
t1.emp_name,
null as old_org_id,
null as old_org_name,
t2.org_id as new_org_id,
t2.org_name as new_org_name,
null as old_org_id_1,
null as old_org_name_1,
t2.org_id_1 as new_org_id_1,
t2.org_name_1 as new_org_name_1,

null as old_org_id_2,
null as old_org_name_2,
t2.org_id_2 as new_org_id_2,
t2.org_name_2 as new_org_name_2,

null as old_org_id_3,
null as old_org_name_3,
t2.org_id_3 as new_org_id_3,
t2.org_name_3 as new_org_name_3,

null as old_org_id_4,
null as old_org_name_4,
t2.org_id_4 as new_org_id_4,
t2.org_name_4 as new_org_name_4,

null as old_org_id_5,
null as old_org_name_5,
t2.org_id_5 as new_org_id_5,
t2.org_name_5 as new_org_name_5,

null as old_org_id_6,
null as old_org_name_6,
t2.org_id_6 as new_org_id_6,
t2.org_name_6 as new_org_name_6,

null as old_org_id_7,
null as old_org_name_7,
t2.org_id_7 as new_org_id_7,
t2.org_name_7 as new_org_name_7,

null as old_org_id_8,
null as old_org_name_8,
t2.org_id_8 as new_org_id_8,
t2.org_name_8 as new_org_name_8,

null as old_org_id_9,
null as old_org_name_9,
t2.org_id_9 as new_org_id_9,
t2.org_name_9 as new_org_name_9,

null as old_org_id_10,
null as old_org_name_10,
t2.org_id_10 as new_org_id_10,
t2.org_name_10 as new_org_name_10,

null as old_org_path_id,
null as old_org_path_name,
t2.org_path_id as new_org_path_id,
t2.org_path_name as new_org_path_name,
t1.org_change_date
from 
(
select 
a.emp_id,
a.emp_name,
to_date(a.hired_date) as org_change_date,
b.org_id
from 
dwd.dwd_dtk_emp_info_df a
lateral view explode(split(a.org_ids,',')) b as org_id
where a.d='${pre1_date}'
) t1
left join ${dim_dbname}.dim_dtk_org_level_info_test t2 on t1.org_id=t2.org_id
;
"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with tmp_dtk_emp_org_change_new_str1 as (
select 
new.emp_id,
new.emp_name,
new.org_path_id,
new.org_company_name,
b.org_id,
substr(new.hired_date,1,10) as hired_date,
if(size(split(new.org_ids,','))>1,1,0) as is_more,
new.is_job
from 
${dwd_dbname}.dwd_dtk_emp_info_df new
lateral view explode(split(new.org_ids,',')) b as org_id
where new.d='${pre1_date}'
),
tmp_dtk_emp_org_change_old_str2 as (
select 
old.emp_id,
old.emp_name,
old.org_path_id,
old.org_company_name,
b.org_id,
substr(old.hired_date,1,10) as hired_date,
if(size(split(old.org_ids,','))>1,1,0) as is_more,
old.is_job
from 
${dwd_dbname}.dwd_dtk_emp_info_df old
lateral view explode(split(old.org_ids,',')) b as org_id
where old.d='${pre2_date}'
),
tmp_dtk_emp_org_change_str3 as ( -- 获取每天入职员工的组织架构
select 
t1.emp_id,
t1.emp_name,
null as old_org_id,
null as old_org_name,
t2.org_id as new_org_id,
t2.org_name as new_org_name,
null as old_org_id_1,
null as old_org_name_1,
t2.org_id_1 as new_org_id_1,
t2.org_name_1 as new_org_name_1,
null as old_org_id_2,
null as old_org_name_2,
t2.org_id_2 as new_org_id_2,
t2.org_name_2 as new_org_name_2,
null as old_org_id_3,
null as old_org_name_3,
t2.org_id_3 as new_org_id_3,
t2.org_name_3 as new_org_name_3,
null as old_org_id_4,
null as old_org_name_4,
t2.org_id_4 as new_org_id_4,
t2.org_name_4 as new_org_name_4,
null as old_org_id_5,
null as old_org_name_5,
t2.org_id_5 as new_org_id_5,
t2.org_name_5 as new_org_name_5,
null as old_org_id_6,
null as old_org_name_6,
t2.org_id_6 as new_org_id_6,
t2.org_name_6 as new_org_name_6,
null as old_org_id_7,
null as old_org_name_7,
t2.org_id_7 as new_org_id_7,
t2.org_name_7 as new_org_name_7,
null as old_org_id_8,
null as old_org_name_8,
t2.org_id_8 as new_org_id_8,
t2.org_name_8 as new_org_name_8,
null as old_org_id_9,
null as old_org_name_9,
t2.org_id_9 as new_org_id_9,
t2.org_name_9 as new_org_name_9,
null as old_org_id_10,
null as old_org_name_10,
t2.org_id_10 as new_org_id_10,
t2.org_name_10 as new_org_name_10,
null as old_org_path_id,
null as old_org_path_name,
t2.org_path_id as new_org_path_id,
t2.org_path_name as new_org_path_name,
t1.hired_date as org_change_date
from 
tmp_dtk_emp_org_change_new_str1 t1
left join ${dim_dbname}.dim_dtk_org_level_info t2 on t1.org_id=t2.org_id and t1.org_company_name=t2.org_company_name
where to_date(t1.hired_date)='${pre1_date}' 
and t1.is_job=1
),
tmp_dtk_emp_org_change_str4 as ( -- 获取每天T-1和T-2只有一个组织的人员架构
select 
t1.emp_id,
t1.emp_name,
t3.org_id as old_org_id,
t3.org_name  as old_org_name,
t2.org_id as new_org_id,
t2.org_name as new_org_name,
t3.org_id_1 as old_org_id_1,
t3.org_name_1 as old_org_name_1,
t2.org_id_1 as new_org_id_1,
t2.org_name_1 as new_org_name_1,
t3.org_id_2 as old_org_id_2,
t3.org_name_2 as old_org_name_2,
t2.org_id_2 as new_org_id_2,
t2.org_name_2 as new_org_name_2,
t3.org_id_3 as old_org_id_3,
t3.org_name_3 as old_org_name_3,
t2.org_id_3 as new_org_id_3,
t2.org_name_3 as new_org_name_3,
t3.org_id_4 as old_org_id_4,
t3.org_name_4 as old_org_name_4,
t2.org_id_4 as new_org_id_4,
t2.org_name_4 as new_org_name_4,
t3.org_id_5 as old_org_id_5,
t3.org_name_5 as old_org_name_5,
t2.org_id_5 as new_org_id_5,
t2.org_name_5 as new_org_name_5,
t3.org_id_6 as old_org_id_6,
t3.org_name_6 as old_org_name_6,
t2.org_id_6 as new_org_id_6,
t2.org_name_6 as new_org_name_6,
t3.org_id_7 as old_org_id_7,
t3.org_name_7 as old_org_name_7,
t2.org_id_7 as new_org_id_7,
t2.org_name_7 as new_org_name_7,
t3.org_id_8 as old_org_id_8,
t3.org_name_8 as old_org_name_8,
t2.org_id_8 as new_org_id_8,
t2.org_name_8 as new_org_name_8,
t3.org_id_9 as old_org_id_9,
t3.org_name_9 as old_org_name_9,
t2.org_id_9 as new_org_id_9,
t2.org_name_9 as new_org_name_9,
t3.org_id_10 as old_org_id_10,
t3.org_name_10 as old_org_name_10,
t2.org_id_10 as new_org_id_10,
t2.org_name_10 as new_org_name_10,
t3.org_path_id as old_org_path_id,
t3.org_path_name as old_org_path_name,
t2.org_path_id as new_org_path_id,
t2.org_path_name as new_org_path_name,
'${pre1_date}' as org_change_date
from 
(
select 
a.emp_id,
a.emp_name,
a.org_id as new_org_id,
b.org_id as old_org_id,
a.hired_date as org_change_date,
a.org_company_name as new_org_company_name,
b.org_company_name as old_org_company_name
from 
tmp_dtk_emp_org_change_new_str1 a
left join tmp_dtk_emp_org_change_old_str2 b on a.emp_id=b.emp_id and a.org_company_name=b.org_company_name
where a.is_more=0 and b.is_more=0
and a.is_job=1
and b.is_job=1
and a.hired_date<>'${pre1_date}'
and a.org_id<>b.org_id
) t1
left join ${dim_dbname}.dim_dtk_org_level_info t2 on t1.new_org_id=t2.org_id and t1.new_org_company_name=t2.org_company_name
left join ${dim_dbname}.dim_dtk_org_level_info t3 on t1.old_org_id=t3.org_id and t1.old_org_company_name=t3.org_company_name

)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_org_change_info_df partition(d='${pre1_date}')
select 
emp_id,
emp_name,
old_org_id,
old_org_name,
new_org_id,
new_org_name,
old_org_id_1,
old_org_name_1,
new_org_id_1,
new_org_name_1,
old_org_id_2,
old_org_name_2,
new_org_id_2,
new_org_name_2,
old_org_id_3,
old_org_name_3,
new_org_id_3,
new_org_name_3,
old_org_id_4,
old_org_name_4,
new_org_id_4,
new_org_name_4,
old_org_id_5,
old_org_name_5,
new_org_id_5,
new_org_name_5,
old_org_id_6,
old_org_name_6,
new_org_id_6,
new_org_name_6,
old_org_id_7,
old_org_name_7,
new_org_id_7,
new_org_name_7,
old_org_id_8,
old_org_name_8,
new_org_id_8,
new_org_name_8,
old_org_id_9,
old_org_name_9,
new_org_id_9,
new_org_name_9,
old_org_id_10,
old_org_name_10,
new_org_id_10,
new_org_name_10,
old_org_path_id,
old_org_path_name,
new_org_path_id,
new_org_path_name,
org_change_date
from 
tmp_dtk_emp_org_change_str3

union all
select 
emp_id,
emp_name,
old_org_id,
old_org_name,
new_org_id,
new_org_name,
old_org_id_1,
old_org_name_1,
new_org_id_1,
new_org_name_1,
old_org_id_2,
old_org_name_2,
new_org_id_2,
new_org_name_2,
old_org_id_3,
old_org_name_3,
new_org_id_3,
new_org_name_3,
old_org_id_4,
old_org_name_4,
new_org_id_4,
new_org_name_4,
old_org_id_5,
old_org_name_5,
new_org_id_5,
new_org_name_5,
old_org_id_6,
old_org_name_6,
new_org_id_6,
new_org_name_6,
old_org_id_7,
old_org_name_7,
new_org_id_7,
new_org_name_7,
old_org_id_8,
old_org_name_8,
new_org_id_8,
new_org_name_8,
old_org_id_9,
old_org_name_9,
new_org_id_9,
new_org_name_9,
old_org_id_10,
old_org_name_10,
new_org_id_10,
new_org_name_10,
old_org_path_id,
old_org_path_name,
new_org_path_id,
new_org_path_name,
org_change_date
from 
tmp_dtk_emp_org_change_str4

union all
select 
emp_id,
emp_name,
old_org_id,
old_org_name,
new_org_id,
new_org_name,
old_org_id_1,
old_org_name_1,
new_org_id_1,
new_org_name_1,
old_org_id_2,
old_org_name_2,
new_org_id_2,
new_org_name_2,
old_org_id_3,
old_org_name_3,
new_org_id_3,
new_org_name_3,
old_org_id_4,
old_org_name_4,
new_org_id_4,
new_org_name_4,
old_org_id_5,
old_org_name_5,
new_org_id_5,
new_org_name_5,
old_org_id_6,
old_org_name_6,
new_org_id_6,
new_org_name_6,
old_org_id_7,
old_org_name_7,
new_org_id_7,
new_org_name_7,
old_org_id_8,
old_org_name_8,
new_org_id_8,
new_org_name_8,
old_org_id_9,
old_org_name_9,
new_org_id_9,
new_org_name_9,
old_org_id_10,
old_org_name_10,
new_org_id_10,
new_org_name_10,
old_org_path_id,
old_org_path_name,
new_org_path_id,
new_org_path_name,
org_change_date
from 
${dwd_dbname}.dwd_dtk_emp_org_change_info_df
where d='${pre2_date}'


"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

