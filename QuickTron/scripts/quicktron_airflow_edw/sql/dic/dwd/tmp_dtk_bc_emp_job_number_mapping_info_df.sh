#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 宝仓员工
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : dim_dtk_emp_job_number_mapping_info、dwd_dtk_emp_info_df
#-- 输出表 ：tmp.tmp_dtk_bc_emp_job_number_mapping_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-09 CREATE 

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with old_dtk_emp_job_number as (
select 
org_company_name,
job_number,
emp_id,
emp_name
from 
${dim_dbname}.dim_dtk_emp_job_number_mapping_info
where d='${pre2_date}' and org_company_name='宝仓'
),
new_dtk_emp_job_number as (
select 
org_company_name,
job_number,
emp_id,
emp_name
from 
${dim_dbname}.dim_dtk_emp_job_number_mapping_info
where d='${pre1_date}' and org_company_name='宝仓'
),
quit_dtk_emp_job_number as ( -- 对比T-1,T-2的
select 
old.*
from 
old_dtk_emp_job_number old
left join new_dtk_emp_job_number new on new.emp_id=old.emp_id
where new.emp_id is null
)
insert overwrite table ${tmp_dbname}.tmp_dtk_bc_emp_job_number_mapping_info_df partition(d='${pre1_date}')
select 
org_company_name,
job_number, 
bc_emp_id, 
zn_emp_id,
emp_name,
is_job
from 
(
select 
*,
row_number() over(partition by bc_emp_id order by is_job desc) as rn
from 
(
select
a.org_company_name,
a.job_number, 
a.emp_id as bc_emp_id, 
b.emp_id as zn_emp_id,
a.emp_name,
'1' as is_job
from 
new_dtk_emp_job_number a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.job_number =b.job_number and b.d='${pre1_date}'

union all
select 
a.org_company_name,
a.job_number, 
a.emp_id as bc_emp_id, 
b.emp_id as zn_emp_id,
a.emp_name,
'0' as is_job
from 
quit_dtk_emp_job_number a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.job_number =b.job_number and b.d='${pre1_date}'


union all
select 
org_company_name,
job_number, 
bc_emp_id, 
zn_emp_id,
emp_name,
is_job
from 
${tmp_dbname}.tmp_dtk_bc_emp_job_number_mapping_info_df
where d='${pre2_date}' and is_job='0'
) t
) rt
where rt.rn=1
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"




