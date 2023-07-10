#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉组织历史架构表
#-- 注意 ： 每天分区历史表数据
#-- 输入表 : dim.dim_dtk_org_level_info 、 ods.ods_qkt_dtk_dingtalk_department_df
#-- 输出表 ：dim.dim_dtk_org_history_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-14 CREATE 


# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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





sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



with dtk_dingtalk_department_str1 as (
select 
id as org_id,
name as org_name,
parent_id as org_parent_id
from 
${ods_dbname}.ods_qkt_dtk_dingtalk_department_df
where d='${pre1_date}' 
and (nvl(org_name,'')='' or  org_name='上海快仓智能科技有限公司')
)
insert overwrite table ${dim_dbname}.dim_dtk_org_history_info_df partition (d='${pre1_date}')
select 
a.org_id,
a.org_name,
b.org_parent_id as parent_org_id,
c.org_name as parent_org_name,
a.org_id_2 as dept_org_id,
a.org_name_2 as dept_org_name,
a.org_path_id,
a.org_path_name,
case  a.org_level_num when 1 then 'company'
	                  when 2 then 'dept'
	              else 'team' end as org_type,
if(a.org_level_num in (1,2),null,concat('team',a.org_level_num-2)) as org_type_level
from 
${dim_dbname}.dim_dtk_org_level_info a
left join dtk_dingtalk_department_str1 b on a.org_id=b.org_id
left join dtk_dingtalk_department_str1 c on b.org_parent_id=c.org_id
where a.org_name_1='上海快仓智能科技有限公司'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

