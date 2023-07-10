#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pms-ar回款信息数据
#-- 注意 ： 每日全量分区
#-- 输入表 : ods_qkt_pms_user_info_df、ods_qkt_pms_project_ar_df、ods_qkt_pms_sales_territory_df、ods_qkt_pms_project_info_df
#-- 输出表 ：dwd.dwd_pms_project_ar_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-13 CREATE
#-- 2 wangziming 2022-12-14 modify 增加字段 质保日期 expiration_date
#-- 3 wangziming 2023-01-30 modify 修改currency 币种清洗规则（将人民币修改为CNY）
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
set hive.vectorized.execution.enabled = false; -- 解决Output column number expected to be 0 when isRepeating


with tmp_bus_assistant_str1 as (
select 
t1.id,
concat_ws(',',collect_list(t1.emp_code)) as bus_assistant_id,
concat_ws(',',collect_list(t2.lastname)) as bus_assistant_name
from 
(
select
a.id,
b.emp_code
from
${ods_dbname}.ods_qkt_pms_project_ar_df a
lateral view explode(split(if(nvl(a.swzl,'')='','unknown',a.swzl),',')) b as emp_code
where a.d='${pre1_date}'
) t1
left join ${ods_dbname}.ods_qkt_pms_user_info_df t2 on t1.emp_code=t2.id and t2.d='${pre1_date}'
group by t1.id
)
insert overwrite table ${dwd_dbname}.dwd_pms_project_ar_info_df partition(d='${pre1_date}')
select 
a.id,
a.htbh as contract_code,
upper(a.sqbh) as project_sale_code, 
upper(a.xmbm) as project_code,
regexp_replace(a.xmmc,'\r|\n|\t','') as project_name,
cast(a.wskje as decimal(18,2)) as uncollected_amount,
a.khmc as project_custormer_name,
a.yskts as ar_days,
a.htmc as contract_name,
a.ysrq as ar_date,
a.xmjd as project_operation_state,
cast(a.htzje as decimal(20,2)) as contract_amount,
b.bus_assistant_id,
b.bus_assistant_name,
if(a.bz='人民币','CNY',a.bz) as currency,
a.xmlx as project_type_name,
split(c.sjqy,' ')[0] as  project_area,
e.lastname as sales_manager,
a.sbdhqdrq as equitment_arrival_date,
a.sxqdrq as online_date,
a.zybgqdrq as final_inspection_date,
a.xmzshrq as after_sale_date,
a.lxrq as project_approval_date,
cast(a.skbl as decimal(5,2)) as collection_ratio, 
cast(a.skje as decimal(20,2)) as collection_amount,
a.skjd as ar_stage,
cast(a.yskje as decimal(20,2)) as already_collection_amount,
a.lcb as collection_stage,
cast(a.erprzje as decimal(20,2)) as erp_entry_amount,
f.zbdqrq as expiration_date
from 
${ods_dbname}.ods_qkt_pms_project_ar_df a
left join tmp_bus_assistant_str1 b on a.id=b.id
left join ${ods_dbname}.ods_qkt_pms_sales_territory_df c on a.xsqy=c.id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_user_info_df e on a.xsjl =e.id and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_project_info_df f on upper(a.xmbm) = upper(f.xmbm) and f.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
