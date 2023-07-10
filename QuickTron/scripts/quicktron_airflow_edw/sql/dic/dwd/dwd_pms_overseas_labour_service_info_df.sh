#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pms项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods_qkt_pms_overseas_labour_service_df、ods_qkt_pms_project_info_df、ods_qkt_pms_user_info_df
#-- 输出表 ：dwd.dwd_pms_overseas_labour_service_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-11-22 CREATE 
#-- 2 wangziming 2022-11-30 modify 增加分区逻辑

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
-- set hive.vectorized.execution.enabled = false; -- 解决Output column number expected to be 0 when isRepeating

with tmp_pms_user_str as (
select 
id,
lastname as user_name
from 
${ods_dbname}.ods_qkt_pms_user_info_df
where d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_pms_overseas_labour_service_info_df partition(d='${pre1_date}')
select 
a.id,
upper(b.xmbm) as project_code,
regexp_replace(b.xmmc,'\r|\n|\t','') as project_name,
a.lwgsmc as service_company_name,
a.ksrqz as start_date,
a.kqscz as attendance_duration,
cast(regexp_replace(a.cbrmb,',','') as decimal(18,2)) as rmb_cost,
a.rs as people_numbers,
a.jbsc as overtime_duration,
c.user_name as project_manager,
e.user_name as spm_name
from 
${ods_dbname}.ods_qkt_pms_overseas_labour_service_df a
left join ${ods_dbname}.ods_qkt_pms_project_info_df b on a.xmbm=b.id and b.d='${pre1_date}'
left join tmp_pms_user_str c on b.xmjl=c.id
left join tmp_pms_user_str e on b.spm=e.id
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
