#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm项目pm与spm的映射关系
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_bpm_ud_spm_df
#-- 输出表 ：dim.dim_bpm_ud_spm_mapping_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-16 CREATE 

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

echo "##############################################hive:{start executor dim}####################################################################"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dim_dbname}.dim_bpm_ud_spm_mapping_info_ful
select 
id,
pm_name,
pm_id,
spm_name,
spm_id
from 
(
select 
id,
pm as pm_name,
pmid as pm_id,
spm as spm_name,
spmid as spm_id,
row_number() over(partition by pmid,spmid order by id desc) as rn
from 
${ods_dbname}.ods_qkt_bpm_ud_spm_df
where d='${pre1_date}'
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

