#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp（kingdee）项目维度表映射关系
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_kde_bd_project_df
#-- 输出表 ：dim.dim_kde_bd_project_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-29 CREATE 

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


insert overwrite table ${dim_dbname}.dim_kde_bd_project_info
select
  fid as id ,
  upper(fnumber) project_code,
  fcreatorid as creator_id,
  fcreatedate as create_time,
  fmodifydate as modify_time,
  fauditorid as auditor_id,
  fauditdate as audit_time,
  fdocumentstatus as document_status,
  fforbidderid as forbidder_id,
  fforbiddate as forbid_date,
  case when upper(fforbidstatus)='A' then '1' 
  	   when upper(fforbidstatus)='B' then '0'
  	   else '-1' end as is_forbid,
  fcreateorgid as  create_org_id,
  fuseorgid as use_org_id,
  fmodifierid as modifier_id,
  fprojectgroup as project_group,
  f_paez_orgid as paez_org_id
from 
${ods_dbname}.ods_qkt_kde_bd_project_df
where d='${pre1_date}'

;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

