#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： bpm项目基础表
#-- 注意 ： 每天全量跑
#-- 输入表 : ods.ods_qkt_bpm_pm_project_df
#-- 输出表 ：dwd.dwd_bpm_project_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-16 CREATE 
#-- 2 wangziming 2022-02-16 modify 修改字段 pm 为pm_name、增加字段spm_name,spm_id、修改字段规则mprojectcode
#-- 3 wangziming 2022-03-15 modify 增加字段 project_attr_ft 项目所属ft
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_bpm_project_info_ful
select 
a.id,
mprojectname as mproject_name,
upper(regexp_replace(mprojectcode,'\\\\s+','')) as mproject_code,
if(length(ccode)=1,regexp_replace(ccode,'-',''), ccode) as customer_code,
if(length(cname)=1,regexp_replace(cname,'-',''), cname) as customer_name,
cclass as customer_class,
upper(regexp_replace(pcode,'\\\\s+','')) as project_code,
pname as project_name,
ptype as project_type,
if(length(pclass)=1,regexp_replace(pclass,'-',''), pclass) as project_class,
if(length(psubject)=1,regexp_replace(psubject,'-',''), psubject) as project_subject,
if(length(pstatus)=1,regexp_replace(pstatus,'-',''), pstatus) as project_status,
a.pm as pm_name,
a.pmid as pm_id,
regexp_replace(briefinfo,'\n|\r|\t|-','') as brief_info,
if(length(priority)=1,regexp_replace(priority,'-',''), priority) as priority,
if(length(fqdept)=1,regexp_replace(fqdept,'-',''), fqdept) as dept_name,
if(length(fqdeptid)=1,regexp_replace(fqdeptid,'-',''), fqdeptid) as deptid,
if(length(deliverable)=1,regexp_replace(deliverable,'-',''), deliverable) as deliverable,
pmo,
pmoid as pmo_id,
pmstart as project_start_time,
pmend as project_end_time,
flowid as flow_id,
cost, 
area,
salespersonnel as sales_person,
salespersonnelid as sales_person_id,
salesareadirector as sales_area_director,
landingarea as landing_area,
salesareadirectorid as sales_area_director_id,
shouqianguwen as pre_sales_consultant,
shouqianguwenid as pre_sales_consultant_id,
b.spm as spm_name,
b.spmid as spm_id,
a.ft as project_attr_ft
from 
${ods_dbname}.ods_qkt_bpm_pm_project_df a
left join ${ods_dbname}.ods_qkt_bpm_ud_spm_df b on a.pmid=b.pmid and b.d='${pre1_date}'
where a.d='${pre1_date}' and pcode not rlike '[\u4e00-\u9fa5]' and length(pcode)>=6
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

