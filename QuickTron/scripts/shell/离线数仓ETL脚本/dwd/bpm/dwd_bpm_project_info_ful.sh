#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
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


use $dbname;
insert overwrite table dwd.dwd_bpm_project_info_ful
select 
id,
mprojectname as mproject_name,
mprojectcode as mproject_code,
if(length(ccode)=1,regexp_replace(ccode,'-',''), ccode) as customer_code,
if(length(cname)=1,regexp_replace(cname,'-',''), cname) as customer_name,
cclass as customer_class,
pcode as project_code,
pname as project_name,
ptype as project_type,
if(length(pclass)=1,regexp_replace(pclass,'-',''), pclass) as project_class,
if(length(psubject)=1,regexp_replace(psubject,'-',''), psubject) as project_subject,
if(length(pstatus)=1,regexp_replace(pstatus,'-',''), pstatus) as project_status,
pm,
pmid as pm_id,
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
shouqianguwenid as pre_sales_consultant_id
from 
ods_qkt_bpm_pm_project_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
