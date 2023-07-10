#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pms项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods_qkt_pms_wbs_change_detail_df、ods_qkt_pms_wbs_change_main_df、ods_qkt_pms_project_info_df、ods_qkt_pms_user_info_df
#-- 输出表 ：dwd.dwd_pms_wbs_change_detail_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-11-17 CREATE 

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

insert overwrite table ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df partition(d='${pre1_date}')
select 
a.id,
regexp_replace(split(a.wbsmc,'<br>')[0],'\r|\n|\t| ','') as wbs_name,
a.yjhksrq as soure_plan_start_date,
a.yjhjsrq as soure_plan_end_date,
a.zrr as principal_id,
e.lastname as principal_name,
a.sjgbrq as actual_close_date,
regexp_replace(a.wdjd,'\r|\n|\t','') as project_stage,
regexp_replace(a.jd,'\r|\n|\t','')  as project_node,
a.xjhksrq as current_plan_start_date,
a.xjhjsrq as current_plan_end_date,
case when a.sfbg='是' then '1'
     when a.sfbg='否' then '0'
     else null end as is_change,
a.wbsrwzt as wbs_task_state,
case when a.lcb='是' then '1'
     when a.lcb='否' then '0'
     else null end as is_milestone,
a.bz as remark,
a.wbscj as wbs_level,
a.rwxh as task_num,
a.yyjrt as source_plan_man_day,
a.xyjrt as current_plan_man_day,
upper(c.xmbm) as project_code,
regexp_replace(c.xmmc,'\r|\n|\t','') as project_name,
b.spm as spm_id,
f.lastname as spm_name
from 
${ods_dbname}.ods_qkt_pms_wbs_change_detail_df a
left join ${ods_dbname}.ods_qkt_pms_wbs_change_main_df b on a.main_id=b.id and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_project_info_df c on b.xmbm=c.id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_user_info_df e on a.zrr=e.id and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_user_info_df f on b.spm=f.id and f.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
