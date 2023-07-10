#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset可视化报表用户行为日志记录
#-- 注意 ： 每日的分区就是当天的日志记录数据
#-- 输入表 : ods.ods_qkt_report_logs_da、dim.dim_report_dashboard_user_info、dim.dim_report_dashboard_slices_info
#-- 输出表 ：dwd.dwd_report_action_log_info_da
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-12 CREATE 
#-- 2 wangziming 2021-12-16_1 midify 增加角色权限字段，图表id进行去重

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


insert overwrite table ${dwd_dbname}.dwd_report_action_log_info_da partition(d)
select
a.id,
a.\`action\` as user_action,
case when a.\`action\`='welcome' then '登录操作'
     when a.\`action\`='sql_json' then 'sql查询操作'
     when a.referrer like concat('http://superset.flashhold.com/superset/dashboard','%') then '看板操作'
     else '图表操作' end as user_action_name,
a.user_id,
b.user_cname,
a.dttm as record_create_time,
a.dashboard_id,
c.dashboard_name,
a.slice_id,
e.slice_name,
a.duration_ms,
concat(from_unixtime(cast(substr(unix_timestamp(a.dttm,'yyyy-MM-dd HH:mm:ss')*1000-a.duration_ms,0,10) as bigint),'yyyy-MM-dd HH:mm:ss') ,'.',substr(unix_timestamp(a.dttm,'yyyy-MM-dd HH:mm:ss')*1000-a.duration_ms,11,13)) as record_start_time,
a.referrer as url_address,
b.role_id,
b.role_cname,
substr(dttm,1,10) as d
from 
${ods_dbname}.ods_qkt_report_logs_da a
left join ${dim_dbname}.dim_report_dashboard_user_info b on a.user_id=b.id
left join (select distinct dashboard_id,dashboard_name from ${dim_dbname}.dim_report_dashboard_slices_info) c on if(nvl(a.dashboard_id,'')<>'',a.dashboard_id,'UNKNOWN')=c.dashboard_id
left join (select DISTINCT slice_id,slice_name from ${dim_dbname}.dim_report_dashboard_slices_info) e on if(nvl(a.slice_id,'')<>'',a.slice_id,'UNKNOWN')=e.slice_id
where a.d>'2021-11-11' and a.d<>'2021-11-12' and (a.\`action\` in ('welcome','sql_json') or a.referrer like concat('http://superset.flashhold.com/superset/dashboard','%') or (a.action='explore_json' and nvl(a.dashboard_id,'')='' and a.slice_id>0))
;


"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_report_action_log_info_da partition(d='${pre1_date}')
select
a.id,
a.\`action\` as user_action,
case when a.\`action\`='welcome' then '登录操作'
     when a.\`action\`='sql_json' then 'sql查询操作'
     when a.referrer like concat('http://superset.flashhold.com/superset/dashboard','%') then '看板操作'
     else '图表操作' end as user_action_name,
a.user_id,
b.user_cname,
a.dttm as record_create_time,
a.dashboard_id,
c.dashboard_name,
a.slice_id,
e.slice_name,
a.duration_ms,
concat(from_unixtime(cast(substr(unix_timestamp(a.dttm,'yyyy-MM-dd HH:mm:ss')*1000-a.duration_ms,0,10) as bigint),'yyyy-MM-dd HH:mm:ss') ,'.',substr(unix_timestamp(a.dttm,'yyyy-MM-dd HH:mm:ss')*1000-a.duration_ms,11,13)) as record_start_time,
a.referrer as url_address,
b.role_id,
b.role_cname
from 
${ods_dbname}.ods_qkt_report_logs_da a
left join ${dim_dbname}.dim_report_dashboard_user_info b on a.user_id=b.id
left join (select distinct dashboard_id,dashboard_name from ${dim_dbname}.dim_report_dashboard_slices_info) c on if(nvl(a.dashboard_id,'')<>'',a.dashboard_id,'UNKNOWN')=c.dashboard_id
left join  (select DISTINCT slice_id,slice_name from ${dim_dbname}.dim_report_dashboard_slices_info) e on if(nvl(a.slice_id,'')<>'',a.slice_id,'UNKNOWN')=e.slice_id
where a.d='${pre1_date}' and (a.\`action\` in ('welcome','sql_json') or a.referrer like concat('http://superset.flashhold.com/superset/dashboard','%') or (a.action='explore_json' and nvl(a.dashboard_id,'')='' and a.slice_id>0))
;

"


echo -e "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


