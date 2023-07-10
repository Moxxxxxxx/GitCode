#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 任务工时状态记录，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_manhour_df,dwd.dwd_ones_task_info_ful、dwd.dwd_ones_org_user_info_ful
#-- 输出表 ：dwd.dwd_ones_task_manhour_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 
#-- 2 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp
#-- 3 wangziming 2022-02-10 modify 增加 project_classify_name 字段（项目名称分类）,并过滤掉已删除的ones任务
#-- 4 wangziming 2022-03-16 modify 增减字段 user_name ,花费工时字段需要进行 除以100000数字进行获取
#-- 5 wangziming 2022-04-21 modify 增加字段 ones项目组类型 project_type_name
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


insert overwrite table ${dwd_dbname}.dwd_ones_task_manhour_info_ful
select
a.uuid,
a.team_uuid,
a.task_uuid,
a.user_uuid,
to_utc_timestamp(a.start_time*1000,'GMT-8') as task_start_time,
a.hours / 100000 as task_spend_hours,
a.remark,
a.status,
to_utc_timestamp(a.create_time*1000,'GMT-8') as task_create_time,
a.type as task_type,
a.end_time as task_end_time,
b.project_classify_name,
c.user_name,
b.project_type_name
from 
${ods_dbname}.ods_qkt_ones_manhour_df a
left join ${dwd_dbname}.dwd_ones_task_info_ful b on b.uuid=a.task_uuid
left join ${dwd_dbname}.dwd_ones_org_user_info_ful c on a.user_uuid=c.uuid
where a.d='${pre1_date}' and a.status='1'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


