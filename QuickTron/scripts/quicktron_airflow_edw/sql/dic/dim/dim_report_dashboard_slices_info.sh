#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： report可视化报表的看板与图表映射关系
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_report_dashboard_slices_ful、ods.ods_qkt_report_slices_ful、ods.ods_qkt_report_dashboards_ful
#-- 输出表 ：dim.dim_report_dashboard_slices_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-12 CREATE 
#-- 2 wangziming 2022-06-08 modify 增加 is_dashboard_publish （看板是否发布）

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

echo "##############################################hive:{start executor dwd}####################################################################"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dim_dbname}.dim_report_dashboard_slices_info
select
a.id,
dashboard_id,
c.dashboard_title as dashboard_name,
slice_id,
b.slice_name,
c.published as is_dashboard_publish
from 
${ods_dbname}.ods_qkt_report_dashboard_slices_ful a
left join ${ods_dbname}.ods_qkt_report_slices_ful b on a.slice_id=b.id
left join ${ods_dbname}.ods_qkt_report_dashboards_ful c on a.dashboard_id=c.id
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

