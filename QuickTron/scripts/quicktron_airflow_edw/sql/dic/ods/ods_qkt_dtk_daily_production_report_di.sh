#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集钉钉-宝仓人员生产日报数据
#-- 注意 ： 每天增量，每天一个增量分区
#-- 输入表 : quality_data.dingtalk_daily_production_report
#-- 输出表 ：ods.ods_qkt_dtk_daily_production_report_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-12 CREATE
#-- 2 wangziming 2023-02-16 modify 增加字段frame_number,并进行初始化
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_dtk_daily_production_report_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=dingtalk_daily_production_report
datax_incre_column=datax_update_time
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

## hcatalog不支持文件覆盖，为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table/d=$pre1_date
if [ $? -eq 0 ] ;then 
    hdfs dfs -rm -r $target_dir$table/d=$pre1_date
    echo 'clean up'
else 
    echo 'not clean up' 
fi



 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
org_name, 
process_instance_id, 
attached_process_instance_ids, 
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid,
\`result\`, 
status, 
title,
production_date, 
work_order_number, 
process, 
product_part_number, 
model_code, 
product_name, 
agv_standard_time, 
harness_or_parts_standard_time, 
standard_time_minutes, 
plan_number, 
production_number, 
all_working_hours_minutes, 
all_losing_hours_minutes, 
semi_finished_attendance_efficiency_production_report, 
semi_finished_prodction_efficiency_production_report, 
finished_attendance_efficiency_production_report, 
finished_prodction_efficiency_production_report, 
inspection_number, 
prodction_efficiency_qualify, 
attendance_efficiency_qualify, 
loss_rate, operator, 
working_hours, 
individual_output_quantity, 
individual_output_hours, 
loss_ategory, 
accountability_unit, 
losing_hours, 
losing_description, 
dt_create_time, 
dt_update_time,
frame_number
from $mysql_table  
where date_format(${datax_incre_column},'%Y-%m-%d')=date_add('${pre1_date}',interval 1 day) and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"
