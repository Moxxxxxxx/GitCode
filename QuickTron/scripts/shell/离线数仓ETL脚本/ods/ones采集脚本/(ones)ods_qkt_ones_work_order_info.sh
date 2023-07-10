#!/bin/bash
# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones工单数据
#-- 注意 ：  每日t-1全量分区
#-- 输入表 : quality_data.ones_work_order_info
#-- 输出表 ：ods.ods_qkt_ones_work_order_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-01-05 modify 增加字段
# --------------------------------------------------------------------------------------------------


## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_ones_work_order_info
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=ones_work_order_info
incre_column=update_time
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


###################### ones_work_order_info->ods_qkt_ones_work_order_info(T-1每日全量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
 ones_work_order_uuid,
ticket_id,
project_code,
case_origin_code,
problem_type,
case_status,
status_time,
owner_name,
created_time,
close_name,
first_category,
second_category,
memo,
supplement,
dealsteps,
work_order_status,
duty_type,
feedback_tel,
contact_user,
create_user,
sign_delete,
create_time,
update_time,
third_category,
second_class,
third_class
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"
