#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场系统告警数据
#-- 注意 ： 每日T-1增量分区
#-- 输入表 : evo_rcs.notification_message,evo_basic.notification_message
#-- 输出表 ：ods.ods_qkt_rcs_notification_message_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-05 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin


######### 设置表的变量
ods_dbname=ods
table=ods_qkt_rcs_notification_message_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_rcs
mysql_table=notification_message
incre_column=last_updated_time
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
id,
unit_id,
message_id,
unit_type,
warning_type,
title,
service_name,
read_status,
status,
event,
notify_level,
happen_at,
close_at,
message_body,
compress_message_body,
warehouse_id,
created_user,
created_app,
created_time,
last_updated_user,
last_updated_app,
last_updated_time,
project_code
from evo_rcs.notification_message  
where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS

union all
select
id,
unit_id,
message_id,
unit_type,
warning_type,
title,
service_name,
read_status,
status,
event,
notify_level,
happen_at,
close_at,
message_body,
compress_message_body,
warehouse_id,
created_user,
created_app,
created_time,
last_updated_user,
last_updated_app,
last_updated_time,
project_code
from evo_basic.notification_message  
where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS" \
--num-mappers 2 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

