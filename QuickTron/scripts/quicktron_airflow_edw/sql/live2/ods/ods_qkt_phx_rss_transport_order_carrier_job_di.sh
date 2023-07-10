#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目潜伏式机器人任务表
#-- 注意 ： 每日T-1增量分区
#-- 输入表 : phoenix_rss.transport_order_carrier_job
#-- 输出表 ：ods.ods_qkt_phx_rss_transport_order_carrier_job_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-02 CREATE 

# ------------------------------------------------------------------------------------------------
## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_phx_rss_transport_order_carrier_job_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=phoenix_rss
mysql_table=transport_order_carrier_job
incre_column=updated_date
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
create_time,
update_time, 
create_user,
update_user, 
job_sn, 
job_type, 
order_id, 
order_no, 
robot_code, 
robot_type_code, 
trace, 
warehouse_id, 
zone_code, 
calibrate_code, 
check_code, 
flag, 
floor, 
heading, 
job_group_id, 
job_state, 
lock_time, 
if(need_operation=0,'0','1') as need_operation,
priority, 
if(put_down=0,'0','1') as put_down,
rack_code, 
rack_move_type, 
rack_type_code, 
robot_end_point, 
\`sequence\`, 
\`source\`, 
source_point_code, 
station_code, 
target_point_code, 
line_code,
map_code, 
source_x, 
source_y, 
target_x, 
target_y, 
ticket_code, 
business_type, 
project_code
from $mysql_table  where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS"  \
--num-mappers 2 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


