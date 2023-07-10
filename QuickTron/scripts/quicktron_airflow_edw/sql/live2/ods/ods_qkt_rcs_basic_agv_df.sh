#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集
#-- 注意 ： 每天全量,每天一个分区
#-- 输入表 : evo_rcs.basic_agv
#-- 输出表 ：ods.ods_qkt_rcs_basic_agv_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-01-06 CREATE 
#-- 2 wangziming 2022-02-14 modify 增加无网项目从实时过来采集

# ------------------------------------------------------------------------------------------------



## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_rcs_basic_agv_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_rcs
mysql_table=basic_agv
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
--query "select id,
agv_code,
warehouse_id,
zone_code,
zone_collection,
agv_type_id,
agv_frame_code,
drive_unit_version,
ip,
dsp_version,
battery_version,
radar_version,
camera_version,
os,
command_version,
product_version,
dbox_version,
iot_version,
disk_space_percent,
state,
created_time,
created_user,
created_app,
last_updated_time,
last_updated_user,
last_updated_app,
bucket_code,
project_code,
assign_charger_code
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"




#!/bin/bash
# 如果是输入的日期按照取输入日期；如没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi


##设置datax变量
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=ods_qkt_rcs_basic_agv_df.json

##执行命令
$datax  -p "-Dpre1_date=${pre1_date}" $json_dir$json_name

:<<eof
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert into table ${ods_dbname}.ods_qkt_rcs_basic_agv_df partition(d='${pre1_date}')
select 
id,
agv_code,
warehouse_id,
zone_code,
zone_collection,
agv_type_id,
agv_frame_code,
drive_unit_version,
ip,
dsp_version,
battery_version,
radar_version,
camera_version,
os,
command_version,
product_version,
dbox_version,
iot_version,
disk_space_percent,
state,
created_time,
created_user,
created_app,
last_updated_time,
last_updated_user,
last_updated_app,
bucket_code,
project_code,
assign_charger_code
from 
${ods_dbname}.ods_qkt_rcs_basic_agv_df
where d='2021-12-30' and project_code in('A51240','A51297')
"


$hive -e "$sql"

eof
