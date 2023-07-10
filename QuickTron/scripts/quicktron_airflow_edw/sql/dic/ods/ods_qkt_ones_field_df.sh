#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集ones全局属性记录
#-- 注意 ： 每天全量，每天一个快照
#-- 输入表 : quality_data.field
#-- 输出表 ：ods.ods_qkt_ones_field_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2022-08-30 modify 增加离线维度字段列的关联关系表

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_ones_field_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=field
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
uuid, team_uuid, \`type\`, name, name_pinyin, description, default_value, renderer, filter_option, search_option, create_time, built_in, step_settings, stay_settings, related_type, related_uuid, status, appear_time_settings
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by task_uuid \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"






sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ods.ods_qkt_ones_field_df partition(d='${pre1_date}')
select 
uuid, 
team_uuid, 
type, 
if(b.attribute_key is not null,b.attribute_value,a.name) as name, 
name_pinyin, 
description, 
default_value, 
renderer, 
filter_option, 
search_option, 
create_time, 
built_in, 
step_settings, 
stay_settings, 
related_type, 
related_uuid, 
status, 
appear_time_settings
from 
ods.ods_qkt_ones_field_df a
left join dim.dim_ones_attribute_info_offline b on regexp_replace(a.name,'[\}\{]','')=b.attribute_key
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


