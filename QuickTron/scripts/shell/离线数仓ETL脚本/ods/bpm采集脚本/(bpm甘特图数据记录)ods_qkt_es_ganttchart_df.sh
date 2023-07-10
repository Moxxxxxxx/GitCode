#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： bpm甘特图数据记录
#-- 注意 ：  每日T-1全量分区
#-- 输入表 : bpm.es_ganttchart
#-- 输出表 ：ods.ods_qkt_bpm_es_ganttchart_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-22 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_bpm_es_ganttchart_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=bpm
mysql_table=es_ganttchart
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
ganttname, 
level, 
haschild, 
status, 
progress, 
progressbyworklog, 
relevance, 
\`type\`, 
typeid, 
description, 
code, 
depends, 
dependsnew, 
canwrite, 
ganttchartstart, 
duration, 
ganttchartend, 
startismilestone, 
endismilestone,
collapsed, 
assigs, 
ganttid, 
projectid, 
flowmodelid, 
gantttype, 
objecttype, 
objectid, 
objectname, 
ganttsort, 
flowid,
ganttmark, 
ganttactualstart, 
ganttactualend, 
gantttimelimit, 
ganttdeviationdays, 
tname, 
taskreport, 
nametype, 
repeat_start, 
budgeted_cost, 
actual_cost, 
associated_contract, 
receivables, 
zhuangtai, 
earned_value_ev, 
cost_deviation_cv, 
progress_deviation_sv, 
cost_performance_cpi, 
progress_performance_spi
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


echo "#########################ods成功导入分区数据###############################"
