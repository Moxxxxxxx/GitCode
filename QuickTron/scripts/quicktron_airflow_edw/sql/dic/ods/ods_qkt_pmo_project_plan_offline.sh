#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 datatron线下项目计划表
#-- 注意 ： 每天全量
#-- 输入表 : datatron.dim_project_plan
#-- 输出表 ：ods.ods_qkt_pmo_project_plan_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-17 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_pmo_project_plan_offline
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=datatron
mysql_table=dim_project_plan
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
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username root \
--password quicktron123456 \
--query "select
project_code, 
plan_mandaypm, 
plan_mandaype, 
plan_mandaylabor,
plan_mandaydev, 
plan_timeline_enter, 
plan_timeline_deliver, 
plan_timeline_golive, 
plan_timeline_acceptance,
plan_timeline_close, 
dt_create_time,
dt_update_time
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

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pmo 线下项目计划表
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_pmo_project_plan_offline
#-- 输出表 ：dim.dwd_pmo_project_plan_offline_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-17 CREATE 
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

insert overwrite table ${dwd_dbname}.dwd_pmo_project_plan_offline_info_df partition(d='${pre1_date}')
select
upper(project_code) as project_code, 
plan_mandaypm as plan_pm_manday, 
plan_mandaype as plan_pe_manday, 
plan_mandaylabor as plan_labor_manday, 
plan_mandaydev as plan_dev_manday, 
substr(plan_timeline_enter,1,10) as plan_enter_date, 
substr(plan_timeline_deliver,1,10) as plan_deliver_date, 
substr(plan_timeline_golive,1,10) as plan_golive_date, 
substr(plan_timeline_acceptance,1,10) as plan_acceptance_date, 
substr(plan_timeline_close,1,10) as plan_close_date
from
${ods_dbname}.ods_qkt_pmo_project_plan_offline
where d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

