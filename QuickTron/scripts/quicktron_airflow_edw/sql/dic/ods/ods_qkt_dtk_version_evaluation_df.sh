#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_dtk_version_evaluation_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=version_evaluation
incre_column=upgrade_date
datax_incre_column=datax_update_time
#hive=/opt/module/hive-3.1.2/scripts/hive
hive=/opt/module/hive-3.1.2/bin/hive
hive_username=wangziming
hive_passwd=wangziming1

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
process_instance_id,
project_code,
project_name,
product_name,
project_type,
project_stage,
upgrade_res,
version,
upgrade_module,
upgrade_date,
applicant,
approver,
result,
status,
score,
score_remark,
evaluate,
first_examiner_user,
first_examiner,
first_time,
second_examiner_user,
second_examiner,
second_time,
third_examiner_user,
third_examiner,
third_time,
remark,
create_time,
finish_time,
bug_describe,
demand_describe,
project_first,
version_desc,
version_tag
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

