#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=dim
table=dim_virtual_org_emp_info_offline
outdir=/data/sqoop/logs/hcatalog
target_dir=/user/hive/warehouse/dim.db/
mysql_dbname=ads
mysql_table=ads_virtual_org_emp_info_offline


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

## hcatalog不支持文件覆盖，为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table
if [ $? -eq 0 ] ;then 
    hdfs dfs -rm -r $target_dir$table/*
    echo 'clean up'
else 
    echo 'not clean up' 
fi

###################### ads_virtual_org_emp_info_offline->dim_virtual_org_emp_info_offline
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username root \
--password quicktron123456 \
--query "select 
emp_code,
emp_name,
role_type,
module_branch,
org_id,
org_name,
virtual_org_name,
is_active,
create_time,
update_time
from $mysql_table where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database $dbname \
--hcatalog-table $table \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


echo "#########################ods成功导入分区数据###############################"
