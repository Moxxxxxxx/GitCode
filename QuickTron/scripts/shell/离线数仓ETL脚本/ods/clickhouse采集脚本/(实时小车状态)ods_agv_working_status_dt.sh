#!/bin/bash

######### 设置表的变量
dbname=ods
table=ods_agv_breakdown_detail_dt
target_dir=/user/hive/warehouse/ods.db/
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

##为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table/d=$pre1_date
if [ $? -eq 0 ] ;then 
    sql="use $dbname;
    alter table $table drop partition(d='$pre1_date');
    alter table $table add partition(d='$pre1_date');"
    echo "clean up and add partition $target_dir$table/d=$pre1_date"
else 
    sql="use $dbname;
    alter table $table add partition(d='$pre1_date');"
    echo "not clean up add partition $target_dir$table/d=$pre1_date" 
fi

$hive $hive_username $hive_passwd -e "$sql"

##设置datax变量
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=ods_agv_breakdown_detail_dt.json

##远程到172.31.237.4上进行datax数据采集脚本调用
#ssh -tt data_sqoop@172.31.237.4 <<effo
##执行命令
$datax  -p "-Dpre1_date=${pre1_date}" $json_dir$json_name

#exit
#effo

