#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 手动执行一次
#-- 参数 ：      
#-- 功能描述 ： git代码提交者的信息
#-- 注意 ：初始化一次
#-- 输入表 : quality_data.auth_user
#-- 输出表 ：ods.ods_qkt_git_auth_user_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-03 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_git_auth_user_ful
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=auth_user
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




###################### Example_classify_linkage->ods_qkt_bpm_example_classify_linkage(T-1每日全量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
id, password, last_login, is_superuser, username, first_name, last_name, email, is_staff, is_active, date_joined, group_id
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 3 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


echo "#########################ods成功导入分区数据###############################"


