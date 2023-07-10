#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： sonar项目分支记录信息
#-- 注意 ：每日T-1快照日期
#-- 输入表 : quality_data.sonar_project_branches
#-- 输出表 ：ods.sonar_project_branches
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2021-12-20 modify split-by 使用非数字会造成数据重复
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_sonar_project_branches_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=sonar_project_branches
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




 /opt/module/sqoop-1.4.7/bin/sqoop import  -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
 uuid, project_uuid, kee, branch_type, merge_branch_uuid, pull_request_binary, manual_baseline_analysis_uuid, created_at, updated_at, exclude_from_purge, need_issue_sync
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 3 \
--split-by created_at \
--hcatalog-database $ods_dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


