#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 datatron线下合同金额表
#-- 注意 ： 每天全量
#-- 输入表 : datatron.contract_amount
#-- 输出表 ：ods.ods_qkt_bpm_contract_amount_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-12 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_bpm_contract_amount_offline
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=datatron
mysql_table=contract_amount
hive=/opt/module/hive-3.1.2/bin/hive

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



 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username root \
--password quicktron123456 \
--query "select
id, 
pre_sales_number, 
official_number, 
contract_approval_start_time, 
contract_approval_end_time, 
contract_no, 
settlement_currency, 
currency_exchange_rate, 
contract_category, 
contract_amount, 
contract_status, 
signing_date
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

echo "#########################ods成功导入分区数据###############################"
