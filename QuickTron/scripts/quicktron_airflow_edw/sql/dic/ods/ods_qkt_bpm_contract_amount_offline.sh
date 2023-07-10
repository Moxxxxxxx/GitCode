#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 datatron线下合同金额表
#-- 注意 ： 每天全量
#-- 输入表 : datatron.contract_amount
#-- 输出表 ：ods.ods_qkt_bpm_contract_amount_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-12 CREATE 

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
mysql_table=dim_contract_amount
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
signing_date,
dt_create_time,
dt_update_time
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： bpm 线下合同金额
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_bpm_contract_amount_offline
#-- 输出表 ：dim.dwd_bpm_contract_amount_offline_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-12 CREATE 
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

insert overwrite table ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful 
select
id, 
upper(pre_sales_number) as pre_sales_code, 
upper(official_number) as project_code, 
contract_approval_start_time, 
contract_approval_end_time, 
contract_no as contract_number, 
settlement_currency, 
currency_exchange_rate, 
contract_category, 
contract_amount, 
contract_status, 
signing_date
from 
${ods_dbname}.ods_qkt_bpm_contract_amount_offline
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

