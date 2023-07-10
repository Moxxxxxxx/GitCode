#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集携程酒店的月结账单结算信息表
#-- 注意 ： 每天增量每月数据，每天一个快照
#-- 输入表 : quality_data.ctrip_hotel_account_check_data_list
#-- 输出表 ：ods.ods_qkt_ctrip_hotel_account_check_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-24 CREATE 初始化数据
#-- 2 wangziming 2022-06-28 modify 重新初始化数据，并添加字段 etd

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_ctrip_hotel_account_check_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=ctrip_hotel_account_check_data_list
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
orderid,
status,
uid,
ctripcardno,
name,
clients,
workcity,
orderdate,
eta,
city,
hotelname,
hotelename,
roomname,
roomcount,
star,
quantity,
price,
amount,
remarks,
corp_paytype,
iscu,
costcenter,
costcenter2,
costcenter3,
costcenter4,
costcenter5,
costcenter6,
dept1,
dept2,
dept3,
hotelrelatedjourneyno,
htlclass,
rebate,
vatdesc,
emptycolumn,
corpid,
accountid,
batchno,
subbatchno,
startdate,
enddate,
batchstatus,
create_time,
update_time,
etd
from $mysql_table  where date_format(${datax_incre_column},'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS"  \
--num-mappers 1 \
--split-by uuid \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

