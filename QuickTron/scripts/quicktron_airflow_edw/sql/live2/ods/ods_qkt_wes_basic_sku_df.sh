#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 wes 商品基本信息
#-- 注意 ： 每日按天全量分区
#-- 输入表 : evo_wes_basic.basic_sku
#-- 输出表 ：ods.ods_qkt_wes_basic_sku_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-05 CREATE 

# ------------------------------------------------------------------------------------------------
## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_wes_basic_sku_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_wes_basic
mysql_table=basic_sku
incre_column=last_updated_time
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
--query "select id,
owner_id,
sku_code,
sku_name,
batch_enabled,
sn_enabled,
lot_barcode_enabled,
over_weight_flag,
upper_limit_quantity,
lower_limit_quantity,
image_url,
expiration_date,
near_expiration_date,
spec,
supplier,
abc_category,
major_category,
medium_category,
minor_category,
mutex_category,
state,
udf1,
udf2,
udf3,
udf4,
udf5,
created_user,
created_app,
created_time,
last_updated_user,
last_updated_app,
last_updated_time,
extended_field,
project_code,
pick_enabled,
replenish_enabled,
cycle_count_enabled,
expiry_date_enabled,
warehouse_id
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 2 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"



