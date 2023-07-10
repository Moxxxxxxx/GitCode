#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目wes 入库单
#-- 注意 ： 每日按天增量分区
#-- 输入表 : evo_wes_replenish.replenish_order
#-- 输出表 ：ods.ods_qkt_replenish_order_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-25 CREATE 

# ------------------------------------------------------------------------------------------------
## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_replenish_order_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_wes_replenish
mysql_table=replenish_order
incre_column=last_updated_date
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
replenish_order_number,
sn_unique_assist_key,
tenant_id,
owner_code,
external_id,
external_order_number,
order_type,
priority_type,
priority_value,
state,
feedback_state,
order_date,
done_date,
done_user,
container_count,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
udf1,
udf2,
udf3,
udf4,
udf5,
created_date,
created_user,
created_app,
last_updated_date,
last_updated_user,
last_updated_app,
project_code
from $mysql_table  where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS"  \
--num-mappers 2 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"



