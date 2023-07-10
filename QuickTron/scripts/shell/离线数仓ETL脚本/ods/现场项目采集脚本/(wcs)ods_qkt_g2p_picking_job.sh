#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_g2p_picking_job
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_wcs_g2p
mysql_table=picking_job
incre_column=updated_date
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


###################### picking_job->ods_qkt_g2p_picking_job(T-1每日增量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select id ,warehouse_id ,zone_code ,job_id ,job_type ,state ,agv_code 
,agv_type ,priority ,priority_type ,picking_order_group_id ,order_id ,order_detail_id,picking_work_id 
,picking_work_detail_id ,sku_id ,lot_id ,pack_id ,frozen ,package_type ,customer_code 
,level3_inventory_id ,quantity ,actual_quantity ,qty_mismatch_reason ,bucket_slot_code 
,bucket_code ,bucket_waypoint_code,bucket_face_num ,target_face_num ,station_code 
,station_slot_code ,station_waypoint_code,bucket_move_job_id ,job_mode ,created_app ,created_date ,updated_app 
,updated_date ,order_type ,order_group_type,project_code 
from $mysql_table  where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS"  \
--num-mappers 2 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


echo "#########################ods成功导入分区数据###############################"

################################ods初始化进行合并#############################################
echo '##############################################hive:{start executor ods}####################################################################'

##  T-2时间
if [ -n "$1" ] ;then
   pre2_date=`date -d "-1 day $1" +%F`

else 
    pre2_date=`date -d "-2 day" +%F`
fi


## 参数
columns=id,warehouse_id,zone_code,job_id,job_type,state,agv_code,agv_type,priority,priority_type,picking_order_group_id,order_id,order_detail_id,picking_work_id,picking_work_detail_id,sku_id,lot_id,pack_id,frozen,package_type,customer_code,level3_inventory_id,quantity,actual_quantity,qty_mismatch_reason,bucket_slot_code,bucket_code,bucket_waypoint_code,bucket_face_num,target_face_num,station_code,station_slot_code,station_waypoint_code,bucket_move_job_id,job_mode,created_app,created_date,updated_app,updated_date,order_type,order_group_type,project_code
### 第一次初始化脚本
init_sql="
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-- set hive.spark.client.server.connect.timeout=900000; -- 设置 hive客户端与spark driver的连接时长
set hive.execution.engine=mr;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table $table partition(d)
select 
$columns,
substr($incre_column,0,10) as d
from 
$table
where d='$pre1_date'
;
"

printf "##############################################start-executor-sql####################################################################\n$init_sql\n##############################################end-executor-sql####################################################################"

#$hive -e "$init_sql"


echo '##############################################hive:{end executor ods}####################################################################'


