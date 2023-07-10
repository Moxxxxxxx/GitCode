#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_basic_bucket_type
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_basic
mysql_table=basic_bucket_type
incre_column=last_updated_time
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



###################### basic_bucket_type->ods_qkt_basic_bucket_type(T-1每日全量->分区)
/opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
  id,
  bucket_type_code,
  warehouse_id,
  virtual_type,
  move_type,
  walk_through,
  apply_type,
  length,
  width,
  height,
  weight_limit,
  available_length,
  available_width,
  available_height,
  layer_layout,
  layer_color,
  work_face,
  deliver_face,
  parcel_collect_face,
  deny_enter_face,
  face_count,
  layer_count,
  leg_diameter,
  state,
  discern_bucket_code,
  fork_height,
  fork_base_height,
  texture_type,
  created_time,
  created_user,
  created_app,
  last_updated_time,
  last_updated_user,
  last_updated_app,
  project_code
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


echo "#########################ods成功导入分区数据###############################"


:<<eof
#################################################################dwd#################################################################
#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
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


use $dbname;
insert overwrite table dwd.dwd_basic_bucket_type_info partition(d,pt)
select 
  id,
  bucket_type_code,
  warehouse_id,
  virtual_type,
  move_type,
  walk_through,
  apply_type,
  length,
  width,
  height,
  weight_limit,
  available_length,
  available_width,
  available_height,
  layer_layout,
  layer_color,
  work_face,
  deliver_face,
  parcel_collect_face,
  deny_enter_face,
  face_count,
  layer_count,
  leg_diameter,
  state as bucket_state,
  discern_bucket_code,
  fork_height,
  fork_base_height,
  texture_type,
  created_time as bucket_created_time,
  created_user as bucket_created_user,
  created_app as bucket_created_app,
  last_updated_time as bucket_updated_time,
  last_updated_user as bucket_updated_user,
  last_updated_app as bucket_updated_app,
  project_code,
  '$pre1_date' as d,
project_code as pt
from 
ods_qkt_basic_bucket_type
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
eof
