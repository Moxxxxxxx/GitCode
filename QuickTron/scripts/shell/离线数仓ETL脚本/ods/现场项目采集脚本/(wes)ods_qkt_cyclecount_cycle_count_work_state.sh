#!/bin/bash


## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin


######### 设置表的变量
dbname=ods
table=ods_qkt_cyclecount_cycle_count_work_state_change
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_wes_cyclecount
mysql_table=cycle_count_work_state_change
incre_column=last_updated_date
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


###################### cycle_count_work_state_change->ods_qkt_cyclecount_cycle_count_work_state_change(T-1每日增量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
id,
warehouse_id,
cycle_count_work_id,
state,
remark,
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
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

echo "#########################ods成功导入分区数据###############################"





################################odsT-1增量与T-2全量进行合并#############################################
echo '##############################################hive:{start executor ods}####################################################################'

##  T-2时间
if [ -n "$1" ] ;then
   pre2_date=`date -d "-1 day $1" +%F`

else 
    pre2_date=`date -d "-2 day" +%F`
fi


## 参数
columns=id,warehouse_id,cycle_count_work_id,state,remark,created_date,created_user,created_app,last_updated_date,last_updated_user,last_updated_app,project_code

init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

use $dbname;
insert overwrite table $table partition (d)
select 
$columns,
substr(last_updated_date,0,10) as d
from 
$table
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

##$hive -e "$init_sql"


echo '##############################################hive:{end executor ods}####################################################################'

:<<eof
#################################################################dwd#################################################################


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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_cyclecount_cycle_count_work_state_change_info partition(d,pt)
select 
id,
warehouse_id,
cycle_count_work_id,
state as work_state,
remark,
created_date as work_created_time,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_time,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
project_code,
d,
project_code as pt
from 
ods_qkt_cyclecount_cycle_count_work_state_change 
;
"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_cyclecount_cycle_count_work_state_change_info partition(d='$pre1_date',pt)
select 
id,
warehouse_id,
cycle_count_work_id,
state as work_state,
remark,
created_date as work_created_time,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_time,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
project_code,
project_code as pt
from 
ods_qkt_cyclecount_cycle_count_work_state_change
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
eof


