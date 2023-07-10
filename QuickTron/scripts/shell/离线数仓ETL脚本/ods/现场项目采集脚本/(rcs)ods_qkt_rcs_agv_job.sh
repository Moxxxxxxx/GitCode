#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_rcs_agv_job
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_rcs
mysql_table=agv_job
incre_column_1=gmt_modified
incre_column_2=update_time
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


###################### agv_job->ods_qkt_rcs_agv_job(T-1每日增量量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
id,
create_time,
update_time,
action_point_code,
action_state,
agv_code,
agv_id,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create,
gmt_create_user,
gmt_modified,
gmt_modified_user,
let_down_flag,
mark_canceling,
project_code
from $mysql_table  where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day)  and \$CONDITIONS"  \
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


################################ods初始化进行合并#############################################
echo '##############################################hive:{start executor ods}####################################################################'

##  T-2时间
if [ -n "$1" ] ;then
   pre2_date=`date -d "-1 day $1" +%F`

else 
    pre2_date=`date -d "-2 day" +%F`
fi


## 参数
columns=id,create_time,update_time,action_point_code,action_state,agv_code,agv_id,bucket_id,bucket_point_code,can_interrupt,dest_point_code,is_let_down,is_report_event,job_context,job_id,job_mark,job_priority,job_state,job_type,own_job_type,src_job_type,top_face_list,top_face,warehouse_id,zone_code,gmt_create,gmt_create_user,gmt_modified,gmt_modified_user,let_down_flag,mark_canceling,project_code
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
substr(coalesce($incre_column_1,$incre_column_2),0,10) as d
from 
$table
where d='$pre1_date'
;
"

printf "##############################################start-executor-sql####################################################################\n$init_sql\n##############################################end-executor-sql####################################################################"

#$hive -e "$init_sql"


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
insert overwrite table dwd.dwd_rcs_agv_job_info partition(d,pt)
select 
id,
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by if(nvl(gmt_modified,'')<>'',gmt_modified,update_time) desc ) as rn 
from
ods_qkt_rcs_agv_job 
) t
where t.rn=1
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
insert overwrite table dwd.dwd_rcs_agv_job_info partition(d,pt)
select 
id,
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by if(nvl(gmt_modified,'')<>'',gmt_modified,update_time) desc) as rn
from ods_qkt_rcs_agv_job
where d>=date_sub('$pre1_date',7) and substr(coalesce(create_time,gmt_create),0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo "##############################################hive:{end executor dwd}####################################################################"
eof
