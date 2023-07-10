#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_dtk_version_evaluation_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=version_evaluation
incre_column=upgrade_date
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



###################### version_evaluation->ods_qkt_dtk_version_evaluation_df(T-1每日全量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
id,
process_instance_id,
project_code,
project_name,
product_name,
project_type,
project_stage,
upgrade_res,
version,
upgrade_module,
upgrade_date,
applicant,
approver,
result,
status,
score,
score_remark,
evaluate,
first_examiner_user,
first_examiner,
first_time,
second_examiner_user,
second_examiner,
second_time,
third_examiner_user,
third_examiner,
third_time,
remark,
create_time,
finish_time,
bug_describe,
demand_describe,
project_first,
version_desc,
version_tag
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
insert overwrite table dwd.dwd_dtk_version_evaluation_info_df partition(d='$pre1_date')
select 
  id,
  process_instance_id,
  project_code,
  project_name,
  product_name,
  project_type,
  project_stage,
  upgrade_res,
  case  when version= '2.81' then '2.8.1'
  		when version= '2.2.0' then '其它'
         else version end as upgrade_version,
  upgrade_module,
  upgrade_date,
  applicant,
  approver,
  result as apply_result,
  status as apply_status,
  if(nvl(score,'')<>'',split(score,'-')[0],score) as score,
  regexp_replace(score_remark,'\n|\r|\t','') as score_remark ,
  regexp_replace(evaluate,'\n|\r|\t','') as evaluate_desc,
  first_examiner_user as first_approver_id,
  first_examiner as first_approver_name,
  first_time as first_approval_time,
  second_examiner_user as second_approver_id,
  second_examiner as  second_approver_name,
  second_time as second_approval_time,
  third_examiner_user as third_approver_id,
  third_examiner as third_approver_name,
  third_time as third_approval_time,
  remark as apply_remark,
  create_time as apply_create_time,
  finish_time as apply_finish_time,
 regexp_replace(bug_describe,'\n|\r|\t','') as bug_desc,
 regexp_replace(demand_describe,'\n|\r|\t','') as demand_desc,
  case when project_first='否' then 0
       when project_first='是' then 1
       else '-1' end as is_first_project,
  version_desc,
  version_tag
from 
ods_qkt_dtk_version_evaluation_df
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
eof


