#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集钉钉 人员信息表
#-- 注意 ： 每天全量，
#-- 输入表 : quality_data.dingtalk_user_info
#-- 输出表 ：ods.ods_qkt_dtk_user_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 
#-- 2 wangziming 2021-12-30 modify 增加字段org_name
#-- 3 wangziming 2022-10-10 modify 过滤出快仓智能公司
#-- 4 wangziming 2022-10-11 modify 增加dim层宝仓和快仓智能公司的工号映射表--------
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_dtk_user_info_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=dingtalk_user_info
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
union_id, open_id, remark, user_id, is_boss, hired_date, tel, department, work_place, email, order_code, is_leader, mobile, active, is_admin, avatar, is_hide, job_number, name, extattr, state_code, \`position\`, create_time, update_time,org_name
from $mysql_table  where 1=1  and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"






#!/bin/bash


ods_dbname=ods
dim_dbnam=dim
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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



-- ----创建宝仓和上海快仓智能科技有限公司的人员映射表，同一工号对应不同的user_id

insert overwrite table ${dim_dbnam}.dim_dtk_emp_job_number_mapping_info partition(d='${pre1_date}')
select 
org_name as org_company_name,
job_number,
user_id as emp_id,
name as emp_name
from 
${ods_dbname}.ods_qkt_dtk_user_info_df
where d='${pre1_date}'
;

-- ----过滤出上海快仓智能科技有限公司的ods_qkt_dtk_user_info_df人员表
insert overwrite table ${ods_dbname}.ods_qkt_dtk_user_info_df partition(d='${pre1_date}')
select 
union_id, 
open_id, 
remark, 
user_id, 
is_boss, 
hired_date, 
tel,
department, 
work_place, 
email, 
order_code, 
is_leader, 
mobile, 
active, 
is_admin, 
avatar, 
is_hide, 
job_number, 
name, 
extattr, 
state_code, 
position, 
create_time, 
update_time, 
org_name
from 
${ods_dbname}.ods_qkt_dtk_user_info_df
where d='${pre1_date}' and org_name='上海快仓智能科技有限公司'
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

