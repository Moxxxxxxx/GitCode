#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集钉钉人员花名册信息表
#-- 注意 ： 每天全量，
#-- 输入表 : quality_data.dingtalk_user_roster
#-- 输出表 ：ods.ods_qkt_dtk_user_roster_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-13 CREATE 
#-- 2 wangziming 2022-10-10 modify 过滤出快仓智能公司
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_dtk_user_roster_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=dingtalk_user_roster
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
corp_id, 
org_name, 
user_id, 
report_manager, 
\`position\`, 
name, 
dept, 
employee_type, 
contract_period_type, 
dept_ids, 
contract_renew_count, 
project_role, 
contract_type, 
main_dept_id, 
first_contract_start_time, 
now_contract_start_time, 
cost_center, 
end_time, 
now_contract_end_time, 
regular_time, 
remark, 
email, 
sex_type, 
entry_age, 
probation_period_type, 
first_contract_end_time, 
work_place, 
main_dept, 
contract_company_name, 
confirm_join_time, 
plan_regular_time, 
employee_status, 
create_time, 
update_time
from $mysql_table  where 1=1 and org_name='上海快仓智能科技有限公司' and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"



