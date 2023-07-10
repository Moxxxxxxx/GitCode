#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场PE日志信息记录表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : quality_data.dingtalk_process_pe_log
#-- 输出表 ：ods.ods_qkt_dtk_process_pe_log_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-08 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_dtk_process_pe_log_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=dingtalk_process_pe_log
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
org_name, 
process_instance_id, 
attached_process_instance_ids, 
biz_action, 
business_id, 
cc_userids, 
create_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid, 
\`result\`, 
status, 
title, 
process_project_code, 
process_project_name, 
project_manage, 
work_status, 
start_work_time, 
end_work_time, 
work_go_out, 
job_content, 
log_date, 
site_team_members, 
task_statis, 
company_job_content, 
tomorrow_schedule, 
finish_today, 
over_time, 
enclosure, 
fault_statis, 
fault_num_statis, 
work_7_24, 
carry_task_num, 
remarks
from ${mysql_table}  where date_format(${datax_incre_column},'%Y-%m-%d')=date_add('$pre1_date',interval 1 day) and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"



