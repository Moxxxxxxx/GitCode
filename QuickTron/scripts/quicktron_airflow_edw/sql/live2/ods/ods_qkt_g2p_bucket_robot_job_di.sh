#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目g2p 标准搬运任务
#-- 注意 ： 每日T-1增量分区
#-- 输入表 : evo_wcs_g2p.bucket_robot_job
#-- 输出表 ：ods.ods_qkt_g2p_bucket_robot_job_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-23 CREATE 
#-- 2 wangziming 2022-03-04 modify 增加字段

# ------------------------------------------------------------------------------------------------
## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_g2p_bucket_robot_job_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=evo_wcs_g2p
mysql_table=bucket_robot_job
incre_column=updated_date
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



###################### bucket_robot_job->ods_qkt_g2p_bucket_robot_job(T-1每日增量量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
state,
source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
sequence,
flag,
created_app,
created_date,
updated_app,
updated_date,
hds_group_id,
bucket_type,
start_area,
project_code,
speed,
task_count_down
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



