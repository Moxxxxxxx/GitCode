#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： 3.x项目机器人类型基础信息表
#-- 注意 ：每日T-1全量
#-- 输入表 : phoenix_basic.basic_robot_type
#-- 输出表 ：ods.ods_qkt_phx_basic_robot_type_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-02 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_phx_basic_robot_type_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=phoenix_basic
mysql_table=basic_robot_type
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




 /opt/module/sqoop-1.4.7/bin/sqoop import  -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
id, 
create_time, 
update_time, 
create_user, 
update_user, 
state, 
warehouse_id, 
robot_type_code, 
robot_type_name, 
first_classification, 
second_classification, 
robot_image, 
robot_camera_distance, 
size_information,
noise, 
speed, 
angular_speed, 
acceleration, 
angular_acceleration, 
specified_load, 
no_load_rated_speed, 
full_load_rated_speed, 
battery_capacity, 
battery_life, 
battery_type, 
charger_port_type, 
charging_time, 
rated_battery_life, 
ditch_capacity, 
crossing_hom_capacity, 
crossing_slope_capacity, 
jacking_height, 
self_weight, 
operating_temperature, 
positioning_accuracy, 
stop_accuracy, 
stop_angle_accuracy, 
walk_face, 
navigation_method, 
drive_mode, 
work_max_height, 
work_min_height, 
zero_position_height, 
safety_distance, 
if(lifting_follow_up=0,'0','1') as lifting_follow_up,
project_code
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database $ods_dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


