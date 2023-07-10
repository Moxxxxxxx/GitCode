-- HIVE建表：
create table if not exists ads.ads_simulation_utility_detail
(
  `simulation_id`           string comment '仿真场景id',
  `simulation_type`         string comment '仿真场景大类',
  `simulation_sub_type`     string comment '仿真场景小类',
  `server_ip`               string comment '仿真环境ip',
  `user_ip`                 string comment '用户ip',
  `action_detail`           string comment '操作详情',
  `start_time`              string comment '开始时间',
  `end_time`                string comment '结束时间',
  `duration_second`         bigint comment '操作用时(秒)'
) comment '仿真环境使用分析详表'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_simulation_utility_detail;
CREATE TABLE `ads_simulation_utility_detail` (
  `id`                      int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `simulation_id`           int(11) DEFAULT NULL comment '仿真场景id',
  `simulation_type`         varchar(50) DEFAULT NULL comment '仿真场景大类',
  `simulation_sub_type`     varchar(50) DEFAULT NULL comment '仿真场景小类',
  `server_ip`               varchar(50) DEFAULT NULL comment '仿真环境ip',
  `user_ip`                 varchar(50) DEFAULT NULL comment '用户ip',
  `action_detail`           varchar(50) DEFAULT NULL comment '操作详情',
  `start_time`              datetime DEFAULT NULL comment '开始时间',
  `end_time`                datetime DEFAULT NULL comment '结束时间',
  `duration_second`         bigint(32) DEFAULT NULL comment '操作用时(秒)',
  `create_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='仿真环境使用分析详表'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 仿真环境使用分析详表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_simulation_record_info_ful
#-- 输出表 ：ads.ads_simulation_utility_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-7 CREATE
# ------------------------------------------------------------------------------------------------

hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--仿真环境使用分析详表 ads_simulation_utility_detail （仿真环境使用分析，月活，周频）

INSERT overwrite table ${ads_dbname}.ads_simulation_utility_detail
SELECT 
  simulation_id                   as simulation_id,
  simulation_type                 as simulation_type,
  simulation_sub_type             as simulation_sub_type,
  server_ip                       as server_ip,
  user_ip                         as user_ip,
  action_detail                   as action_detail,
  data_time                       as start_time,
  end_time                        as end_time,
  scene_duration_second           as duration_second
FROM 
  (
  SELECT
      uuid,
      server_ip,
      user_ip,
      simulation_id,
      simulation_type,
      simulation_sub_type,
      scene_actions_concat,
      CASE 
      WHEN scene_actions_concat = 'map-create->simulation-startAndBuild' then '仿真场景参数编辑'
      WHEN scene_actions_concat IN ('simulation-startAndBuild->simulation-end','simulation-startAndBuild->simulation-finish') then '仿真场景运行'
      ELSE 'abnormal_data' END as action_detail,
      data_time,
      from_unixtime(unix_timestamp( data_time, 'yyyy-MM-dd HH:mm:ss' )+scene_duration_second,'yyyy-MM-dd HH:mm:ss') AS end_time,
      scene_duration_second 
    FROM
      ${dwd_dbname}.dwd_simulation_record_info_ful 
    WHERE
      scene_actions_concat <> 'OTHER') t
WHERE t.action_detail <> 'abnormal_data';
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_simulation_utility_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：仿真环境使用分析详表 ads_simulation_utility_detail （仿真环境使用分析，月活，周频）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_simulation_utility_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_simulation_utility_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "activities_date,dashboards_count,charts_count"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
