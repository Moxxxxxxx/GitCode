-- HIVE建表：
create table if not exists ads.ads_superset_sql_lab_activity_detail
(
  `user_name`             string comment '用户名',
  `record_create_time`    string comment '执行开始时间',
  `duration_ms`           bigint comment '执行耗时(ms)'
) comment 'sqllab详表'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_sql_lab_activity_detail;
CREATE TABLE `ads_superset_sql_lab_activity_detail` (
  `id`                    bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `user_name`             varchar(50) DEFAULT NULL comment '用户名',
  `record_create_time`    datetime DEFAULT NULL comment '执行开始时间',
  `duration_ms`           bigint(32) DEFAULT NULL comment '执行耗时(ms)',
  `create_time`           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`           datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='sqllab详表'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dwd.dwd_report_action_log_info_da
#-- 输出表 ：ads.ads_superset_sql_lab_activity_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 user_name应为user_cname
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
--sqllab详表 ads_superset_sql_lab_activity_detail （superset sqllab详表）

INSERT overwrite table ${ads_dbname}.ads_superset_sql_lab_activity_detail
SELECT 
       user_cname                                         as user_name,
       record_create_time                                 as record_create_time,
       duration_ms                                        as duration_ms
FROM(
    SELECT 
          user_cname,
            record_create_time,
            duration_ms 
        FROM 
          ${dwd_dbname}.dwd_report_action_log_info_da 
        WHERE 
          user_action = 'sql_json'
        ORDER BY 
          record_create_time DESC
    ) t;
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

truncate table ads_superset_sql_lab_activity_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：sqllab详表 ads_superset_sql_lab_activity_detail （sqllab详表）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_sql_lab_activity_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_sql_lab_activity_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "user_name,record_create_time,duration_ms"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
