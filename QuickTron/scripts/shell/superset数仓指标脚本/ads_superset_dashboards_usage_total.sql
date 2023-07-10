-- HIVE建表：
create table if not exists ads.ads_superset_dashboards_usage_total
(
  `id`                string comment 'id',
  `dashboard_id`      string comment '看板id',
  `dashboard_name`    string comment '看板名称',
  `usage_count`       bigint comment '使用次数'
) comment '各看板总使用数量'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_dashboards_usage_total;
CREATE TABLE `ads_superset_dashboards_usage_total` (
  `id`                bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `dashboard_id`      varchar(50) DEFAULT NULL comment '看板id',
  `dashboard_name`    varchar(50) DEFAULT NULL comment '看板名称',
  `usage_count`       bigint(32) DEFAULT NULL comment '使用次数',
  `create_time`       datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`       datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='各看板总使用数量'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dws.dws_report_dashboard_daycount
#-- 输出表 ：ads.ads_superset_dashboards_usage_total
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 3 查博文 2021-11-15 MODIFY 插入mysql时删除id column
#-- 4 查博文 2021-11-15 MODIFY 插入hive ads层时删除id column
#-- 5 查博文 2021-11-15 美化
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
--各看板总使用数量 ads_superset_dashboards_usage_total （superset看板）

INSERT overwrite table ${ads_dbname}.ads_superset_dashboards_usage_total
SELECT 
    dashboard_id                                            as dashboard_id,
        dashboard_name                                          as dashboard_name,
        usage_count                                             as usage_count
FROM (SELECT 
      dashboard_id,
      dashboard_name,
            SUM(operation_count) as usage_count
      FROM 
          ${dws_dbname}.dws_report_dashboard_daycount 
      WHERE 
          dashboard_id <> 'UNKNOWN'
      GROUP BY 
          dashboard_id,
          dashboard_name) t;
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

truncate table ads_superset_dashboards_usage_total;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板总使用数量 ads_superset_dashboards_usage_total （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_dashboards_usage_total \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_dashboards_usage_total \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "dashboard_id,dashboard_name,usage_count"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
