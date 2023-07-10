-- HIVE建表：
create table if not exists ads.ads_superset_sql_lab_excute_trend
(
  `activity_date`      date comment '日期-年月日',
  `activity_week`      bigint comment '周数',
  `daily_total`        bigint comment '当日执行次数'
) comment 'sqllab使用趋势'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_sql_lab_excute_trend;
CREATE TABLE `ads_superset_sql_lab_excute_trend` (
  `id`                int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键', 
  `activity_date`     date NOT NULL COMMENT '主键-年月日',
  `activity_week`     bigint(32) DEFAULT NULL comment '周数',
  `daily_total`       bigint(32) DEFAULT NULL comment '当日执行次数',
  `create_time`       datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`       datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`activity_date`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='sqllab使用趋势'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dws.dws_report_sql_edit_info_daycount
#-- 输出表 ：ads.ads_superset_sql_lab_excute_trend
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 daily_total字段名修改
#-- 3 查博文 2021-11-15 美化
#-- 1.1 查博文 2021-11-16 更新过滤条件，去除开发人员
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
--sqllab使用趋势 ads_superset_sql_lab_excute_trend （superset sql-lab执行次数）

INSERT overwrite table ${ads_dbname}.ads_superset_sql_lab_excute_trend
SELECT activity_date                                          as activity_date,
       activity_week                                          as activity_week,
       daily_total                                          as daily_total
FROM(
    SELECT 
          start_date as activity_date, 
            start_week as activity_week, 
            SUM(operation_count) as daily_total
        FROM 
          ${dws_dbname}.dws_report_sql_edit_info_daycount 
        WHERE 
            user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
        GROUP BY 
          start_date,start_week
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

truncate table ads_superset_sql_lab_excute_trend;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：sqllab使用趋势 ads_superset_sql_lab_excute_trend （superset sql-lab执行次数）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_sql_lab_excute_trend \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_sql_lab_excute_trend \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "activity_date,activity_week,daily_total"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
