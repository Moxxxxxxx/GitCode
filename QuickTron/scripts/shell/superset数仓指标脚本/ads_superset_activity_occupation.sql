-- HIVE建表：
create table if not exists ads.ads_superset_activity_occupation
(
  `activities_date`     date comment '日期-年月日',
  `dashboards_count`    bigint comment '用户名称',
  `charts_count`        bigint comment '用户操作大类'
) comment '看板和图表使用率'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_activity_occupation;
CREATE TABLE `ads_superset_activity_occupation` (
  `id`                  int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `activities_date`     date NOT NULL COMMENT '年月日',
  `dashboards_count`    bigint(32) DEFAULT NULL comment '看板使用数',
  `charts_count`        bigint(32) DEFAULT NULL comment '图表使用数',
  `create_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='看板和图表使用率'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset看板和图表使用占比用表
#-- 注意 ： 
#-- 输入表 : dws.dws_report_dashboard_daycount、dws.dws_report_slice_daycount
#-- 输出表 ：ads.ads_superset_activity_occupation
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 MODIFY 别名
#-- 5 查博文 2021-11-15 美化
#-- 6 查博文 2021-11-15 增加筛选条件，去除数据开发人人员
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
--看板和图表使用率 ads_superset_activity_occupation （superset看板和图标使用次数）

INSERT overwrite table ${ads_dbname}.ads_superset_activity_occupation
SELECT activities_date                                         as activities_date,
       dashboards_count                                        as dashboards_count,
       charts_count                                            as charts_count
FROM (
  SELECT 
          dashboards.start_date as activities_date,
          nvl(dashboards_count,0) as dashboards_count,
          nvl(charts_count,0) as charts_count
      FROM 
         (
          SELECT 
                start_date,
                sum(operation_count) as dashboards_count
          FROM 
              ${dws_dbname}.dws_report_dashboard_daycount
          WHERE 
              dashboard_id <> 'UNKNOWN'
                AND user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
          GROUP BY 
              start_date 
          ) as dashboards
      FULL JOIN 
          (
          SELECT 
              start_date,sum(operation_count) as charts_count
          FROM 
              ${dws_dbname}.dws_report_slice_daycount
          WHERE 
              user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
          GROUP BY 
              start_date
          ) as charts
      ON  dashboards.start_date = charts.start_date) t;
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

truncate table ads_superset_activity_occupation;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：看板和图表使用率 ads_superset_activity_occupation （superset看板和图标使用次数）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_activity_occupation \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_activity_occupation \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "activities_date,dashboards_count,charts_count"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
