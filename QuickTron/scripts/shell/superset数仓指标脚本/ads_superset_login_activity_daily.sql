-- HIVE建表：
create table if not exists ads.ads_superset_login_activity_daily
(
  `user_id`                       string comment '用户id',
  `user_name`                     string comment '用户名称',
  `date_node`                     date comment '日期-年月日',
  `current_date_login_count`      bigint comment '当天登录次数',
  `total_login_count`             bigint comment '用户总登录次数'
) comment '逐日用户登录记录'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_login_activity_daily;
CREATE TABLE `ads_superset_login_activity_daily` (
  `id`                            bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `user_id`                       varchar(50) DEFAULT NULL comment '用户id',
  `user_name`                     varchar(50) DEFAULT NULL comment '用户名称',
  `date_node`                     date DEFAULT NULL comment '日期-年月日',
  `current_date_login_count`      bigint(32) DEFAULT NULL comment '当天登录次数',
  `total_login_count`             bigint(32) DEFAULT NULL comment '用户总登录次数',
  `create_time`                   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='逐日用户登录记录'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset看板和图表使用占比用表
#-- 注意 ： 
#-- 输入表 : dws.dws_report_user_login_daycount
#-- 输出表 ：ads.ads_superset_activity_occupation
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 美化
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
--逐日用户登录记录 ads_superset_login_activity_daily （superset看板活跃用户）

INSERT overwrite table ${ads_dbname}.ads_superset_login_activity_daily
SELECT 
       user_id                                                 as user_id,
       user_name                                               as user_name,
       date_node                                               as date_node,
       current_date_login_count                                as current_date_login_count,
       total_login_count                                       as total_login_count
FROM( 
  SELECT 
        daily.user_id,
        user_cname as user_name,
            start_date as date_node,
            current_date_login_count,
            total_login_count
     FROM( 
          SELECT 
              user_id,
              user_cname,
                start_date,
                SUM(operation_count) as current_date_login_count
          FROM 
              ${dws_dbname}.dws_report_user_login_daycount
          GROUP BY 
              user_id,
                user_cname,
                start_date
         ) as daily  
     LEFT JOIN 
         (
          SELECT 
              user_id,
                SUM(operation_count) as total_login_count
          FROM 
              ${dws_dbname}.dws_report_user_login_daycount 
          GROUP BY 
              user_id,
                user_cname
         ) as totally 
     ON daily.user_id = totally.user_id
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

truncate table ads_superset_login_activity_daily;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_superset_login_activity_daily （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_login_activity_daily \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_login_activity_daily \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "user_id,user_name,date_node,current_date_login_count,total_login_count"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "
