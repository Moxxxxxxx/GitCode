-- HIVE建表：
create table if not exists ads.ads_superset_user_activity_detail
(
  `user_name`           string comment '用户名称',
  `user_action_name`    string comment '用户操作大类',
  `user_action_detail`  string comment '用户操作小类',
  `action_time`         string comment '日期-年月日时分'
) comment '用户活动详表'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_superset_user_activity_detail;
CREATE TABLE `ads_superset_user_activity_detail` (
  `id`                  bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `user_name`           varchar(50) DEFAULT NULL comment '用户名称',
  `user_action_name`    varchar(50) DEFAULT NULL comment '用户操作大类',
  `user_action_detail`  varchar(50) DEFAULT NULL comment '用户操作小类',
  `action_time`         date DEFAULT NULL comment '日期-年月日时分',
  `create_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='用户活动详表'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dwd.dwd_report_action_log_info_da
#-- 输出表 ：ads.ads_superset_user_activity_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 删除多余括号
#-- 3 查博文 2021-11-15 美化
#-- 5 查博文 2021-11-15 时间字段增加ss
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
--用户活动详表 ads_superset_user_activity_detail （superset用户明细表）

INSERT overwrite table ${ads_dbname}.ads_superset_user_activity_detail
SELECT 
  user_cname          as user_name,
  user_action_name      as user_action_name,
  user_action_detail      as user_action_detail,
  action_time         as action_time
FROM(
    SELECT 
          id,
            detail.user_action_name,
            detail.user_cname,
            detail.action_time
        FROM(
            SELECT 
                  id,
                    user_action,
                    user_action_name,
                    user_id,user_cname, 
                    from_unixtime(unix_timestamp(record_create_time ,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') as action_time,
                    dashboard_id,dashboard_name,
                    slice_id,slice_name,
                    url_address 
            FROM  
                  ${dwd_dbname}.dwd_report_action_log_info_da 
            WHERE 
                  user_id IS NOT NULL
            ) detail
        GROUP BY 
            id,detail.user_action_name, 
            detail.user_cname, 
            detail.action_time
    ) as actions
        LEFT JOIN(
                SELECT 
                    id,
                    CASE WHEN user_action_name='看板操作' THEN dashboard_name 
                    WHEN user_action_name = '图标操作' THEN slice_name 
                    ELSE '无详情'
                    END AS user_action_detail
                FROM 
                    ${dwd_dbname}.dwd_report_action_log_info_da 
                WHERE 
                    user_id IS NOT NULL
                 ) as details
        ON actions.id = details.id
        WHERE user_action_detail IS NOT NULL 
    GROUP BY  user_action_name,user_cname,action_time,user_action_detail
    ORDER BY action_time DESC;
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

truncate table ads_superset_user_activity_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_superset_user_activity_detail （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_user_activity_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_user_activity_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "user_name,user_action_name,user_action_detail,action_time"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "

