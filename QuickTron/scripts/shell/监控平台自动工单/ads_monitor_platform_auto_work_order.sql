-- HIVE建表：
CREATE TABLE IF NOT EXISTS ads.ads_monitor_platform_auto_work_order
(
  `ones_work_order_uuid`            string comment 'ones工单uuid',
  `ticket_id`                       string comment '票据id',
  `project_code`                    string comment '项目编码',
  `work_order_status`               string comment '工单状态',
  `work_order_create_time`          string comment '工单创建时间',
  `work_order_status_update_time`   string comment '工单状态变更时间',
  `error_detail`                    string comment '错误详情'
) comment '监控平台钉钉自动工单表'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_monitor_platform_auto_work_order;
CREATE TABLE `ads_monitor_platform_auto_work_order` (
  `id`                              bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `ones_work_order_uuid`            varchar(50) DEFAULT NULL comment 'ones工单uuid',
  `ticket_id`                       varchar(50) DEFAULT NULL comment '票据id',
  `project_code`                    varchar(50) DEFAULT NULL comment '项目编码',
  `work_order_status`               varchar(50) DEFAULT NULL comment '工单状态',
  `work_order_create_time`          datetime DEFAULT NULL comment '工单创建时间',
  `work_order_status_update_time`   datetime DEFAULT NULL comment '工单状态变更时间',
  `error_detail`                    varchar(50) DEFAULT NULL comment '错误详情',
  `create_time`                     datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                     datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='监控平台钉钉自动工单表'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 监控平台钉钉自动工单表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_ones_work_order_info_df
#-- 输出表 ：ads.ads_monitor_platform_auto_work_order
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-18 CREATE
#-- 1 查博文 2021-11-19 增加天分区
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

:<<eof
 if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
 else
    pre2_date=`date -d "-2 day" +%F`
fi
eof

echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--监控平台钉钉自动工单表 ads_monitor_platform_auto_work_order 

INSERT overwrite table ${ads_dbname}.ads_monitor_platform_auto_work_order
SELECT
    ones_work_order_uuid  as  ones_work_order_uuid,
    ticket_id             as  ticket_id,
    project_code          as  project_code,
    work_order_status     as  work_order_status,
    created_time          as  work_order_create_time,
    status_time           as  work_order_status_update_time,
    memo                  as  error_detail
FROM(
    SELECT 
      a.ones_work_order_uuid,
      a.ticket_id,project_code,
      a.work_order_status,
      a.created_time,
      a.status_time,
      regexp_replace(LTRIM(split(b.memo_array,':')[0]),'[{\"]','') as memo
    FROM 
      ${dwd_dbname}.dwd_ones_work_order_info_df a
      lateral view explode(split(a.memo,',')) b as memo_array 
    WHERE 
      a.case_origin_code = '钉钉后台'
      AND a.created_user = '普勇军'
      AND a.project_code NOT IN ('TEST001','TE-tese2','TE-test')
      AND d = '${pre1_date}'
    ) t
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

truncate table ads_monitor_platform_auto_work_order;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_monitor_platform_auto_work_order （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_monitor_platform_auto_work_order \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_monitor_platform_auto_work_order \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "ones_work_order_uuid,ticket_id,project_code,work_order_status,work_order_create_time,work_order_status_update_time,error_detail"

echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "

