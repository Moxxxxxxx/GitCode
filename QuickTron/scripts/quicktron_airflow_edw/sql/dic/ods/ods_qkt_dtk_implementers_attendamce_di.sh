#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ： d     
#-- 功能描述 ： 采集钉钉快仓实施运维人员考勤记录信息
#-- 注意 ：每天增量，每天一个增量分区
#-- 输入表 : quality_data.dingtalk_qkt_implementers_attendance
#-- 输出表 ods.ods_qkt_dtk_implementers_attendamce_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-30 CREATE 
#-- 2 wangziming 2022-02-28 modify 初始化新的数据
#-- 3 wangziming 2022-09-14 modify 初始化新的数据
# ------------------------------------------------------------------------------------------------

start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase  quality_data
\--userName root 
\--passWord quicktron123456 
\--querySql select process_instance_id, attached_process_instance_ids, biz_action, business_id, 
 cc_userids, create_time, finish_time, originator_dept_id, originator_dept_name, 
 originator_userid, result, status, title, project_code, service_type, 
 task_type_implementation, task_type_after_sales, task_type_devops, service_org,
  checkin, checkin_time, checkin_point, checkin_address, checkout, checkout_time,
   checkout_point, checkout_address, remarks, dt_create_time, dt_update_time,'\${pre1_date}' as d
from dingtalk_qkt_implementers_attendance where date_format(datax_update_time,'%Y-%m-%d')=date_add('\$pre1_date',interval 1 day)
\--separator 
\--writerPlugin hivewriter 
\--dataBase ods 
\--table ods_qkt_dtk_implementers_attendamce_di 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d 
\--column process_instance_id,attached_process_instance_ids,biz_action,business_id,cc_userids,create_time:2,finish_time:2,originator_dept_id, originator_dept_name,originator_userid,result, status, title, project_code, service_type,task_type_implementation, task_type_after_sales, task_type_devops, service_org,checkin, checkin_time:2, checkin_point, checkin_address, checkout, checkout_time:2,checkout_point, checkout_address, remarks, dt_create_time:2, dt_update_time:2,d" "ods_qkt_dtk_implementers_attendamce_di"