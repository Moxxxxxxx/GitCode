#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： 钉钉公出审批流数据记录
#-- 注意 ：每日T-1全量
#-- 输入表 : quality_data.dingtalk_process_attendance_business
#-- 输出表 ：ods.ods_qkt_dtk_process_attendance_business_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-09-06 CREATE 

# ------------------------------------------------------------------------------------------------


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase quality_data 
\--userName root 
\--passWord quicktron123456 
\--querySql select org_name, process_instance_id, cc_userids, attached_process_instance_ids, biz_action, business_id, dd_select_field, create_time, finish_time, textarea_field, dd_attachment, dd_goout_field_start_time, dd_goout_field_end_time, dd_goout_field_duration, originator_dept_id, originator_dept_name, originator_userid, result, status, title, dt_create_time, dt_update_time,'\${pre1_date}' as d from  dingtalk_process_attendance_business
\--separator 
\--writerPlugin hivewriter 
\--dataBase ods 
\--table ods_qkt_dtk_process_attendance_business_df 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--partition d
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--column org_name, process_instance_id, cc_userids, attached_process_instance_ids, biz_action, business_id, dd_select_field, create_time, finish_time, textarea_field, dd_attachment, dd_goout_field_start_time, dd_goout_field_end_time, dd_goout_field_duration, originator_dept_id, originator_dept_name, originator_userid, result, status, title, dt_create_time, dt_update_time, d
" "ods_qkt_dtk_process_attendance_business_df" "${pre1_date}"
