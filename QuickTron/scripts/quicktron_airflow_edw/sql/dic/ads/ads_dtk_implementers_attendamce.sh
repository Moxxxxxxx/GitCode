#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim
tmp_dbname=tmp

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ads_dtk_implementers_attendamce    --快仓实施运维人员考勤记录信息表

INSERT overwrite table ${ads_dbname}.ads_dtk_implementers_attendamce
SELECT '' as id, --主键 
       tt1.cur_date,
       tt1.business_id,
       nvl(pvd.project_code,tt1.project_code) as project_code,
       pvd.project_name,
       nvl(pvd.project_dispaly_state,'未知') as project_operation_state,
       nvl(pvd.project_area,'未知') as project_area,
       nvl(pvd.project_ft,'未知') as project_ft,
       nvl(pvd.project_priority,'未知') as project_priority,
       nvl(pvd.project_progress_stage,'未知') as project_progress_stage,
       tt1.originator_dept_name as team_name,
       tt1.originator_user_name as member_name,
       tt1.service_type,
       '劳务' as member_function, -- 职能【劳务】
       tt1.check_duration,
       tt1.checkin_time,
       tt1.checkout_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
         a.business_id, -- 审批编号
         a.project_code, -- 项目编号
         a.originator_dept_name, -- 团队名称
         IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
         IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
         IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
         a.checkin_time, -- 考勤签到时间
         a.checkout_time, -- 考勤签退时间
         row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
  FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
  WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
)tt1
LEFT JOIN 
(
  SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
         a.business_id, -- 审批编号
         a.project_code, -- 项目编号
         a.originator_dept_name, -- 团队名称
         IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
         IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
         IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
         a.checkin_time, -- 考勤签到时间
         a.checkout_time, -- 考勤签退时间
         row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
  FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
  WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
)tt2
ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON pvd.project_code = tt1.project_code OR pvd.project_sale_code = tt1.project_code
WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"