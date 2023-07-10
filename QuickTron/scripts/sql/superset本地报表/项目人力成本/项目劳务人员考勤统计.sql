--ads_project_service_check    --项目劳务人员考勤统计

INSERT overwrite table ${ads_dbname}.ads_project_service_check
SELECT NULL as id, --主键 
       tt1.cur_date,
       tt1.project_code,
       tt1.project_name,
       tt1.project_ft,
       tt1.project_operation_state,
       tt1.originator_dept_name as team_name,
       tt1.originator_user_name as member_name,
       tt1.service_type,
       SUM(tt1.check_duration) as check_duration_hour,
       case when SUM(tt1.check_duration) < 4 then 0
            when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
            when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
            when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM  
 (
 SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
       a.business_id, -- 审批编号
       a.project_code, -- 项目编号
       b.project_name, -- 项目名称
       IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
       b.project_operation_state, -- 项目运营阶段
       a.originator_dept_name, -- 团队名称
       IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
       IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
       IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
       a.checkin_time, -- 考勤签到时间
       a.checkout_time, -- 考勤签退时间
       row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
ON a.project_code = b.project_code
WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
  AND b.d = DATE_ADD(CURRENT_DATE(), -1)
)tt1
LEFT JOIN 
 (
 SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
       a.business_id, -- 审批编号
       a.project_code, -- 项目编号
       b.project_name, -- 项目名称
       IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
       b.project_operation_state, -- 项目运营阶段
       a.originator_dept_name, -- 团队名称
       IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
       IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
       IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
       a.checkin_time, -- 考勤签到时间
       a.checkout_time, -- 考勤签退时间
       row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
ON a.project_code = b.project_code
WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
  AND b.d = DATE_ADD(CURRENT_DATE(), -1)
)tt2
ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
GROUP BY tt1.cur_date,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type;