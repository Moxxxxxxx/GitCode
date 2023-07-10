--ads_dtk_process_maintenance_log_info    --维保日志明细表

INSERT overwrite table ${ads_dbname}.ads_dtk_process_maintenance_log_info
SELECT '' as id,
       m.business_id, -- 审批编号
       m.create_time as business_create_time, -- 创建时间
       m.originator_user_name, -- 发起人	
       m.log_date, -- 日志日期
       date_format(m.log_date,'yyyy-MM') as log_month, -- 日志月份
       m.project_code, -- 项目编码
       m.project_name, -- 项目名称
       m.attendance_status, -- 出勤状态
       m.job_content, -- 工作内容
       m.internal_work_content, -- 内部工作内容
       IF(m.working_hours is null,0,m.working_hours) as working_hours, -- 工作时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
WHERE m.org_name = '宝仓' AND m.d = DATE_ADD(CURRENT_DATE(), -1) 
  AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED';