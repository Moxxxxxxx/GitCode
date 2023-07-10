--ads_dtk_maintenance_working_hours    --维保有效工时统计

INSERT overwrite table ${ads_dbname}.ads_dtk_maintenance_working_hours
SELECT '' as id,
       tud.emp_name,
       tud.days,
       tud.day_type,
       date_format(tud.days,'yyyy-MM') as months,
       IF(t1.working_hours is null,0,t1.working_hours) as working_hours,
       IF(t12.leave_days is null,0,t12.leave_days) as leave_days,
       IF(t12.leave_type is null,'无',t12.leave_type) as leave_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.org_name_2,
         tu.org_name_3,
         tu.emp_id,
         tu.emp_name,
         tu.emp_position,
         tu.is_job,
         tu.hired_date,
         tu.quit_date,
         td.days,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type   
  FROM
  (
    SELECT tmp.org_name_2,
           tmp.org_name_3,
           tmp.emp_id,
           tmp.emp_name,
           tmp.emp_position,
           tmp.is_job,
           tmp.hired_date,
           tmp.quit_date
    FROM
    (
      SELECT DISTINCT l.org_name_2,
                      l.org_name_3,
                      te.emp_id,
                      te.emp_name,
                      te.emp_position,
                      te.prg_path_name,
                      te.is_job,
                      date(te.hired_date) as hired_date,
                      date(te.quit_date) as quit_date,
                      row_number()over(PARTITION by te.emp_id order by l.org_name_2 asc,l.org_name_3 asc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
      LEFT JOIN ${dim_dbname}.dim_dtk_emp_org_mapping_info m 
      ON m.emp_id = te.emp_id AND m.org_company_name = '上海快仓智能科技有限公司'
      LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info l 
      ON l.org_id = m.org_id 
      WHERE te.d = DATE_ADD(CURRENT_DATE(), -1) 
        AND te.is_active = 1 
        AND te.org_company_name = '上海快仓智能科技有限公司' 
        AND te.org_cnames IN ('售后维保组','售后运维组') -- 筛选维保人员
        )tmp
    WHERE tmp.rn =1
  )tu  
  LEFT JOIN
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2022-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  )td
  ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,DATE_ADD(CURRENT_DATE(), -1),tu.quit_date)
)tud
LEFT JOIN 
( 
  SELECT m.originator_user_id, -- 发起人id
         m.originator_user_name, -- 发起人	
         m.log_date, -- 日志日期
         date_format(m.log_date,'yyyy-MM') as log_month, -- 日志月份
         SUM(IF(m.working_hours is null,0,m.working_hours)) as working_hours -- 工作时长
  FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
  WHERE m.org_name = '宝仓' AND m.d = DATE_ADD(CURRENT_DATE(), -1) 
    AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
  GROUP BY m.originator_user_id,m.originator_user_name,m.log_date,date_format(m.log_date,'yyyy-MM')
)t1
ON tud.emp_id = t1.originator_user_id AND tud.days = t1.log_date
LEFT JOIN 
(
  SELECT tud.days as stat_date,
         l.originator_user_id,
		 te.emp_id,
		 CASE when (tud.days > l.start_date and tud.days < l.end_date)
                or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then 8
              when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then 4
              when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then 4
         end as leave_days,
         l.leave_type
  FROM 
  (
    SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
           l.originator_user_id,
           IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
           IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
           IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
           IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
           IF(l.leave_type != '哺乳假','正常请假','哺乳假') as leave_type,
           row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.start_date asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
    LEFT JOIN ${dwd_dbname}.dwd_dtk_process_leave_info_df l1
    ON l.originator_user_id = l1.originator_user_id AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.process_result = l1.process_result AND l.process_status = l1.process_status AND l.is_valid =l1.is_valid
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) AND l.org_name = '宝仓'
  )l
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON l.originator_user_id = te.emp_id	
  LEFT JOIN ${dim_dbname}.dim_day_date tud
  on l.start_date <= tud.days and l.end_date >= tud.days
  WHERE l.rn = 1
    AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
) t12 
ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days;