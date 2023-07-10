--ads_project_pe_day_month    --项目投入pe人天月统计

INSERT overwrite table ${ads_dbname}.ads_project_pe_day_month
SELECT '' as id, -- 主键
       t1.true_project_code as project_code,
       t1.month_scope as cur_month,
       IF(t2.pe_day is null,0,t2.pe_day) as pe_day,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT p.true_project_code,
         td.month_scope,
         p.pe_day
  FROM
  (
    SELECT DISTINCT date_format(d.days,'yyyy-MM') as month_scope
    FROM ${dim_dbname}.dim_day_date d
  )td
  left join
  (
    SELECT tmp.true_project_code,
           tmp.month_scope,
           tmp.pe_day,
           row_number()over(PARTITION by tmp.true_project_code order by tmp.month_scope)rn
    FROM 
    (
      SELECT tt.project_code,
             tt.month_scope,
             tt.pe_day,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code,tt.month_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT tmp.project_code,
               date_format(tmp.log_date,'yyyy-MM') as month_scope,
               SUM(case when tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末' then 1
                       when tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假' then 0.5
                       when tmp.day_type = '全天请假' then 0 end) as pe_day
        FROM 
        (
          SELECT tud.org_name_2,
                 tud.org_name_3,
                 tud.emp_name, -- 人员名称
                 tud.emp_position,
                 tud.is_job, -- 是否在职
                 tud.days, -- 日期
                 IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- 日期类型
                 IF(t1.log_date is not null or t12.leave_type is not null,'已打卡','未打卡') as ischeck, -- 是否打卡
                 t1.work_status, -- 出勤状态
                 t1.job_content, -- 工作内容
                 t1.log_date, -- 日志日期
                 t1.project_code, -- 项目编码
                 t1.project_name, -- 项目名称
                 t1.project_manage, -- 项目经理
                 t1.business_id, -- 审批编号
                 t1.create_time, -- 创建时间
                 t1.originator_user_name, -- 发起人
                 t1.approval_status, -- 审批状态
                 CASE when t1.log_date is not null and t1.work_status IN ('出差/On business trip','远程支持/Remote support') and i.project_code is null then '编码不存在' end as unusual_res -- 异常原因
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
                SELECT DISTINCT split(tg.org_path_name,'/')[1] as org_name_2,
                                split(tg.org_path_name,'/')[2] as org_name_3,
                                te.emp_id,
                                te.emp_name,
                                te.emp_position,
                                te.prg_path_name,
                                te.is_job,
                                date(te.hired_date) as hired_date,
                                date(te.quit_date) as quit_date,
                                row_number()over(PARTITION by te.emp_id order by split(tg.org_path_name,'/')[1] asc,split(tg.org_path_name,'/')[2] asc)rn
                FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
                LEFT JOIN 
                (
                  SELECT DISTINCT m.emp_id,
                                  m.emp_name,
                                  m.org_id,
                                  m.org_role_type,
                                  m.is_need_fill_manhour,
                                  m.org_start_date,
                                  m.org_end_date,
                                  m.is_job
                  FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
                  WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
                )tmp
                ON te.emp_id = tmp.emp_id
                LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
                ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
                WHERE 1 = 1
                  AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司' AND te.is_active = 1
                  -- 筛选PE全部人员
                  AND ((split(tg.org_path_name,'/')[1] IN ('营销中心','箱式FT') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('项目部') AND split(tg.org_path_name,'/')[2] IN ('箱式交付组','项目交付组') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('欧洲分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('海外交付工程师','PE','实习生','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('北美分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('东南亚及中国台湾区域') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('韩国子公司') AND te.emp_position IN ('项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('日本分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付组') AND te.emp_position IN ('维保技术员','项目工程师')))
              )tmp
              WHERE tmp.rn =1
            )tu  
            LEFT JOIN
            (
              SELECT DISTINCT days,
                              day_type
              FROM ${dim_dbname}.dim_day_date
              WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
            )td
            ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,DATE_ADD(CURRENT_DATE(), -1),tu.quit_date)
          )tud
          LEFT JOIN 
          (
            select *
            FROM ${dwd_dbname}.dwd_dtk_process_pe_log_info_df p
            WHERE d = DATE_ADD(CURRENT_DATE(), -1) AND p.approval_status != 'TERMINATED' AND p.create_time >= '2021-07-01 00:00:00'-- 审批状态剔除终止，起始时间从2021-07-01开始
          )t1
          ON t1.originator_user_id = tud.emp_id AND tud.days = t1.log_date 
          LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df i
          ON i.d = DATE_ADD(CURRENT_DATE(), -1) AND (i.project_code = t1.project_code or i.project_sale_code = t1.project_code)
          LEFT JOIN 
          (
            SELECT tud.days as stat_date,
                   l.originator_user_id,
		           te.emp_id,
		           CASE when (tud.days > l.start_date and tud.days < l.end_date)
                          or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then '全天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
                   end as leave_type
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
              WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
            )l
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
            ON l.originator_user_id = te.emp_id	
            LEFT JOIN ${dim_dbname}.dim_day_date tud
            on l.start_date <= tud.days and l.end_date >= tud.days
            WHERE l.rn = 1
            AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
          ) t12 
          ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
        )tmp
        WHERE tmp.project_code is not NULL 
        GROUP BY tmp.project_code,date_format(tmp.log_date,'yyyy-MM')
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tt.project_code or s.project_sale_code = tt.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )p
  ON td.month_scope >= p.month_scope AND td.month_scope <= date_format(DATE_ADD(CURRENT_DATE(), -1),'yyyy-MM')
  WHERE p.true_project_code is not null AND p.rn = 1
)t1
LEFT JOIN
-- PE人天
(
  SELECT a.true_project_code,
         a.month_scope,
         SUM(a.pe_day) as pe_day
  FROM
  (
    SELECT tmp.*
    FROM 
    (
      SELECT tt.project_code,
             tt.month_scope,
             tt.pe_day,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code,tt.month_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT tmp.project_code,
               date_format(tmp.log_date,'yyyy-MM') as month_scope,
               SUM(case when tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末' then 1
                        when tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假' then 0.5
                        when tmp.day_type = '全天请假' then 0 end) as pe_day
        FROM 
        (
          SELECT tud.org_name_2,
                 tud.org_name_3,
                 tud.emp_name, -- 人员名称
                 tud.emp_position,
                 tud.is_job, -- 是否在职
                 tud.days, -- 日期
                 IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- 日期类型
                 IF(t1.log_date is not null or t12.leave_type is not null,'已打卡','未打卡') as ischeck, -- 是否打卡
                 t1.work_status, -- 出勤状态
                 t1.job_content, -- 工作内容
                 t1.log_date, -- 日志日期
                 t1.project_code, -- 项目编码
                 t1.project_name, -- 项目名称
                 t1.project_manage, -- 项目经理
                 t1.business_id, -- 审批编号
                 t1.create_time, -- 创建时间
                 t1.originator_user_name, -- 发起人
                 t1.approval_status, -- 审批状态
                 CASE when t1.log_date is not null and t1.work_status IN ('出差/On business trip','远程支持/Remote support') and i.project_code is null then '编码不存在' end as unusual_res -- 异常原因
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
                SELECT DISTINCT split(tg.org_path_name,'/')[1] as org_name_2,
                                split(tg.org_path_name,'/')[2] as org_name_3,
                                te.emp_id,
                                te.emp_name,
                                te.emp_position,
                                te.prg_path_name,
                                te.is_job,
                                date(te.hired_date) as hired_date,
                                date(te.quit_date) as quit_date,
                                row_number()over(PARTITION by te.emp_id order by split(tg.org_path_name,'/')[1] asc,split(tg.org_path_name,'/')[2] asc)rn
                FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
                LEFT JOIN 
                (
                  SELECT DISTINCT m.emp_id,
                                  m.emp_name,
                                  m.org_id,
                                  m.org_role_type,
                                  m.is_need_fill_manhour,
                                  m.org_start_date,
                                  m.org_end_date,
                                  m.is_job
                  FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
                  WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
                )tmp
                ON te.emp_id = tmp.emp_id
                LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
                ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
                WHERE 1 = 1
                  AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司' AND te.is_active = 1
                  -- 筛选PE全部人员
                  AND ((split(tg.org_path_name,'/')[1] IN ('营销中心','箱式FT') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('项目部') AND split(tg.org_path_name,'/')[2] IN ('箱式交付组','项目交付组') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('欧洲分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('海外交付工程师','PE','实习生','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('北美分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('东南亚及中国台湾区域') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('韩国子公司') AND te.emp_position IN ('项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('日本分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付组') AND te.emp_position IN ('维保技术员','项目工程师')))
              )tmp
              WHERE tmp.rn =1
            )tu  
            LEFT JOIN
            (
              SELECT DISTINCT days,
                              day_type
              FROM ${dim_dbname}.dim_day_date
              WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
            )td
            ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,DATE_ADD(CURRENT_DATE(), -1),tu.quit_date)
          )tud
          LEFT JOIN 
          (
            select *
            FROM ${dwd_dbname}.dwd_dtk_process_pe_log_info_df p
            WHERE d = DATE_ADD(CURRENT_DATE(), -1) AND p.approval_status != 'TERMINATED' AND p.create_time >= '2021-07-01 00:00:00'-- 审批状态剔除终止，起始时间从2021-07-01开始
          )t1
          ON t1.originator_user_id = tud.emp_id AND tud.days = t1.log_date 
          LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful i 
          ON t1.project_code = i.project_code
          LEFT JOIN 
          (
            SELECT tud.days as stat_date,
                   l.originator_user_id,
		           te.emp_id,
		           CASE when (tud.days > l.start_date and tud.days < l.end_date)
                          or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then '全天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
                   end as leave_type
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
              WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
            )l
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
            ON l.originator_user_id = te.emp_id	
            LEFT JOIN ${dim_dbname}.dim_day_date tud
            on l.start_date <= tud.days and l.end_date >= tud.days
            WHERE l.rn = 1
            AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
          ) t12 
          ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
        )tmp
        WHERE tmp.project_code is not NULL 
        GROUP BY tmp.project_code,date_format(tmp.log_date,'yyyy-MM')
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tt.project_code or s.project_sale_code = tt.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code,a.month_scope
)t2
ON t1.true_project_code = t2.true_project_code and t1.month_scope = t2.month_scope;