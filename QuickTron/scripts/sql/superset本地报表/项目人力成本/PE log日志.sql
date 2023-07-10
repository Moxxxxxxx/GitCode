--ads_dtk_process_pe_log_detail    --PE log日志

with project_view_detail as 
(
SELECT b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name, -- 项目名称
       b.project_type_name, -- 项目类型名称
       IF(t2.project_code is not null,'历史项目','新项目') as project_type, -- 项目类型
       IF(b.project_ft is null,'未知',b.project_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
       b.project_priority, -- 项目评级
       IF(b.project_area_place is null,'未知',b.project_area_place) as project_area, -- 区域-PM
       b.pre_project_approval_time, -- 前置申请完成时间
       b.project_handover_end_time, -- 交接审批完成时间
       t7.end_time, -- 设备到货审批完成时间
       b.online_process_approval_time, -- 上线审批单完成时间
       b.final_inspection_process_approval_time, -- 验收审批单完成时间
       b.post_project_date -- 项目结项时间
FROM 
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name,
         tt.project_type_name,
         tt.project_ft,
         tt.project_priority,
         tt.project_area_place,
         tt.online_process_approval_time,
         tt.final_inspection_process_approval_time,
         tt.project_handover_end_time,
         tt.pre_project_approval_time,
         tt.post_project_date
  FROM 
  (
    SELECT b.project_code as true_project_code, -- 项目编码
           b.project_sale_code as true_project_sale_code, -- 售前编码
           b.project_name, -- 项目名称
           b.project_type_name, -- 项目类型名称
           IF(nvl(b.project_attr_ft,'')='',NULL,b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
           b.project_priority, -- 项目评级
           IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place) as project_area_place, -- 区域-PM
           b.online_process_approval_time,
           b.final_inspection_process_approval_time,
           h.end_time as project_handover_end_time, -- 交接审批完成时间
           b.pre_project_approval_time, -- 前置申请完成时间
           b.post_project_date, -- 项目结项时间
           row_number()over(PARTITION by b2.project_sale_code order by b2.project_code,h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_share_project_base_info_df b
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b2
    ON (b.project_code = b2.project_code or b.project_sale_code = b2.project_sale_code) AND b.d =b2.d 
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE b.d = DATE_ADD(CURRENT_DATE(), -1)   
      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
      AND b.project_type_id IN (0,1,4,7,8,9) -- 只保留外部项目/公司外部项目/售前项目/硬件部项目/纯硬件项目/自营仓项目
      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
  )tt
  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
)b
-- 历史项目基本信息
LEFT JOIN 
(
  SELECT f.project_code,
         f.contract_sign_date,
         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
) t2
ON b.project_code = t2.project_code
-- 物料已发货数量
LEFT JOIN
(
  SELECT tt.true_project_code,
         SUM(tt.fhsl) as fhsl
  FROM 
  (
    SELECT a.project_code as string21,
           s.project_code as true_project_code,
           s.project_sale_code,
           h.start_time,
           sum(if(b.Number1 is null,0,b.Number1)) fhsl,
           row_number()over(PARTITION by a.project_code order by h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_project_delivery_approval_info_ful a
    LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b  
    ON a.flow_id = b.FlowID AND b.string14 is not null
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = a.project_code or s.project_sale_code = a.project_code)
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON s.project_code = h.project_code AND h.rn = 1
    WHERE a.approve_status != '50' --进行自动终止
    GROUP BY a.project_code,h.start_time,s.project_code,s.project_sale_code
    )tt
  WHERE tt.rn = 1
  GROUP BY tt.true_project_code
)t4
ON b.project_code = t4.true_project_code
-- 纯硬件项目 设备到货确认里程碑审批流
LEFT JOIN
(
  SELECT tmp.project_code,
         tmp.equitment_arrival_date,
         tmp.end_time_month,
         tmp.end_time
  FROM 
  (
    SELECT e.project_code,
           date(e.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
           date_format(e.end_time,'yyyy-MM') as end_time_month, -- 流程审批完成日期
           e.end_time,
           row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
    WHERE e.approve_status = 30 
  )tmp
  WHERE tmp.rn = 1
)t7
ON b.project_code = t7.project_code
)

INSERT overwrite table ${ads_dbname}.ads_dtk_process_pe_log_detail
SELECT '' as id,
       tud.org_name_2,
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
       IF(t1.working_hours is null AND t1.log_date is not null,0,t1.working_hours) as working_hours, -- 工作时长
       t1.project_code, -- 项目编码
       t1.project_name, -- 项目名称
       t1.project_manage, -- 项目经理
       pvd.project_area, -- 区域-PM
       pvd.project_ft, -- 大区/FT => <技术方案评审>ft
       pvd.project_priority,
       case when pvd.project_type = '历史项目' AND t1.log_date <= DATE_ADD(CURRENT_DATE(), -1) then '已结项' 
            when pvd.project_type = '新项目' AND pvd.project_type_name = '纯硬件项目' AND t1.log_date <= IF(pvd.end_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.end_time) THEN '发货阶段(硬件项目)'
            when pvd.project_type = '新项目' AND pvd.project_type_name = '纯硬件项目' AND t1.log_date > IF(pvd.end_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.end_time) AND t1.log_date <= IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) THEN '已结项(硬件项目)'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND t1.log_date <= IF(pvd.online_process_approval_time is null,IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time),pvd.online_process_approval_time) then '上线阶段' 
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND t1.log_date > IF(pvd.online_process_approval_time is null,IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time),pvd.online_process_approval_time) AND t1.log_date <= IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time) then '验收阶段'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND t1.log_date > IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time) AND t1.log_date <= IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) then '结项阶段'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND t1.log_date > IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) AND t1.log_date <= DATE_ADD(CURRENT_DATE(), -1) then '已结项' 
       else null end as project_progress_stage,
       t1.business_id, -- 审批编号
       t1.create_time, -- 创建时间
       t1.originator_user_name, -- 发起人
       t1.approval_status, -- 审批状态
       CASE when t1.log_date is not null and t1.work_status IN ('出差/On business trip','远程支持/Remote support') and i.project_code is null then '编码不存在' end as unusual_res, -- 异常原因
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
  SELECT *
  FROM ${dwd_dbname}.dwd_dtk_process_pe_log_info_df p
  WHERE p.d = DATE_ADD(CURRENT_DATE(), -1) AND p.approval_status != 'TERMINATED' AND p.create_time >= '2021-07-01 00:00:00'-- 审批状态剔除终止，起始时间从2021-07-01开始
)t1
ON t1.originator_user_id = tud.emp_id AND tud.days = t1.log_date 
LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df i 
ON t1.project_code = i.project_code AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND (i.project_code LIKE 'FH-%' OR i.project_code LIKE 'A%' OR i.project_code LIKE 'C%') AND i.project_type_id IN (0,1,4,7,8,9) AND (i.is_business_project = 0 OR (i.is_business_project = 1 AND i.is_pre_project = 1))
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
LEFT JOIN project_view_detail pvd
ON pvd.project_code = t1.project_code OR pvd.project_sale_code = t1.project_code;