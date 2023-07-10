--ads_dtk_implementers_attendamce    --快仓实施运维人员考勤记录信息表

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
           IF(length(b.project_attr_ft) = 0,NULL,b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
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

INSERT overwrite table ${ads_dbname}.ads_dtk_implementers_attendamce
SELECT NULL as id, --主键 
       tt1.cur_date,
       tt1.business_id,
       tt1.project_code,
       tt1.project_name,
       tt1.project_operation_state,
       pvd.project_area,
       pvd.project_ft,
       pvd.project_priority,
       case when pvd.project_type = '历史项目' AND tt1.cur_date <= DATE_ADD(CURRENT_DATE(), -1) then '已结项' 
            when pvd.project_type = '新项目' AND pvd.project_type_name = '纯硬件项目' AND tt1.cur_date <= IF(pvd.end_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.end_time) THEN '发货阶段(硬件项目)'
            when pvd.project_type = '新项目' AND pvd.project_type_name = '纯硬件项目' AND tt1.cur_date > IF(pvd.end_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.end_time) AND tt1.cur_date <= IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) THEN '已结项(硬件项目)'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND tt1.cur_date <= IF(pvd.online_process_approval_time is null,IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time),pvd.online_process_approval_time) then '上线阶段' 
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND tt1.cur_date > IF(pvd.online_process_approval_time is null,IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time),pvd.online_process_approval_time) AND tt1.cur_date <= IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time) then '验收阶段'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND tt1.cur_date > IF(pvd.final_inspection_process_approval_time is null,IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date),pvd.final_inspection_process_approval_time) AND tt1.cur_date <= IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) then '结项阶段'
            when pvd.project_type = '新项目' AND pvd.project_type_name != '纯硬件项目' AND tt1.cur_date > IF(pvd.post_project_date is null,DATE_ADD(CURRENT_DATE(), -1),pvd.post_project_date) AND tt1.cur_date <= DATE_ADD(CURRENT_DATE(), -1) then '已结项' 
       else null end as project_progress_stage,
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
       b.project_name, -- 项目名称
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
LEFT JOIN project_view_detail pvd
ON pvd.project_code = tt1.project_code OR pvd.project_sale_code = tt1.project_code
WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time;