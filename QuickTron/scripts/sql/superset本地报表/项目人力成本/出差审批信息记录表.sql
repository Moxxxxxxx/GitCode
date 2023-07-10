--ads_dtk_process_business_travel    --出差审批信息记录表

INSERT overwrite table ${ads_dbname}.ads_dtk_process_business_travel
SELECT NULL as id, --主键 
       tt.cur_date,
       tt.business_create_time,
       tt.business_id,
       tt.project_code,
       tt.project_name,
       tt.project_ft,
       tt.project_operation_state,
       tt.team_name,
       tt.member_name,
       tt.member_function,
       tt.trip_duration,
       tt.start_time,
       tt.end_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date, -- 统计时间
       t.create_time as business_create_time, -- 审批业务创建时间
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       b.project_name, -- 项目名称
       IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
       b.project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time, -- 出差结束时间
       row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
ON t.originator_user_id = i.emp_id
LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
ON t.project_code = b.project_code
WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.is_project_matching = '1' AND t.d = DATE_ADD(CURRENT_DATE(), -1) -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
  AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
  AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
  AND b.d = DATE_ADD(CURRENT_DATE(), -1)
)tt
WHERE tt.rn = 1;