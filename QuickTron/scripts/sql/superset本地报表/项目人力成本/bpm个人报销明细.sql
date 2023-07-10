--bpm个人报销明细 ads_bpm_personal_expense_account_info_ful

INSERT overwrite table ${ads_dbname}.ads_bpm_personal_expense_account_info_ful
SELECT '' as id, -- 主键
       p.flow_id, -- 流程号
       p.flow_name, -- 流程名称
       p.start_time as flow_start_time, -- 开始时间
       p.end_time as flow_end_time, -- 结束时间
       p.reimburse_categories, -- 报销类型
       p.apply_user_name, -- 申请人
       p.reimburse_user_name, -- 报销人
       p.cost_tenant, -- 费用承担人
       p.reimburse_date, -- 报销日期
       p.total_reimburse_amount, -- 总金额
       i.project_code, -- 项目编码
       i.project_name, -- 项目名称
       i.start_date, -- 费用开始日期
       i.end_date, -- 费用结束日期
       i.place, -- 地点
       i.cost_categories, -- 费用类别
       i.total_days, -- 总计天数
       i.summary, -- 摘要（是由）
       i.total_amount as amount, -- 金额
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful p
LEFT JOIN ${dwd_dbname}.dwd_bpm_personal_expense_account_item_info_ful i
ON p.flow_id = i.flow_id
WHERE p.approve_status = 30;