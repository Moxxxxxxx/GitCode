--ads_project_general_view_detail_copy    --项目概览大表除金额

INSERT overwrite table ${ads_dbname}.ads_project_general_view_detail_copy
SELECT '' as id, -- 主键
       g.project_code_class, -- 项目编码种类
       g.project_code, -- 项目编码
       g.project_sale_code, -- 售前编码
       g.project_name, -- 项目名称
       g.project_info, -- 项目编码及名称
       g.project_product_name, -- 产品线
       g.project_type, -- 项目类型
       g.project_dispaly_state, -- 项目阶段
       g.project_dispaly_state_group, -- 项目阶段组
       g.project_ft, -- 大区/FT => <技术方案评审>ft
       g.project_priority, -- 项目评级
       g.project_current_version , -- 版本号
       g.sales_area_director, -- owner
       g.project_area, -- 区域-PM
       g.pm_name, -- PM
       g.spm_name, -- spm
       g.sap_counselor, -- 顾问
       g.sales_person, -- 销售
       g.pre_sales_consultant, -- 售前顾问
       g.contract_signed_year, -- 合同日期
       g.contract_signed_date, -- 合同日期
       g.deliver_goods_achieving_rate, -- 发货完成率
       g.pre_project_approval_time, -- 前置申请完成时间
       g.project_handover_end_time, -- 交接审批完成时间
       g.expect_online_date, -- 预计上线时间
       g.online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       g.online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       g.online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       g.is_online, -- 是否上线
       g.sap_entry_date, -- 实施入场时间
       g.online_times, -- 上线时长
       g.no_online_times, -- 持续未上线天数
       g.expect_final_inspection_date, -- 预计终验时间
       g.final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
       g.final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
       g.final_inspection_process_month_begin, -- 终验单审批月初 => <终验报告里程碑>完成时间
       g.is_final_inspection, -- 是否终验
       g.final_inspection_times, -- 终验时长
       g.no_final_inspection_times, -- 持续未验收天数
       g.project_stage, -- 项目阶段
       g.project_progress_stage, -- 项目进度阶段
       g.zeroweek_work_num, -- 当前周
       g.oneweek_work_num, -- 当前周+前一周 
       g.twoweek_work_num, -- 当前周+前二周 
       g.threeweek_work_num, -- 当前周+前三周 
       g.fourweek_work_num, -- 当前周+前四周 
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${ads_dbname}.ads_project_general_view_detail g