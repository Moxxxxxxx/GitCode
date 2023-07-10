#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-13 创建
#-- 2 wangyingying 2022-10-19 融合bpm和pms项目数据
#-- 3 wangyingying 2022-10-20 增加项目结项日期字段
#-- 4 wangyingying 2022-10-26 补充项目阶段等字段逻辑
#-- 5 wangyingying 2022-11-09 增加项目区域组（国内|国外）字段
#-- 6 wangyingying 2022-11-18 增加pms项目运营状态字段
#-- 7 wangyingying 2022-11-26 增加小车数量及开箱合格率等字段
#-- 8 wangyingying 2022-11-29 增加项目状态字段
#-- 9 wangyingying 2022-12-27 增加项目集项目编码、是否为主项目
#-- 10 wangyingying 2022-12-28 增加项目类型字段
#-- 11 wangyingying 2022-12-30 增加核心项目状态和运营阶段字段
#-- 12 wangyingying 2023-01-04 增加设备到货签订日期、预计设备到货日期字段
#-- 13 wangyingying 2023-01-11 增加核心项目类型字段
#-- 14 wangyingying 2023-02-10 增加是否活跃字段
# ------------------------------------------------------------------------------------------------


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
--ads_pms_project_general_view_detail    --pms项目概览大表

INSERT overwrite table ${ads_dbname}.ads_pms_project_general_view_detail
SELECT '' as id, -- 主键
       project_code_class, -- 项目编码种类
       project_code, -- 项目编码
       project_sale_code, -- 售前编码
       project_name, -- 项目名称
       project_info, -- 项目编码及名称
       project_product_name, -- 产品线
       project_type, -- 项目类型
       project_dispaly_state, -- 项目阶段
       project_dispaly_state_group, -- 项目阶段组
       project_ft, -- 大区/FT => <技术方案评审>ft
       project_priority, -- 项目评级
       project_current_version, -- 版本号
       sales_area_director, -- owner
       project_area, -- 区域-PM
       pm_name, -- PM
       spm_name, -- spm
       sap_counselor, -- 顾问
       sales_person, -- 销售
       pre_sales_consultant, -- 售前顾问
       pe_members, -- 现场PE
       amount, -- 合同金额（线下表）
       contract_signed_year, -- 合同日期
       contract_signed_date, -- 合同日期
       deliver_goods_achieving_rate, -- 发货完成率
       deliver_goods_desc, -- 发货进度提醒
       pre_project_approval_time, -- 前置申请完成时间
       project_handover_end_time, -- 交接审批完成时间
       expect_online_date, -- 预计上线时间
       online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       online_overdue_days, -- 上线逾期天数 => 计划上线时间 和 实际上线时间 差值
       online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       is_online, -- 是否上线
       sap_entry_date, -- 实施入场时间
       online_times, -- 上线时长
       no_online_times, -- 持续未上线天数
       expect_final_inspection_date, -- 预计终验时间
       final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
       final_inspection_overdue_days, -- 验收逾期天数 => 计划验收时间 和 实际验收时间 差值
       final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
       final_inspection_process_month_begin, -- 终验单审批月初 => <终验报告里程碑>完成时间
       is_final_inspection, -- 是否终验
       final_inspection_times, -- 终验时长
       no_final_inspection_times, -- 持续未验收天数
       post_project_date, -- 项目结项日期
       project_stage, -- 项目阶段
       project_progress_stage, -- 项目进度阶段
       zeroweek_work_num, -- 当前周
       oneweek_work_num, -- 当前周+前一周 
       twoweek_work_num, -- 当前周+前二周 
       threeweek_work_num, -- 当前周+前三周 
       fourweek_work_num, -- 当前周+前四周
       data_source, -- 数据来源
	   project_area_group, -- 项目区域组（国内|国外）
	   pms_project_operation_state, -- pms项目运营状态
	   total_agv_num, -- agv总数量
	   open_package_agv_num, -- 小车开箱数量
	   qualified_rate, -- 小车开箱合格率
	   pms_project_status, -- 项目状态
	   core_project_code, -- 项目集项目编码
	   is_main_project, -- 是否为主项目
	   project_type_name, -- 项目类型
	   pms_core_project_status, -- pms核心项目状态
       pms_core_project_operation_state, -- pms核心项目运营状态
	   equitment_arrival_date, -- 设备到货签订日期
       equitment_arrival_plan_end_date, -- 预计设备到货日期
	   yf_collection_ratio, -- 预付比例
       dh_collection_ratio, -- 到货比例
       ys_collection_ratio, -- 验收比例
       zb_collection_ratio, -- 质保比例
	   pms_core_project_type_name, -- pms核心项目类型
	   is_active, -- 是否活跃
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail

-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"