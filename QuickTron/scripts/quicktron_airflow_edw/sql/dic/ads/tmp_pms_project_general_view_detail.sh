#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-20 创建
#-- 2 wangyingying 2022-10-20 增加项目结项日期字段
#-- 3 wangyingying 2022-10-21 补充审批流节点时间
#-- 4 wangyingying 2022-10-25 补充项目阶段等字段逻辑
#-- 5 wangyingying 2022-10-30 修改合同金额取值逻辑
#-- 6 wangyingying 2022-11-01 修改逾期天数逻辑
#-- 7 wangyingying 2022-11-09 增加项目区域组（国内|国外）字段
#-- 8 wangyingying 2022-11-18 增加pms项目运营状态字段
#-- 9 wangyingying 2022-11-26 增加小车数量及开箱合格率等字段
#-- 10 wangyingying 2022-11-28 调整开箱合格率等字段缺省值
#-- 11 wangyingying 2022-11-29 增加项目状态字段
#-- 12 wangyingying 2022-12-14 增加上线验收审批完成日期字段
#-- 13 wangyingying 2022-12-27 增加项目及项目编码、是否主项目
#-- 14 wangyingying 2022-12-28 增加项目类型字段
#-- 15 wangyingying 2022-12-30 增加核心项目状态和运营阶段字段
#-- 16 wangyingying 2023-01-04 增加设备到货签订日期、预计设备到货日期字段
#-- 17 wangyingying 2023-01-08 增加AR回款比例字段
#-- 18 wangyingying 2023-01-11 增加核心项目类型字段
#-- 19 wangyingying 2023-02-10 增加是否活跃字段
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
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--tmp_pms_project_general_view_detail    --pms项目概览大表

INSERT overwrite table ${tmp_dbname}.tmp_pms_project_general_view_detail
SELECT '' as id, -- 主键
       b.project_code_class, -- 项目编码种类
       b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name, -- 项目名称
       b.project_info, -- 项目编码及名称
       b.project_product_name, -- 产品线
       b.project_type, -- 项目类型
       b.project_dispaly_state, -- 项目阶段
       b.project_dispaly_state_group, -- 项目阶段组
       b.project_ft, -- 大区/FT => <技术方案评审>ft
       b.project_priority, -- 项目评级
       b.project_current_version, -- 版本号
       b.sales_area_director, -- owner
       b.project_area, -- 区域-PM
       b.pm_name, -- PM
       b.spm_name, -- spm
       b.sap_counselor, -- 顾问
       b.sales_person, -- 销售
       b.pre_sales_consultant, -- 售前顾问
       b.pe_members, -- 现场PE
       b.amount, -- 合同金额（线下表）
       b.contract_signed_year, -- 合同日期
       TO_DATE(b.contract_signed_date) as contract_signed_date, -- 合同日期
       IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) as deliver_goods_achieving_rate, -- 发货完成率
       CASE WHEN IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) IS NULL THEN NULL
            WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group != '项目结项' AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 历史项目+项目未结项
            WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group = '项目结项' AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 历史项目+项目已结项
            WHEN b.project_type = '新项目' AND b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 0 AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) <= 1 THEN '发货进度正常' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type = '新项目' AND b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 1 AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type = '新项目' AND b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NULL AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) <= 1 THEN '发货进度正常' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type = '新项目' AND b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NOT NULL AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type = '新项目' AND b.project_type_name != '纯硬件项目' AND b.is_online = '已上线' AND b.is_final_inspection = '未验收' AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已上线未验收
            WHEN b.project_type = '新项目' AND b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group != '项目结项' AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已验收+项目未结项
            WHEN b.project_type = '新项目' AND b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group = '项目结项' AND IF(t4.fhsl IS NULL and t3.cgsl IS NULL,NULL,IF(CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)) IS NULL,0,CAST(t4.fhsl / t3.cgsl as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已验收+项目已结项
       ELSE '发货进度异常' END as deliver_goods_desc, -- 发货进度提醒
       TO_DATE(b.pre_project_approval_time) as pre_project_approval_time, -- 前置申请完成时间
       TO_DATE(b.project_handover_end_time) as project_handover_end_time, -- 交接审批完成时间
       TO_DATE(b.expect_online_date) as expect_online_date, -- 预计上线时间
       TO_DATE(b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       b.online_overdue_days, -- 上线逾期天数 => 计划上线时间 和 实际上线时间 差值
	   b.online_process_date, -- 上线单审批日期 => <上线报告里程碑>完成时间
       b.online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       TO_DATE(CONCAT(b.online_process_month,'-01')) as online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       b.is_online, -- 是否上线
       TO_DATE(b.sap_entry_date) as sap_entry_date, -- 实施入场时间
       b.online_times, -- 上线时长
       b.no_online_times, -- 持续未上线天数
       TO_DATE(b.expect_final_inspection_date) as expect_final_inspection_date, -- 预计终验时间
       TO_DATE(b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
       b.final_inspection_overdue_days, -- 验收逾期天数 => 计划验收时间 和 实际验收时间 差值
	   b.final_inspection_process_date, -- 终验单审批日期 => <终验报告里程碑>完成时间
       b.final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
       TO_DATE(CONCAT(b.final_inspection_process_month,'-01')) as final_inspection_process_month_begin, -- 终验单审批月初 => <终验报告里程碑>完成时间
       b.is_final_inspection, -- 是否终验
       b.final_inspection_times, -- 终验时长
       b.no_final_inspection_times, -- 持续未验收天数
       TO_DATE(b.post_project_date) as post_project_date, -- 项目结项日期
       CASE WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group != '项目结项' THEN '已验收未结项' -- 历史项目+项目未结项
            WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 历史项目+项目已结项
            WHEN b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 0 THEN '未发货未上线' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 1 THEN '已结项' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NULL THEN '未发货未上线' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NOT NULL THEN '已发货未上线' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '已上线' AND b.is_final_inspection = '未验收' THEN '已上线未验收' -- 外部项目+已上线未验收
            WHEN b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '已验收未结项' -- 外部项目+已验收+项目未结项
            WHEN b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       END as project_stage, -- 项目阶段
       CASE WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group != '项目结项' THEN '结项阶段' -- 历史项目+项目未结项
            WHEN b.project_type = '历史项目' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 历史项目+项目已结项
            WHEN b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 0 THEN '发货阶段(硬件项目)' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type_name = '纯硬件项目' AND b.is_equitment_arrival = 1 THEN '已结项(硬件项目)' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NULL THEN '发货阶段' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl IS NOT NULL THEN '上线阶段' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type_name != '纯硬件项目' AND b.is_online = '已上线' AND b.is_final_inspection = '未验收' THEN '验收阶段' -- 外部项目+已上线未验收
            WHEN b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '结项阶段' -- 外部项目+已验收+项目未结项
            WHEN b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       END as project_progress_stage, -- 项目进度阶段
       nvl(t5.zeroweek_work_num,0) as zeroweek_work_num, -- 当前周
       nvl(t5.oneweek_work_num,0) as oneweek_work_num, -- 当前周+前一周 
       nvl(t5.twoweek_work_num,0) as twoweek_work_num, -- 当前周+前二周 
       nvl(t5.threeweek_work_num,0) as threeweek_work_num, -- 当前周+前三周 
       nvl(t5.fourweek_work_num,0) as fourweek_work_num, -- 当前周+前四周
       'BPM' as data_source, -- 数据来源
       b.project_area_group, -- 项目区域组（国内|国外）
	   NULL as pms_project_operation_state, -- pms项目运营状态
	   nvl(t6.agv_num,0) as total_agv_num, -- agv总数量
	   t7.fenmu as open_package_agv_num, -- 小车开箱数量
	   t7.qualified_rate, -- 小车开箱合格率
	   NULL as pms_project_status, -- 项目状态
	   nvl(t8.core_project_code,b.project_code) as core_project_code, -- 项目集项目编码
	   nvl(t8.is_main_project,1) as is_main_project, -- 是否为主项目
	   b.project_type_name, -- 项目类型
	   t8.project_dispaly_state as pms_core_project_status, --  pms核心项目状态
       t8.project_operation_state as pms_core_project_operation_state, -- pms核心项目运营状态
       TO_DATE(b.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
       TO_DATE(b.equitment_arrival_plan_end_date) as equitment_arrival_plan_end_date, -- 预计设备到货日期
       t9.yf_collection_ratio, -- 预付比例
       t10.dh_collection_ratio, -- 到货比例
       t11.ys_collection_ratio, -- 验收比例
       t12.zb_collection_ratio, -- 质保比例
	   t8.project_type_name as pms_core_project_type_name, -- pms核心项目类型
	   NULL as is_active, -- 是否活跃
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT CASE WHEN b.project_code LIKE 'A%' THEN 'A'
              WHEN b.project_code LIKE 'C%' THEN 'C'
              WHEN b.project_code LIKE 'FH-%' THEN 'FH'
         ELSE '未知' END as project_code_class, -- 项目编码种类
         b.project_code, -- 项目编码
         b.project_sale_code, -- 售前编码
         b.project_name, -- 项目名称
         CONCAT(b.project_code,'-',b.project_name) as project_info, -- 项目编码及名称
         CASE WHEN b.project_product_name != 'UNKNOWN' THEN b.project_product_name END as project_product_name, -- 产品线
         IF(f.project_code IS NOT NULL,'历史项目','新项目') as project_type, -- 项目类型
         CASE WHEN b.project_dispaly_state != 'UNKNOWN' THEN b.project_dispaly_state END as project_dispaly_state, -- 项目阶段
         CASE WHEN b.project_dispaly_state IN ('0.未开始','1.立项/启动阶段','2.需求确认/分解','3.设计开发/测试') THEN '需求确认/分解阶段'
              WHEN b.project_dispaly_state IN ('4.采购/生产','5.发货/现场实施') THEN '发货阶段'
              WHEN b.project_dispaly_state = '6.上线/初验/用户培训' THEN '上线实施阶段'
              WHEN b.project_dispaly_state = '7.终验' THEN '验收阶段'
              WHEN b.project_dispaly_state = '8.移交运维/转售后' THEN '售后移交阶段'
              WHEN b.project_dispaly_state = '9.项目结项' THEN '项目结项'
              WHEN b.project_dispaly_state = '10.项目暂停' THEN '项目暂停'
              WHEN b.project_dispaly_state = '11.项目取消' THEN '项目取消'
         END as project_dispaly_state_group, -- 项目阶段组
         CASE WHEN b.project_attr_ft != 'UNKNOWN' THEN b.project_attr_ft END as project_ft, -- FT
         b.project_priority, -- 项目评级
         CASE WHEN b.project_current_version != 'UNKNOWN' THEN b.project_current_version END as project_current_version, -- 版本号
         b.sales_area_director, -- 销售区域经理
         IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place IS NULL,'销售',b.project_area_place) as project_area, -- 区域-PM
         b.project_manager as pm_name, -- 项目经理
         nvl(b.spm_name,k.spm_name) as spm_name, -- 销售项目经理
         b.sap_counselor, -- 顾问
         b.sales_manager as sales_person, -- 销售经理
         b.pre_sales_consultant, -- 售前顾问
         a.contract_amount as amount, -- 合同金额（线下表）
         DATE_FORMAT(nvl(b.contract_signed_date,f.contract_sign_date),'yyyy') as contract_signed_year, -- 合同日期年份
         nvl(b.contract_signed_date,f.contract_sign_date) as contract_signed_date, -- 合同日期
         b.pre_project_approval_date as pre_project_approval_time, -- 前置申请完成时间
         b.external_project_handover_approval_date as project_handover_end_time, -- 外部交接审批完成时间
         p.plan_golive_date as expect_online_date, -- 预计上线时间
         IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
         case WHEN p.plan_golive_date IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date) IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date) > p.plan_golive_date THEN IF(DATEDIFF(IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date),p.plan_golive_date) <= 0,0,DATEDIFF(IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date),p.plan_golive_date))
              WHEN p.plan_golive_date IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.online_date) IS NULL THEN DATEDIFF('${pre1_date}',p.plan_golive_date)
         end as online_overdue_days, -- 上线逾期天数 => 计划上线时间 和 实际上线时间 差值
		 IF(b.project_type_name = '纯硬件项目',e.end_time_date,DATE_FORMAT(b.online_process_approval_date,'yyyy-MM-dd')) as online_process_date, -- 上线单审批日期 => <上线报告里程碑>完成时间
         IF(b.project_type_name = '纯硬件项目',e.end_time_month,DATE_FORMAT(b.online_process_approval_date,'yyyy-MM')) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
         IF(b.project_type_name = '纯硬件项目' and e.project_code IS NOT NULL,'已上线',IF(f.project_code IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线'))) as is_online, -- 是否上线
         b.sap_entry_date, -- 实施入场时间
         DATEDIFF(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
         IF(f.project_code IS NOT NULL,NULL,IF(b.sap_entry_date IS NOT NULL AND b.online_date IS NULL,DATEDIFF('${pre1_date}',b.sap_entry_date),NULL)) as no_online_times, -- 持续未上线天数
         p.plan_acceptance_date as expect_final_inspection_date, -- 预计终验时间
         IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
         case WHEN p.plan_acceptance_date IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date) IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date) > p.plan_acceptance_date THEN IF(DATEDIFF(IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date),p.plan_acceptance_date)<= 0,0,DATEDIFF(IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date),p.plan_acceptance_date))
              WHEN p.plan_acceptance_date IS NOT NULL AND IF(b.project_type_name = '纯硬件项目',e.equitment_arrival_date,b.final_inspection_date) IS NULL THEN DATEDIFF('${pre1_date}',p.plan_acceptance_date)
         end as final_inspection_overdue_days, -- 验收逾期天数 => 计划验收时间 和 实际验收时间 差值
		 IF(b.project_type_name = '纯硬件项目',e.end_time_date,DATE_FORMAT(b.final_inspection_process_approval_date,'yyyy-MM-dd')) as final_inspection_process_date, -- 终验单审批日期 => <终验报告里程碑>完成时间
         IF(b.project_type_name = '纯硬件项目',e.end_time_month,DATE_FORMAT(b.final_inspection_process_approval_date,'yyyy-MM')) as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
         IF(b.project_type_name = '纯硬件项目' and e.project_code IS NOT NULL,'已验收',IF(f.project_code IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收'))) as is_final_inspection, -- 是否终验
         IF(b.project_type_name = '纯硬件项目' and e.project_code IS NOT NULL,0,DATEDIFF(b.final_inspection_date,b.online_date)) as final_inspection_times, -- 终验时长
         IF(b.final_inspection_date IS NULL AND b.online_date IS NOT NULL,DATEDIFF('${pre1_date}',b.online_date),NULL) as no_final_inspection_times, -- 持续未验收天数
         b.pe_members, -- 现场PE
         b.project_type_name, -- 项目类型名称
         IF(e.project_code IS NULL,0,1) as is_equitment_arrival, -- 是否设备到货
         b.post_project_date, -- 项目结项日期
         b.project_area_type as project_area_group, -- 项目区域组（国内|国外）
         b.equitment_arrival_date, -- 设备到货签订日期
         b.equitment_arrival_plan_end_date -- 预计设备到货日期
  FROM ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  -- 历史项目基本信息
  LEFT JOIN ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
  ON b.project_code = f.project_code
  -- 补充spm
  LEFT JOIN
  (
    SELECT DISTINCT kf.string31 as project_code,
                    kf.string35 as spm_name
    FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
    LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
    ON kf.flowid = ef.flow_id
    WHERE oflowmodelid= '81687' AND string31 IS NOT NULL AND ef.flow_status = '30'
  )k
  ON b.project_code = k.project_code
  -- 项目收入
  LEFT JOIN
  (
    SELECT a.project_code,
           a.contract_amount
    FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
  )a
  ON b.project_code = a.project_code
  -- 计划时间节点线下表
  LEFT JOIN
  (
    SELECT p.project_code,
           p.plan_golive_date, -- 计划上线日期
           p.plan_acceptance_date -- 计划验收日期
    FROM ${dwd_dbname}.dwd_pmo_project_plan_offline_info_df p
    WHERE p.d = '${pre1_date}'
  )p
  ON b.project_code = p.project_code
  -- 纯硬件项目 设备到货确认里程碑审批流
  LEFT JOIN
  (
    SELECT tmp.project_code,
           tmp.equitment_arrival_date, -- 设备到货签订日期
           tmp.end_time_month, -- 流程审批完成月份
		   tmp.end_time_date -- 流程审批完成日期
    FROM 
    (
      SELECT e.project_code,
             TO_DATE(e.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
             DATE_FORMAT(e.end_time,'yyyy-MM') as end_time_month, -- 流程审批完成月份
			 DATE_FORMAT(e.end_time,'yyyy-MM-dd') as end_time_date, -- 流程审批完成日期
             row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
      WHERE e.approve_status = 30 
    )tmp
    WHERE tmp.rn = 1
  )e
  ON b.project_code = e.project_code
  WHERE b.d = '${pre1_date}'  
    AND b.data_source = 'BPM' -- 只筛选来源是bpm的项目
    AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
    AND b.project_type_id IN (0,1,4,7,8,9) -- 只保留外部项目/公司外部项目/售前项目/硬件部项目/纯硬件项目/自营仓项目
    AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
)b
-- 物料采购数量
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.cgsl) as cgsl
  FROM
  (
    SELECT tmp.project_code,
           SUM(IF(tmp.Number1 IS NULL,0,tmp.Number1)) as cgsl
    FROM
    (
      --采购申请
      SELECT a.project_code, -- 项目编码
             b.string22, -- 物料编码
             b.string23, -- 物料名称
             b.string24, -- 规格型号
             b.string26, -- 单位
             b.Number1 -- 采购数量
      FROM ${dwd_dbname}.dwd_bpm_materials_purchase_request_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 IS NOT NULL
      WHERE a.end_time IS NOT NULL AND a.subscribe_type != '材料采购申请'--流程已结束(审批完成，正常结束)--20220214 采购申请数量去除材料采购申请BY朱文
   
      UNION ALL 
      --采购申请变更
      SELECT a.project_code,
             b.string22,
             NULL,
             NULL,
             NULL,
             b.Number3
      FROM ${dwd_dbname}.dwd_bpm_purchase_request_change_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 IS NOT NULL
      WHERE end_time IS NOT NULL --流程已结束(审批完成，正常结束)
    )tmp
    WHERE tmp.string22 NOT LIKE 'R1S9%' 
      AND tmp.string22 NOT LIKE 'R2S9%'
      AND tmp.string22 NOT LIKE 'R3S9%'
      AND tmp.string22 NOT LIKE 'R4S9%'
      AND (tmp.string22 NOT LIKE 'R5S9%' OR tmp.string22 in('R5S90518','R5S90528')) --R5S90518:第三方外包
      AND (tmp.string22 NOT LIKE 'R6S9%' OR tmp.string22 in('R6S90077','R6S90078','R6S90080','R6S90058')) --国际物流费用
      AND tmp.string22 NOT LIKE 'R7S9%'
      AND tmp.string22 NOT LIKE 'R8S9%'
      AND tmp.string22 NOT LIKE 'R9S9%'
    GROUP BY tmp.project_code
    HAVING SUM(IF(tmp.Number1 IS NULL,0,tmp.Number1)) > 0
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t3
ON b.project_code = t3.project_code
-- 物料已发货数量
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.fhsl) as fhsl
  FROM 
  (
    SELECT a.project_code,
           SUM(IF(b.Number1 IS NULL,0,b.Number1)) fhsl
    FROM ${dwd_dbname}.dwd_bpm_project_delivery_approval_info_ful a
    LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b  
    ON a.flow_id = b.FlowID AND b.string14 IS NOT NULL
    GROUP BY a.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t4
ON b.project_code = t4.project_code
-- 工单统计
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.zeroweek_work_num) as zeroweek_work_num,
         SUM(tmp.oneweek_work_num) as oneweek_work_num,
         SUM(tmp.twoweek_work_num) as twoweek_work_num,
         SUM(tmp.threeweek_work_num) as threeweek_work_num,
         SUM(tmp.fourweek_work_num) as fourweek_work_num
  FROM
  (
    SELECT w.project_code,
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') THEN 1 ELSE 0 END) as zeroweek_work_num, -- 当前周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 1 THEN 1 ELSE 0 END) as oneweek_work_num, -- 当前周+前一周 
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 2 THEN 1 ELSE 0 END) as twoweek_work_num, -- 当前周+前两周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 3 THEN 1 ELSE 0 END) as threeweek_work_num, -- 当前周+前三周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 4 THEN 1 ELSE 0 END) as fourweek_work_num -- 当前周+前四周
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
    WHERE w.d = '${pre1_date}' AND w.project_code IS NOT NULL AND w.work_order_status != '已驳回' AND LOWER(w.project_code) NOT REGEXP 'test|tese'    
    GROUP BY w.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t5
ON b.project_code = t5.project_code
-- AGV数量
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(nvl(tmp.actual_sale_num,0)) AS agv_num -- agv销售数量
  FROM 
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.real_qty,0) - nvl(sr.real_qty,0) AS actual_sale_num -- 实际销售数量 => 出库-退库
    FROM 
    -- 出库
    (
      SELECT so.project_code, -- 项目编码
             SUM(nvl(so.real_qty,0)) AS real_qty -- 出库数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code, -- 项目编码
             SUM(nvl(sr.real_qty,0)) AS real_qty -- 退货数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND sr.project_code IS NOT NULL
      GROUP BY sr.project_code
   )sr
   ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t6
ON b.project_code = t6.project_code
-- 小车开箱情况
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.is_open_package_pass = 1,1,0)) as fenzi,
         COUNT(DISTINCT c.erp_agv_uuid) as fenmu,
         cast(SUM(IF(is_open_package_pass = 1,1,0)) / COUNT(DISTINCT c.erp_agv_uuid) as decimal(10,2)) as qualified_rate
  FROM ${dwd_dbname}.dwd_pms_open_package_check_info_df c
  WHERE c.d = '${pre1_date}'
  GROUP BY c.project_code
)t7
ON b.project_code = t7.project_code
-- 项目集离线数据
LEFT JOIN 
(
  SELECT s.project_code, -- 项目编码
         s.core_project_code, -- 项目集项目编码
         IF(s.project_code = s.core_project_code,1,0) AS is_main_project, -- 是否为主项目
         b.project_dispaly_state, -- 项目阶段状态
         b.project_operation_state, -- 项目运营阶段
		 b.project_type_name -- 项目类型
  FROM ${dim_dbname}.dim_project_set_offline s
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND s.core_project_code = b.project_code
)t8
ON b.project_code = t8.project_code
-- AR回款:预付款
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS yf_collection_ratio -- 预付比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '预付款'
)t9
ON b.project_code = t9.project_code
-- AR回款:到货
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.equitment_arrival_date AS dh_date, -- 到货日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS dh_collection_ratio -- 到货比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '到货'
)t10
ON b.project_code = t10.project_code
-- AR回款:验收
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.final_inspection_date AS ys_date, -- 验收日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS ys_collection_ratio -- 验收比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '终验'
)t11
ON b.project_code = t11.project_code
-- AR回款:质保
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.expiration_date AS zb_date, -- 质保到期日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS zb_collection_ratio -- 质保比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '质保'
)t12
ON b.project_code = t12.project_code

UNION ALL 

SELECT '' as id, -- 主键
       IF(b.project_code LIKE 'FH-%','FH',SUBSTR(b.project_code,0,1)) as project_code_class, -- 项目编码种类
       b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name, -- 项目名称
       CONCAT(b.project_code,'-',b.project_name) as project_info, -- 项目编码及名称
       b.project_product_name, -- 产品线
       '新项目' as project_type, -- 项目类型
       case when b.project_dispaly_state = '已取消' then b.project_dispaly_state
            when b.project_dispaly_state = '已暂停' then b.project_dispaly_state
       else b.project_operation_state end as project_dispaly_state, -- 项目阶段
       b.project_dispaly_state_group, -- 项目阶段组
       b.project_attr_ft as project_ft, -- 大区/FT => <技术方案评审>ft
       b.project_priority, -- 项目评级
       b.project_current_version, -- 版本号
       b.sales_area_director, -- owner
       b.project_area, -- 区域-PM
       b.project_manager as pm_name, -- PM
       b.spm_name, -- spm
       b.sap_counselor, -- 顾问
       b.sales_manager as sales_person, -- 销售
       b.pre_sales_consultant, -- 售前顾问 
       b.pe_members, -- 现场PE
       CAST(b.contract_rmb_amount as DECIMAL(10,2)) as amount, -- 合同金额
       DATE_FORMAT(b.contract_signed_date,'yyyy') as contract_signed_year, -- 合同日期年份
       TO_DATE(b.contract_signed_date) as contract_signed_date, -- 合同日期
       IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) as deliver_goods_achieving_rate, -- 发货完成率
       CASE WHEN IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) IS NULL THEN NULL
            WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NULL AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) <= 1 THEN '发货进度正常' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NOT NULL AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NULL AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) <= 1 THEN '发货进度正常' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NOT NULL AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '已上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已上线未验收
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group != '项目结项' AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已验收+项目未结项
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group = '项目结项' AND IF(b.material_shipments_quantity IS NULL AND b.material_purchase_quantity IS NULL,NULL,IF(CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)) IS NULL,0,CAST(b.material_shipments_quantity / b.material_purchase_quantity as DECIMAL(10,4)))) = 1 THEN '发货进度正常' -- 外部项目+已验收+项目已结项
       ELSE '发货进度异常' END as deliver_goods_desc, -- 发货进度提醒
       TO_DATE(b.pre_project_approval_date) as pre_project_approval_time, -- 前置申请完成时间
       TO_DATE(b.external_project_handover_approval_date) as project_handover_end_time, -- 交接审批完成时间
       TO_DATE(b.inspection_plan_end_date) as expect_online_date, -- 预计上线时间
       TO_DATE(b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       IF(DATEDIFF(nvl(b.online_date,'${pre1_date}'),b.inspection_plan_end_date) <= 0,0,DATEDIFF(nvl(b.online_date,'${pre1_date}'),b.inspection_plan_end_date)) as online_overdue_days, -- 上线逾期天数 => 计划上线时间 和 实际上线时间 差值
	   IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM-dd'),DATE_FORMAT(b.online_process_approval_date,'yyyy-MM-dd')) as online_process_date, -- 上线单审批日期 => <上线报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM'),DATE_FORMAT(b.online_process_approval_date,'yyyy-MM')) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       TO_DATE(CONCAT(IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM'),DATE_FORMAT(b.online_process_approval_date,'yyyy-MM')),'-01')) as online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) as is_online, -- 是否上线
       TO_DATE(b.sap_entry_date) as sap_entry_date, -- 实施入场时间
       DATEDIFF(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
       CASE WHEN b.sap_entry_date IS NOT NULL AND b.online_date IS NULL THEN DATEDIFF('${pre1_date}',b.sap_entry_date) END as no_online_times, -- 持续未上线天数
       TO_DATE(b.final_inspection_plan_end_date) as expect_final_inspection_date, -- 预计终验时间
       TO_DATE(b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
       IF(DATEDIFF(nvl(b.final_inspection_date,'${pre1_date}'),b.final_inspection_plan_end_date) <= 0,0,DATEDIFF(nvl(b.final_inspection_date,'${pre1_date}'),b.final_inspection_plan_end_date)) as final_inspection_overdue_days, -- 验收逾期天数 => 计划验收时间 和 实际验收时间 差值
	   IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM-dd'),DATE_FORMAT(b.final_inspection_process_approval_date,'yyyy-MM-dd')) as final_inspection_process_date, -- 终验单审批日期 => <终验报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM'),DATE_FORMAT(b.final_inspection_process_approval_date,'yyyy-MM')) as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
       TO_DATE(CONCAT(IF(b.project_type_name = '纯硬件项目',DATE_FORMAT(b.equitment_arrival_approval_date,'yyyy-MM'),DATE_FORMAT(b.final_inspection_process_approval_date,'yyyy-MM')),'-01')) as final_inspection_process_month_begin, -- 终验单审批月初 => <终验报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) as is_final_inspection, -- 是否终验
       DATEDIFF(b.final_inspection_date,b.online_date) as final_inspection_times, -- 终验时长
       CASE WHEN b.final_inspection_date IS NULL AND b.online_date IS NOT NULL THEN DATEDIFF('${pre1_date}',b.online_date) END as no_final_inspection_times, -- 持续未验收天数
       TO_DATE(b.post_project_date) as post_project_date, -- 项目结项日期
       CASE WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NULL THEN '未发货未上线' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NOT NULL THEN '已结项' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NULL THEN '未发货未上线' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NOT NULL THEN '已发货未上线' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '已上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' THEN '已上线未验收' -- 外部项目+已上线未验收
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '已验收未结项' -- 外部项目+已验收+项目未结项
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       END as project_stage, -- 项目阶段
       CASE WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NULL THEN '发货阶段(硬件项目)' -- 纯硬件项目+设备到货审批完成
            WHEN b.project_type_name = '纯硬件项目' AND b.equitment_arrival_date IS NOT NULL THEN '已结项(硬件项目)' -- 纯硬件项目+设备到货审批未完成
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NULL THEN '发货阶段' -- 外部项目+未上线未验收+未发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '未上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' AND b.material_shipments_quantity IS NOT NULL THEN '上线阶段' -- 外部项目+未上线未验收+已发货
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已上线',IF(b.online_process_approval_date IS NULL,'未上线','已上线')) = '已上线' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '未验收' THEN '验收阶段' -- 外部项目+已上线未验收
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '结项阶段' -- 外部项目+已验收+项目未结项
            WHEN b.project_type_name != '纯硬件项目' AND IF(b.project_type_name = '纯硬件项目' AND b.equitment_arrival_approval_date IS NOT NULL,'已验收',IF(b.final_inspection_process_approval_date IS NULL,'未验收','已验收')) = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       END as project_progress_stage, -- 项目进度阶段
       NVL(t5.zeroweek_work_num,0) as zeroweek_work_num, -- 当前周
       NVL(t5.oneweek_work_num,0) as oneweek_work_num, -- 当前周+前一周 
       NVL(t5.twoweek_work_num,0) as twoweek_work_num, -- 当前周+前二周 
       NVL(t5.threeweek_work_num,0) as threeweek_work_num, -- 当前周+前三周 
       NVL(t5.fourweek_work_num,0) as fourweek_work_num, -- 当前周+前四周
       'PMS' as data_source, -- 数据来源
       b.project_area_type as project_area_group, -- 项目区域组（国内|国外）
	   b.project_operation_state as pms_project_operation_state, -- pms项目运营状态
	   nvl(t6.agv_num,0) as total_agv_num, -- agv总数量
	   t7.fenmu as open_package_agv_num, -- 小车开箱数量
	   t7.qualified_rate, -- 小车开箱合格率
	   b.project_dispaly_state as pms_project_status, -- 项目状态
	   nvl(t8.core_project_code,b.project_code) as core_project_code, -- 项目集项目编码
	   nvl(t8.is_main_project,1) as is_main_project, -- 是否为主项目
	   b.project_type_name, -- 项目类型
	   nvl(t8.project_dispaly_state,b.project_dispaly_state) as pms_core_project_status, -- 项目状态
       nvl(t8.project_operation_state,b.project_operation_state) as pms_core_project_operation_state, -- pms核心项目运营状态,
       TO_DATE(b.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
       TO_DATE(b.equitment_arrival_plan_end_date) as equitment_arrival_plan_end_date, -- 预计设备到货日期
       t9.yf_collection_ratio, -- 预付比例
       t10.dh_collection_ratio, -- 到货比例
       t11.ys_collection_ratio, -- 验收比例
       t12.zb_collection_ratio, -- 质保比例
	   nvl(t8.project_type_name,b.project_type_name) as pms_core_project_type_name, -- pms核心项目类型
	   CASE WHEN b.project_operation_state IN ('交接','项目启动','蓝图规划','到货签收','现场实施','上线') AND t13.stat_date >= DATE_ADD('${pre1_date}',-6) THEN '活跃'
	        WHEN b.project_operation_state IN ('终验') AND t13.stat_date >= DATE_ADD('${pre1_date}',-13) THEN '活跃'
	        WHEN b.project_operation_state IN ('交接','项目启动','蓝图规划','到货签收','现场实施','上线') AND (t13.stat_date < DATE_ADD('${pre1_date}',-6) OR t13.stat_date IS NULL) THEN '非活跃'
	        WHEN b.project_operation_state IN ('终验') AND (t13.stat_date < DATE_ADD('${pre1_date}',-13) OR t13.stat_date IS NULL) THEN '非活跃'
	   END as is_active, -- 是否活跃
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_pms_share_project_base_info_df b
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.zeroweek_work_num) as zeroweek_work_num,
         SUM(tmp.oneweek_work_num) as oneweek_work_num,
         SUM(tmp.twoweek_work_num) as twoweek_work_num,
         SUM(tmp.threeweek_work_num) as threeweek_work_num,
         SUM(tmp.fourweek_work_num) as fourweek_work_num
  FROM
  (
    SELECT w.project_code,
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') THEN 1 ELSE 0 END) as zeroweek_work_num, -- 当前周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 1 THEN 1 ELSE 0 END) as oneweek_work_num, -- 当前周+前一周 
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 2 THEN 1 ELSE 0 END) as twoweek_work_num, -- 当前周+前两周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 3 THEN 1 ELSE 0 END) as threeweek_work_num, -- 当前周+前三周
           SUM(CASE WHEN WEEKOFYEAR(w.created_time) <= WEEKOFYEAR('${pre1_date}') AND WEEKOFYEAR(w.created_time) >= WEEKOFYEAR('${pre1_date}') - 4 THEN 1 ELSE 0 END) as fourweek_work_num -- 当前周+前四周
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
    WHERE w.d = '${pre1_date}' AND w.project_code IS NOT NULL AND w.work_order_status != '已驳回' AND LOWER(w.project_code) NOT REGEXP 'test|tese'    
    GROUP BY w.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t5
ON b.project_code = t5.project_code
-- AGV数量
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(nvl(tmp.actual_sale_num,0)) AS agv_num -- agv销售数量
  FROM 
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.real_qty,0) - nvl(sr.real_qty,0) AS actual_sale_num -- 实际销售数量 => 出库-退库
    FROM 
    -- 出库
    (
      SELECT so.project_code, -- 项目编码
             SUM(nvl(so.real_qty,0)) AS real_qty -- 出库数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code, -- 项目编码
             SUM(nvl(sr.real_qty,0)) AS real_qty -- 退货数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND sr.project_code IS NOT NULL
      GROUP BY sr.project_code
   )sr
   ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t6
ON b.project_code = t6.project_code
-- 小车开箱情况
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.is_open_package_pass = 1,1,0)) as fenzi,
         COUNT(DISTINCT c.erp_agv_uuid) as fenmu,
         cast(SUM(IF(is_open_package_pass = 1,1,0)) / COUNT(DISTINCT c.erp_agv_uuid) as decimal(10,2)) as qualified_rate
  FROM ${dwd_dbname}.dwd_pms_open_package_check_info_df c
  WHERE c.d = '${pre1_date}'
  GROUP BY c.project_code
)t7
ON b.project_code = t7.project_code
-- 项目集离线数据
LEFT JOIN 
(
  SELECT s.project_code, -- 项目编码
         s.core_project_code, -- 项目集项目编码
         IF(s.project_code = s.core_project_code,1,0) AS is_main_project, -- 是否为主项目
         b.project_dispaly_state, -- 项目状态
         b.project_operation_state, -- 项目运营状态
		 b.project_type_name -- 项目类型
  FROM ${dim_dbname}.dim_project_set_offline s
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND s.core_project_code = b.project_code
)t8
ON b.project_code = t8.project_code
-- AR回款:预付款
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS yf_collection_ratio -- 预付比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '预付款'
)t9
ON b.project_code = t9.project_code
-- AR回款:到货
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.equitment_arrival_date AS dh_date, -- 到货日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS dh_collection_ratio -- 到货比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '到货'
)t10
ON b.project_code = t10.project_code
-- AR回款:验收
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.final_inspection_date AS ys_date, -- 验收日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS ys_collection_ratio -- 验收比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '终验'
)t11
ON b.project_code = t11.project_code
-- AR回款:质保
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.expiration_date AS zb_date, -- 质保到期日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS zb_collection_ratio -- 质保比例
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.ar_stage = '质保'
)t12
ON b.project_code = t12.project_code
-- 项目人力投入情况
LEFT JOIN 
(
  SELECT b.project_code,
         MAX(tmp.stat_date) AS stat_date
  FROM
  (
    -- 研发工时
    SELECT t1.external_project_code AS project_code,
           MAX(TO_DATE(t.task_start_time)) AS stat_date -- 工时登记日期
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t -- ones工作项工时登记信息表
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 -- ones工作项信息表
    ON t.task_uuid = t1.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid IS NOT NULL -- 筛选登记工时类型 且 工时状态有效 且 人员不为空
  	  AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') -- 筛选工作项有效 且 工作项类型为缺陷、任务、需求
      AND t1.external_project_code IS NOT NULL AND TO_DATE(t.task_start_time) >= DATE_ADD('${pre1_date}',-13) AND TO_DATE(t.task_start_time) <= '${pre1_date}'
    GROUP BY t1.external_project_code
  
    UNION ALL 
    -- pe日志
    SELECT p.project_code,
           MAX(TO_DATE(p.log_date)) AS stat_date -- 日报日期
    FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p -- pms人员日志数据信息
    WHERE p.d = '${pre1_date}' AND p.role_type = 'PE' -- 筛选PE数据
      AND p.project_code IS NOT NULL AND TO_DATE(p.log_date) >= DATE_ADD('${pre1_date}',-13) AND TO_DATE(p.log_date) <= '${pre1_date}'
    GROUP BY p.project_code
    
    UNION ALL 
    -- 钉钉劳务
    SELECT p.project_code,
           MAX(DATE(p.checkin_time)) AS stat_date -- 日报日期
    FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di p -- 钉钉考务考勤信息
    WHERE p.approval_status = 'COMPLETED' AND p.approval_result = 'agree' AND p.project_code IS NOT NULL AND p.d >= DATE_ADD('${pre1_date}',-13) AND p.d <= '${pre1_date}'
    GROUP BY p.project_code
    
    UNION ALL 
    -- 海外劳务
    SELECT p.project_code,
           MAX(DATE(p.start_date)) AS stat_date -- 日报日期
    FROM ${dwd_dbname}.dwd_pms_overseas_labour_service_info_df p -- 钉钉考务考勤信息
    WHERE p.d >= DATE_ADD('${pre1_date}',-13) AND p.d <= '${pre1_date}' AND p.project_code IS NOT NULL
    GROUP BY p.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
  ON b.d = '${pre1_date}' AND (b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code) 
  WHERE b.project_code IS NOT NULL 
  GROUP BY b.project_code
)t13
ON b.project_code = t13.project_code
WHERE b.d = '${pre1_date}'
  AND b.data_source = 'PMS' -- 只筛选来源是pms的项目
  AND b.is_external_project IS NULL -- 只筛选外部项目
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql" && hive_concatenate tmp tmp_pms_project_general_view_detail
