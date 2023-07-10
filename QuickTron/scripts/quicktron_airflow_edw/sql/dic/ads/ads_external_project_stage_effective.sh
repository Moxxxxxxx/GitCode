#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--ads_external_project_stage_effective    --外部项目上线验收效率

INSERT overwrite table ${ads_dbname}.ads_external_project_stage_effective
SELECT '' as id,
       td.month_scope,
       IF(tmp1.online_effciency is null,0,tmp1.online_effciency) as online_effciency, -- 上线效率
       IF(tmp2.final_inspection_effciency is null,0,tmp2.final_inspection_effciency) as final_inspection_effciency, -- 验收效率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
    SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1
      AND days >= '2020-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
) td
-- 上线
LEFT JOIN 
(
SELECT b.online_process_month,
       COUNT(DISTINCT b.project_code) as project_num,
       SUM(b.online_times) as online_times,
       CAST(SUM(b.online_times) / COUNT(DISTINCT b.project_code) as decimal(10,2)) as online_effciency
FROM 
(
	SELECT case when b.project_code like 'A%' THEN 'A'
	            when b.project_code like 'C%' THEN 'C'
	            when b.project_code like 'FH-%' THEN 'FH'
	            else '未知' end as project_code_class, -- 项目编码种类
	       b.project_code, -- 项目编码
	       b.project_sale_code, -- 售前编码
	       b.project_name, -- 项目名称
	       b.project_product_name, -- 产品线
	       IF(t2.project_code is not null,'历史项目','新项目') as project_type, -- 项目类型
	       b.project_dispaly_state, -- 项目阶段
	       b.project_dispaly_state_group, -- 项目阶段组
	       b.project_ft, -- 大区/FT => <技术方案评审>ft
	       b.project_priority, -- 项目评级
	       b.project_current_version, -- 版本号
	       t1.sales_area_director, -- owner
	       b.project_area_place as project_area, -- 区域-PM
	       t1.pm_name, -- PM
	       b.sap_counselor, -- 顾问
	       t1.sales_person, -- 销售
	       t1.pre_sales_consultant, -- 售前顾问
	       t6.contract_amount as amount, -- 合同金额（线下表）
	       IF(b.contract_signed_year is null,t2.contract_sign_year,b.contract_signed_year) as contract_signed_year, -- 合同日期
	       IF(b.contract_signed_date is null,t2.contract_sign_date,b.contract_signed_date) as contract_signed_date, -- 合同日期
	       b.pre_project_approval_time, -- 前置申请完成时间
	       b.project_handover_end_time, -- 交接审批完成时间
	       b.expect_online_date, -- 预计上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已上线',IF(t2.project_code is not null,'已上线',b.is_online)) as is_online, -- 是否上线
	       b.sap_entry_date, -- 实施入场时间
	       b.online_times, -- 上线时长
	       IF(t2.project_code is not null,NULL,b.no_online_times) as no_online_times, -- 持续未上线天数
	       b.expect_final_inspection_date, -- 预计终验时间
	       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.final_inspection_process_month) as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已验收',IF(t2.project_code is not null,'已验收',b.is_final_inspection)) as is_final_inspection, -- 是否终验
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,0,b.final_inspection_times) as final_inspection_times, -- 终验时长
	       b.no_final_inspection_times -- 持续未验收天数
	FROM 
	(
	  SELECT tt.true_project_code as project_code,
	         tt.true_project_sale_code as project_sale_code,
	         tt.project_name,
	         tt.project_type_name,
	         tt.project_dispaly_state,
	         tt.project_dispaly_state_group,
	         tt.project_ft,
	         tt.project_priority,
	         tt.project_current_version,
	         tt.sap_counselor,
	         tt.contract_signed_year,
	         tt.contract_signed_date,
	         tt.expect_online_date,
	         tt.online_date,
	         tt.is_online,
	         tt.sap_entry_date,
	         tt.online_times,
	         tt.no_online_times,
	         tt.expect_final_inspection_date, 
	         tt.final_inspection_date,
	         tt.is_final_inspection,
	         tt.final_inspection_times,
	         tt.no_final_inspection_times,
	         tt.project_product_name,
	         tt.project_area_place,
	         tt.online_process_month,
	         tt.final_inspection_process_month,
	         tt.project_handover_end_time,
	         tt.project_handover_start_time,
	         tt.pre_project_approval_time
	  FROM 
	  (
	    SELECT b.project_code as true_project_code, -- 项目编码
	           b.project_sale_code as true_project_sale_code, -- 售前编码
	           b2.project_code,
	           b2.project_sale_code,
	           b.project_name, -- 项目名称
	           case when b.project_type_id = 0 then '外部项目'
	                when b.project_type_id = 1 then '公司外部项目'
	                when b.project_type_id = 4 then '售前项目'
	                when b.project_type_id = 7 then '硬件部项目'
	                when b.project_type_id = 8 then '纯硬件项目'
	                when b.project_type_id = 9 then '自营仓项目'
	                end as project_type_name,
	           IF(b.project_dispaly_state = 'UNKNOWN',NULL,b.project_dispaly_state) as project_dispaly_state, -- 项目阶段
	           case when b.project_dispaly_state = '0.未开始' OR b.project_dispaly_state = '1.立项/启动阶段' OR b.project_dispaly_state = '2.需求确认/分解' OR b.project_dispaly_state = '3.设计开发/测试' then '需求确认/分解阶段'
	                when b.project_dispaly_state = '4.采购/生产' OR b.project_dispaly_state = '5.发货/现场实施' then '发货阶段'
	                when b.project_dispaly_state = '6.上线/初验/用户培训' then '上线实施阶段'
	                when b.project_dispaly_state = '7.终验' then '验收阶段'
	                when b.project_dispaly_state like '8.移交运维/转售后' then '售后移交阶段'
	                when b.project_dispaly_state = '9.项目结项' then '项目结项'
	                when b.project_dispaly_state = '10.项目暂停' then '项目暂停'
	                when b.project_dispaly_state = '11.项目取消' then '项目取消'
	                else NULL end as project_dispaly_state_group, -- 项目阶段组
	           IF(nvl(b.project_attr_ft,'')='',NULL,b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
	           b.project_priority, -- 项目评级
	           IF(b.project_current_version = 'UNKNOWN',NULL,b.project_current_version) as project_current_version, -- 版本号
	           b.sap_counselor, -- 顾问
	           date_format(b.contract_signed_date,'yyyy') as contract_signed_year, -- 合同日期年份
	           b.contract_signed_date, -- 合同日期
	           b.expect_online_date, -- 预计上线时间
	           b.online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
	           IF(b.online_process_approval_time is null,'未上线','已上线') as is_online, -- 是否上线
	           b.sap_entry_date, -- 实施入场时间
	           datediff(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
	           IF(b.sap_entry_date is not null AND b.online_date is null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.sap_entry_date),NULL) as no_online_times, -- 持续未上线天数
	           b.expect_final_inspection_date, -- 预计终验时间
	           b.final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
	           IF(b.final_inspection_process_approval_time is null,'未验收','已验收') as is_final_inspection, -- 是否终验
	           datediff(b.final_inspection_date,b.online_date) as final_inspection_times, -- 终验时长
	           IF(b.final_inspection_date is null AND b.online_date is not null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.online_date),NULL) as no_final_inspection_times, -- 持续未验收天数
	           IF(b.project_product_name = 'UNKNOWN',NULL,b.project_product_name) as project_product_name, -- 产品线
	           IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place) as project_area_place, -- 区域-PM
	           date_format(b.online_process_approval_time,'yyyy-MM') as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
	           date_format(b.final_inspection_process_approval_time,'yyyy-MM') as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
	           h.end_time as project_handover_end_time, -- 交接审批完成时间
	           h.start_time as project_handover_start_time, -- 交接审批开始时间
	           b.pre_project_approval_time, -- 前置申请完成时间
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
	      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%') -- 只保留FH/A/C开头的项目
	      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
	  )tt
	  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
	)b
	-- 项目基本信息
	LEFT JOIN 
	(
	  SELECT p.project_code,
	         p.pm_name,
	         p.sales_area_director,
	         p.sales_person,
	         p.pre_sales_consultant
	  FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
	  WHERE (p.project_code LIKE 'FH-%' OR p.project_code LIKE 'A%' OR p.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
	    AND p.project_type IN ('外部项目','外部产品项目','售前项目','硬件部项目','纯硬件项目','自营仓项目')
	) t1
	ON b.project_code = t1.project_code
	-- 历史项目基本信息
	LEFT JOIN 
	(
	  SELECT f.project_code,
	         f.contract_sign_date,
	         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
	  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
	) t2
	ON b.project_code = t2.project_code
	-- 项目收入
	LEFT JOIN
	(
	  SELECT a.project_code,
	         a.contract_amount
	  FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
	)t6
	ON b.project_code = t6.project_code
	-- 纯硬件项目 设备到货确认里程碑审批流
	LEFT JOIN
	(
	  SELECT tmp.project_code,
	         tmp.equitment_arrival_date,
	         tmp.end_time_month
	  FROM 
	  (
	    SELECT e.project_code,
	           date(e.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
	           date_format(e.end_time,'yyyy-MM') as end_time_month, -- 流程审批完成日期
	           row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
	    FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
	    WHERE e.approve_status = 30 
	  )tmp
	  WHERE tmp.rn = 1
	)t7
	ON b.project_code = t7.project_code
)b
WHERE b.is_online = '已上线' AND b.project_type = '新项目' AND b.sap_entry_date is not null
GROUP BY b.online_process_month
)tmp1
ON tmp1.online_process_month = td.month_scope
-- 验收
LEFT JOIN 
(
SELECT b.final_inspection_process_month,
       COUNT(DISTINCT b.project_code) as project_num,
       SUM(b.final_inspection_times) as final_inspection_times,
       CAST(SUM(b.final_inspection_times) / COUNT(DISTINCT b.project_code) as decimal(10,2)) as final_inspection_effciency
FROM 
(
	SELECT case when b.project_code like 'A%' THEN 'A'
	            when b.project_code like 'C%' THEN 'C'
	            when b.project_code like 'FH-%' THEN 'FH'
	            else '未知' end as project_code_class, -- 项目编码种类
	       b.project_code, -- 项目编码
	       b.project_sale_code, -- 售前编码
	       b.project_name, -- 项目名称
	       b.project_product_name, -- 产品线
	       IF(t2.project_code is not null,'历史项目','新项目') as project_type, -- 项目类型
	       b.project_dispaly_state, -- 项目阶段
	       b.project_dispaly_state_group, -- 项目阶段组
	       b.project_ft, -- 大区/FT => <技术方案评审>ft
	       b.project_priority, -- 项目评级
	       b.project_current_version, -- 版本号
	       t1.sales_area_director, -- owner
	       b.project_area_place as project_area, -- 区域-PM
	       t1.pm_name, -- PM
	       b.sap_counselor, -- 顾问
	       t1.sales_person, -- 销售
	       t1.pre_sales_consultant, -- 售前顾问
	       t6.contract_amount as amount, -- 合同金额（线下表）
	       IF(b.contract_signed_year is null,t2.contract_sign_year,b.contract_signed_year) as contract_signed_year, -- 合同日期
	       IF(b.contract_signed_date is null,t2.contract_sign_date,b.contract_signed_date) as contract_signed_date, -- 合同日期
	       b.pre_project_approval_time, -- 前置申请完成时间
	       b.project_handover_end_time, -- 交接审批完成时间
	       b.expect_online_date, -- 预计上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已上线',IF(t2.project_code is not null,'已上线',b.is_online)) as is_online, -- 是否上线
	       b.sap_entry_date, -- 实施入场时间
	       b.online_times, -- 上线时长
	       IF(t2.project_code is not null,NULL,b.no_online_times) as no_online_times, -- 持续未上线天数
	       b.expect_final_inspection_date, -- 预计终验时间
	       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
	       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.final_inspection_process_month) as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已验收',IF(t2.project_code is not null,'已验收',b.is_final_inspection)) as is_final_inspection, -- 是否终验
	       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,0,b.final_inspection_times) as final_inspection_times, -- 终验时长
	       b.no_final_inspection_times -- 持续未验收天数
	FROM 
	(
	  SELECT tt.true_project_code as project_code,
	         tt.true_project_sale_code as project_sale_code,
	         tt.project_name,
	         tt.project_type_name,
	         tt.project_dispaly_state,
	         tt.project_dispaly_state_group,
	         tt.project_ft,
	         tt.project_priority,
	         tt.project_current_version,
	         tt.sap_counselor,
	         tt.contract_signed_year,
	         tt.contract_signed_date,
	         tt.expect_online_date,
	         tt.online_date,
	         tt.is_online,
	         tt.sap_entry_date,
	         tt.online_times,
	         tt.no_online_times,
	         tt.expect_final_inspection_date, 
	         tt.final_inspection_date,
	         tt.is_final_inspection,
	         tt.final_inspection_times,
	         tt.no_final_inspection_times,
	         tt.project_product_name,
	         tt.project_area_place,
	         tt.online_process_month,
	         tt.final_inspection_process_month,
	         tt.project_handover_end_time,
	         tt.project_handover_start_time,
	         tt.pre_project_approval_time
	  FROM 
	  (
	    SELECT b.project_code as true_project_code, -- 项目编码
	           b.project_sale_code as true_project_sale_code, -- 售前编码
	           b2.project_code,
	           b2.project_sale_code,
	           b.project_name, -- 项目名称
	           case when b.project_type_id = 0 then '外部项目'
	                when b.project_type_id = 1 then '公司外部项目'
	                when b.project_type_id = 4 then '售前项目'
	                when b.project_type_id = 7 then '硬件部项目'
	                when b.project_type_id = 8 then '纯硬件项目'
	                when b.project_type_id = 9 then '自营仓项目'
	                end as project_type_name,
	           IF(b.project_dispaly_state = 'UNKNOWN',NULL,b.project_dispaly_state) as project_dispaly_state, -- 项目阶段
	           case when b.project_dispaly_state = '0.未开始' OR b.project_dispaly_state = '1.立项/启动阶段' OR b.project_dispaly_state = '2.需求确认/分解' OR b.project_dispaly_state = '3.设计开发/测试' then '需求确认/分解阶段'
	                when b.project_dispaly_state = '4.采购/生产' OR b.project_dispaly_state = '5.发货/现场实施' then '发货阶段'
	                when b.project_dispaly_state = '6.上线/初验/用户培训' then '上线实施阶段'
	                when b.project_dispaly_state = '7.终验' then '验收阶段'
	                when b.project_dispaly_state like '8.移交运维/转售后' then '售后移交阶段'
	                when b.project_dispaly_state = '9.项目结项' then '项目结项'
	                when b.project_dispaly_state = '10.项目暂停' then '项目暂停'
	                when b.project_dispaly_state = '11.项目取消' then '项目取消'
	                else NULL end as project_dispaly_state_group, -- 项目阶段组
	           IF(nvl(b.project_attr_ft,'')='',NULL,b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
	           b.project_priority, -- 项目评级
	           IF(b.project_current_version = 'UNKNOWN',NULL,b.project_current_version) as project_current_version, -- 版本号
	           b.sap_counselor, -- 顾问
	           date_format(b.contract_signed_date,'yyyy') as contract_signed_year, -- 合同日期年份
	           b.contract_signed_date, -- 合同日期
	           b.expect_online_date, -- 预计上线时间
	           b.online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
	           IF(b.online_process_approval_time is null,'未上线','已上线') as is_online, -- 是否上线
	           b.sap_entry_date, -- 实施入场时间
	           datediff(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
	           IF(b.sap_entry_date is not null AND b.online_date is null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.sap_entry_date),NULL) as no_online_times, -- 持续未上线天数
	           b.expect_final_inspection_date, -- 预计终验时间
	           b.final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
	           IF(b.final_inspection_process_approval_time is null,'未验收','已验收') as is_final_inspection, -- 是否终验
	           datediff(b.final_inspection_date,b.online_date) as final_inspection_times, -- 终验时长
	           IF(b.final_inspection_date is null AND b.online_date is not null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.online_date),NULL) as no_final_inspection_times, -- 持续未验收天数
	           IF(b.project_product_name = 'UNKNOWN',NULL,b.project_product_name) as project_product_name, -- 产品线
	           IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place) as project_area_place, -- 区域-PM
	           date_format(b.online_process_approval_time,'yyyy-MM') as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
	           date_format(b.final_inspection_process_approval_time,'yyyy-MM') as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
	           h.end_time as project_handover_end_time, -- 交接审批完成时间
	           h.start_time as project_handover_start_time, -- 交接审批开始时间
	           b.pre_project_approval_time, -- 前置申请完成时间
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
	      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%') -- 只保留FH/A/C开头的项目
	      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
	  )tt
	  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
	)b
	-- 项目基本信息
	LEFT JOIN 
	(
	  SELECT p.project_code,
	         p.pm_name,
	         p.sales_area_director,
	         p.sales_person,
	         p.pre_sales_consultant
	  FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
	  WHERE (p.project_code LIKE 'FH-%' OR p.project_code LIKE 'A%' OR p.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
	    AND p.project_type IN ('外部项目','外部产品项目','售前项目','硬件部项目','纯硬件项目','自营仓项目')
	) t1
	ON b.project_code = t1.project_code
	-- 历史项目基本信息
	LEFT JOIN 
	(
	  SELECT f.project_code,
	         f.contract_sign_date,
	         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
	  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
	) t2
	ON b.project_code = t2.project_code
	-- 项目收入
	LEFT JOIN
	(
	  SELECT a.project_code,
	         a.contract_amount
	  FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
	)t6
	ON b.project_code = t6.project_code
	-- 纯硬件项目 设备到货确认里程碑审批流
	LEFT JOIN
	(
	  SELECT tmp.project_code,
	         tmp.equitment_arrival_date,
	         tmp.end_time_month
	  FROM 
	  (
	    SELECT e.project_code,
	           date(e.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
	           date_format(e.end_time,'yyyy-MM') as end_time_month, -- 流程审批完成日期
	           row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
	    FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
	    WHERE e.approve_status = 30 
	  )tmp
	  WHERE tmp.rn = 1
	)t7
	ON b.project_code = t7.project_code
)b
WHERE b.is_final_inspection = '已验收' AND b.project_type = '新项目' AND b.online_process_month is not null
GROUP BY b.final_inspection_process_month
)tmp2
ON tmp2.final_inspection_process_month = td.month_scope;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"