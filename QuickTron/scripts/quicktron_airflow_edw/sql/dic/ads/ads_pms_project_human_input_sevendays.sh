#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-02-13 创建
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
-- ads_pms_project_human_input_sevendays    --pms项目近七天人力投入

INSERT overwrite table ${ads_dbname}.ads_pms_project_human_input_sevendays
SELECT '' as id, -- 主键
       t1.project_code, -- 项目编码
       t1.project_sale_code, -- 售前编码
       t1.project_name, -- 项目名称
       t1.project_ft, -- 大区/FT => <技术方案评审>ft
       t1.project_priority, -- 项目评级
       t1.project_area, -- 区域-PM
	   t1.data_source, -- 数据来源
	   t1.project_area_group, -- 项目区域组（国内|国外）
	   t1.pms_project_operation_state, -- pms项目运营状态
	   t1.pms_project_status, -- 项目状态
	   t1.core_project_code, -- 项目集项目编码
	   t1.is_main_project, -- 是否为主项目
	   t1.project_type_name, -- 项目类型
	   t1.pms_core_project_status, -- pms核心项目状态
       t1.pms_core_project_operation_state, -- pms核心项目运营状态
	   t1.pms_core_project_type_name, -- pms核心项目类型
	   t1.is_active, -- 是否活跃
	   nvl(sum(case when t2.stat_date = '${pre1_date}' THEN nvl(t2.person_num,0) end),0) as one_day_ago, -- 第一天
	   nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-1) THEN nvl(t2.person_num,0) end),0) as two_day_ago, -- 第二天
	   nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-2) THEN nvl(t2.person_num,0) end),0) as three_day_ago, -- 第三天
	   nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-3) THEN nvl(t2.person_num,0) end),0) as four_day_ago, -- 第四天
       nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-4) THEN nvl(t2.person_num,0) end),0) as five_day_ago, -- 第五天
	   nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-5) THEN nvl(t2.person_num,0) end),0) as six_day_ago, -- 第六天
	   nvl(sum(case when t2.stat_date = DATE_ADD('${pre1_date}',-6) THEN nvl(t2.person_num,0) end),0) as seven_day_ago, -- 第七天
	   nvl(sum(case when t2.data_type = 'PE' THEN nvl(t2.person_num,0) end),0) as pe_total, -- PE近七天人天
	   nvl(sum(case when t2.data_type = '研发' THEN nvl(t2.person_num,0) end),0) as te_total, -- 研发近七天人天
	   nvl(sum(case when t2.data_type = '钉钉劳务' THEN nvl(t2.person_num,0) end),0) as se_total, -- 劳务近七天人天
	   nvl(sum(nvl(t2.person_num,0)),0) as project_total, -- 项目近七天人天
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail t1
LEFT JOIN 
(
  SELECT tmp.project_code,
         tmp.stat_date,
         tmp.data_type,
         SUM(nvl(person_num,0)) as person_num
  FROM 
  (
    -- pe日志:有记录则为一人天
    SELECT pvd.project_code,
           TO_DATE(p.log_date) as stat_date,
           'PE' as data_type,
           COUNT(DISTINCT p.applicant_user_id) as person_num
    FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p -- pms人员日志数据信息
    LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd -- 项目大表
    ON nvl(p.project_code,'unknown') = pvd.project_code OR nvl(p.project_code,'unknown') = pvd.project_sale_code
    WHERE p.d = '${pre1_date}' AND pvd.project_code is not null AND p.log_date >= DATE_ADD('${pre1_date}',-6) AND TO_DATE(p.log_date) <= '${pre1_date}' AND p.role_type = 'PE' -- 筛选PE数据
    GROUP BY pvd.project_code,p.log_date
    
    UNION ALL 
    
    -- 研发工时:超过10h为一人天。否则为半人天
    SELECT tmp.project_code,
           tmp.stat_date,
           '研发' as data_type,
           SUM(nvl(tmp.person_num,0)) as person_num
    FROM 
    (
      SELECT pvd.project_code,
             TO_DATE(t.task_start_time) AS stat_date,
             t.user_uuid,
             IF(SUM(t.task_spend_hours) >= 10,1,0.5) as person_num
      FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t -- ones工作项工时登记信息表
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 -- ones工作项信息表
      ON t.task_uuid = t1.uuid
      LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd -- 项目大表
      ON nvl(t1.external_project_code,'unknown') = pvd.project_code OR nvl(t1.external_project_code,'unknown') = pvd.project_sale_code
      WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid IS NOT NULL -- 筛选登记工时类型 且 工时状态有效 且 人员不为空
        AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') -- 筛选工作项有效 且 工作项类型为缺陷、任务、需求
        AND pvd.project_code is not null AND TO_DATE(t.task_start_time) >= DATE_ADD('${pre1_date}',-6) AND TO_DATE(t.task_start_time) <= '${pre1_date}'
      GROUP BY pvd.project_code,TO_DATE(t.task_start_time),t.user_uuid
    )tmp
    GROUP BY tmp.project_code,tmp.stat_date
    
    UNION ALL 
    
    -- 钉钉劳务:有记录则为一人天
    SELECT pvd.project_code,
           DATE(p.checkin_time) AS stat_date,
           '钉钉劳务' as data_type,
           COUNT(DISTINCT p.originator_user_id) as person_num
    FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di p -- 钉钉考务考勤信息
    LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd -- 项目大表
    ON nvl(p.project_code,'unknown') = pvd.project_code OR nvl(p.project_code,'unknown') = pvd.project_sale_code
    WHERE p.approval_status = 'COMPLETED' AND p.approval_result = 'agree' AND pvd.project_code is not null AND p.d >= DATE_ADD('${pre1_date}',-6) AND p.d <= '${pre1_date}'
    GROUP BY pvd.project_code,DATE(p.checkin_time)
  )tmp
  GROUP BY tmp.project_code,tmp.stat_date,tmp.data_type
)t2
ON t1.project_code = t2.project_code
GROUP BY t1.project_code,t1.project_sale_code,t1.project_name,t1.project_ft,t1.project_priority,t1.project_area,t1.data_source,t1.project_area_group,t1.pms_project_operation_state,t1.pms_project_status,t1.core_project_code,t1.is_main_project,t1.project_type_name,t1.pms_core_project_status,t1.pms_core_project_operation_state,t1.pms_core_project_type_name,t1.is_active;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"