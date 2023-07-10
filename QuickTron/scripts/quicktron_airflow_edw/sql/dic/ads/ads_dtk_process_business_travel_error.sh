#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--ads_dtk_process_business_travel_error    --出差审批异常信息记录表
INSERT overwrite table ${ads_dbname}.ads_dtk_process_business_travel_error
SELECT DISTINCT '' as id,
       tmp.cur_date,
       tmp.org_name,
       tmp.business_id, -- 审批业务单号
       tmp.project_code, -- 项目编号
       tmp.project_name, -- 项目名称
       tmp.project_ft, -- 所属产品线
       tmp.project_operation_state, -- 项目运营阶段
       tmp.team_name, -- 团队
       tmp.member_name, -- 成员
       tmp.member_function, -- 职能【PM,PE,TE,顾问】
       tmp.error_type,-- 异常类型
       tmp.trip_duration, -- 出差天数（天）
       tmp.start_time, -- 出差开始时间
       tmp.end_time, -- 出差结束时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
-- 出差 项目编码不存在
SELECT date(t.create_time) as cur_date,
       t.org_name,
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       '项目编码不存在' as error_type,-- 异常类型
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time -- 出差结束时间
  FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
  ON t.originator_user_id = i.emp_id
  WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.is_project_matching = 0
   AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
   
   UNION ALL 
   
-- 出差 出差申请时间段有交叉
SELECT DISTINCT tt1.cur_date,
       tt1.org_name,
       tt1.business_id,
       tt1.project_code,
       tt1.project_name,
       tt1.project_ft,
       tt1.project_operation_state,
       tt1.team_name,
       tt1.member_name,
       tt1.member_function,
       '出差申请时间段有交叉' as error_type,
       tt1.trip_duration,
       tt1.start_time,
       tt1.end_time
FROM 
(
SELECT date(t.create_time) as cur_date, -- 统计时间
       t.org_name,
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',IF(t.start_am_or_pm = 'AM','00:00:00','12:00:00')) as start_time_desc,
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',IF(t.end_am_or_pm = 'AM','00:00:00','12:00:00')) as end_time_desc,
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time -- 出差结束时间
       --row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
ON t.originator_user_id = i.emp_id
WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.is_project_matching = 1 -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
  AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
  AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
)tt1
LEFT JOIN 
(
SELECT date(t.create_time) as cur_date, -- 统计时间
       t.org_name,
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',IF(t.start_am_or_pm = 'AM','00:00:00','12:00:00')) as start_time_desc,
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',IF(t.end_am_or_pm = 'AM','00:00:00','12:00:00')) as end_time_desc,
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time -- 出差结束时间
       --row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
ON t.originator_user_id = i.emp_id
WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.is_project_matching = 1 -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
  AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
  AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
)tt2
ON tt1.member_name = tt2.member_name and tt1.business_id != tt2.business_id and tt2.start_time_desc >= tt1.start_time_desc and tt2.start_time_desc <= tt1.end_time_desc
WHERE tt2.cur_date is not null 

UNION ALL 

SELECT DISTINCT tt2.cur_date,
       tt2.org_name,
       tt2.business_id,
       tt2.project_code,
       tt2.project_name,
       tt2.project_ft,
       tt2.project_operation_state,
       tt2.team_name,
       tt2.member_name,
       tt2.member_function,
       '出差申请时间段有交叉' as error_type,
       tt2.trip_duration,
       tt2.start_time,
       tt2.end_time
FROM 
(
SELECT date(t.create_time) as cur_date, -- 统计时间
       t.org_name,
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',IF(t.start_am_or_pm = 'AM','00:00:00','12:00:00')) as start_time_desc,
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',IF(t.end_am_or_pm = 'AM','00:00:00','12:00:00')) as end_time_desc,
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time -- 出差结束时间
       --row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
ON t.originator_user_id = i.emp_id
WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.is_project_matching = 1 -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
  AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
  AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
)tt1
LEFT JOIN 
(
SELECT date(t.create_time) as cur_date, -- 统计时间
       t.org_name,
       t.business_id, -- 审批业务单号
       t.project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       i.org_cnames as team_name, -- 团队
       i.emp_name as member_name, -- 成员
       CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
            WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
            WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
            WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
            ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
       t.business_travel_days as trip_duration, -- 出差天数（天）
       CONCAT(t.start_date,' ',IF(t.start_am_or_pm = 'AM','00:00:00','12:00:00')) as start_time_desc,
       CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
       CONCAT(t.end_date,' ',IF(t.end_am_or_pm = 'AM','00:00:00','12:00:00')) as end_time_desc,
       CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time -- 出差结束时间
       --row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
ON t.originator_user_id = i.emp_id
WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.is_project_matching = 1 -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
  AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
  AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
)tt2
ON tt1.member_name = tt2.member_name and tt1.business_id != tt2.business_id and tt2.start_time_desc >= tt1.start_time_desc and tt2.start_time_desc <= tt1.end_time_desc
WHERE tt2.cur_date is not null
)tmp;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"