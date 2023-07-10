#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads




    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ones缺陷明细 ads_ones_bug_detail

INSERT overwrite table ${ads_dbname}.ads_ones_bug_detail
SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       pc.project_assign_name as person_incharge, -- 项目负责人
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
       date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
       t.issue_type_cname as work_type, -- 工作项类型
       t.\`number\` as ones_work_id, -- ones工作项编码
       t.task_status_cname as work_status, -- 工作项状态
       t.summary as work_summary, -- 工作项标题
       date(t.task_create_time) as ones_create_date, -- ones工作项创建日期
       date(t.server_update_time) as ones_update_date, -- ones工作项更新日期
       date_format(c.task_process_time, 'yyyy-MM-dd HH:mm:ss') as ones_close_time, -- ones工作项关闭时间
       t.task_priority_value as work_priority, -- 工作项优先级
       t.severity_level as critical_level, -- 缺陷严重等级
       IF(ir.task_uuid is null,0,1) as is_remove, -- 是否移动
       IF(rt1.reopen_times is null,0,rt1.reopen_times) as reopen_time, -- 缺陷重启次数
       IF(rt.reopen_type is null,'无reopen',rt.reopen_type) as reopen_type, -- 缺陷重启流转类型
       IF(rt.reopen_type_count is null,0,rt.reopen_type_count) as reopen_type_count, -- 缺陷重启流转类型次数
       IF(c.is_effective = '无效Bug',1,0) as is_ineffective_bug, -- 是否无效bug
       case when c.old_task_field_value = '非Bug' then '非Bug'
            when c.old_task_field_value = '重复Bug' then '重复Bug'
            else '有效Bug' end as ineffective_type, -- 无效bug类型
       IF(t.property_value_map['first_category'] is null,'未知',t.property_value_map['first_category']) as bug_first_category, -- bug一级分类
       IF(t.property_value_map['second_category'] is null,'未知',t.property_value_map['second_category']) as bug_second_category, -- bug二级分类
       IF(t.property_value_map['third_category'] is null,'未知',t.property_value_map['second_category']) as bug_third_category, -- bug三级分类
       t.task_owner_cname as task_create_member, -- 工作项创建人
       IF(cft.dept_name is null,'未知',cft.dept_name) as task_create_member_ft, -- 工作项创建人所属ft
       t.task_assign_cname as task_assign_member, -- 工作项负责人
       IF(aft.dept_name is null,'未知',aft.dept_name) as task_assign_member_ft, -- 工作项负责人所属ft
       IF(t.task_solver_cname = 'UNKNOWN',NULL,t.task_solver_cname) as last_bug_solver, -- bug最后解决人
       IF(t.task_solver_cname != 'UNKNOWN' AND sft.dept_name is null,'未知',sft.dept_name) as last_bug_solver_ft, -- bug最后解决人所属ft
       tsvd.total_solve_duration as total_solve_duration, -- 修复总时长
       tsvd.total_verify_duration as total_verify_duration, -- 验证总时长
       case when t.severity_level = 1 AND ls.task_process_time is null then null
            when t.severity_level = 1 AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(ls.task_process_time) = date(t.task_create_time),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),1)) then 1
            when t.severity_level = 2 AND ls.task_process_time is null then null
            when t.severity_level = 2 AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),1),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),2)) then 1
            when t.severity_level = 3 OR t.severity_level is null then 1 
       else 0 end as bug_solve_intime_s, -- bug修复时间节点（按严重等级）
       case when t.severity_level = 1 AND lv.task_process_time is null then null
            when t.severity_level = 1 AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(lv.task_process_time) = date(t.task_create_time),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),1)) then 1
            when t.severity_level = 2 AND lv.task_process_time is null then null
            when t.severity_level = 2 AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),1),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),2)) then 1
            when t.severity_level = 3 OR t.severity_level is null then 1 
       else 0 end as bug_verify_intime_s, -- bug验证时间节点（按严重等级）
       case when t.task_priority_value = 'P0' AND ls.task_process_time is null then null
            when t.task_priority_value = 'P0' AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(ls.task_process_time) = date(t.task_create_time),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),1)) then 1
            when t.task_priority_value = 'P1' AND ls.task_process_time is null then null
            when t.task_priority_value = 'P1' AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),1),date(ls.task_process_time) <= DATE_ADD(date(t.task_create_time),2)) then 1
            when t.task_priority_value = 'P2' OR t.task_priority_value = 'P3' OR t.task_priority_value is null then 1 
       else 0 end as bug_solve_intime_p, -- bug修复时间节点（按优先级）
       case when t.task_priority_value = 'P0' AND lV.task_process_time is null then null
            when t.task_priority_value = 'P0' AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(lv.task_process_time) = date(t.task_create_time),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),1)) then 1
            when t.task_priority_value = 'P1' AND lV.task_process_time is null then null
            when t.task_priority_value = 'P1' AND IF(t.task_create_time >= date_format(t.task_create_time, 'yyyy-MM-dd 00:00:00') AND t.task_create_time < date_format(t.task_create_time, 'yyyy-MM-dd 18:00:00'),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),1),date(lv.task_process_time) <= DATE_ADD(date(t.task_create_time),2)) then 1
            when t.task_priority_value = 'P2' OR t.task_priority_value = 'P3' OR t.task_priority_value is null then 1 
       else 0 end as bug_verify_intime_p, -- bug验证时间节点（按优先级）
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t
ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid
-- 工作项关闭时间、bug是否有效、bug类型
LEFT JOIN 
(
  SELECT *,
         IF(c.old_task_field_value IN ('非Bug','重复Bug'),'无效Bug','有效Bug') as is_effective,
         row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
  FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
  WHERE c.task_process_field = 'field005' -- 状态 
    AND c.new_task_field_value = '已关闭'
)c
ON t.uuid = c.task_uuid AND c.rn = 1
-- 创建人所属ft
LEFT JOIN 
(
  SELECT t.uuid,
         d.emp_name,
         d.emp_email,
         d.dept_name,
         d.org_start_date,
         d.org_end_date,
         row_number()over(PARTITION by t.uuid order by d.org_end_date asc) rn
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df d  
  ON t.task_owner_email = d.emp_email
  WHERE d.is_valid = 1 AND d.d = DATE_ADD(CURRENT_DATE(), -1) AND IF(d.org_start_date <= date(t.task_create_time) AND d.org_end_date >= date(t.task_create_time),1,0) = 1
)cft
ON t.task_owner_email = cft.emp_email AND t.uuid = cft.uuid AND cft.rn = 1     
-- 负责人所属ft
LEFT JOIN 
(
  SELECT t.uuid,
         d.emp_name,
         d.emp_email,
         d.dept_name,
         d.org_start_date,
         d.org_end_date,
         row_number()over(PARTITION by t.uuid order by d.org_end_date asc) rn
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df d  
  ON t.task_assign_email = d.emp_email
  WHERE d.is_valid = 1 AND d.d = DATE_ADD(CURRENT_DATE(), -1) AND IF(d.org_start_date <= date(t.server_update_time) AND d.org_end_date >= date(t.server_update_time),1,0) = 1
)aft
ON t.task_assign_email = aft.emp_email AND t.uuid = aft.uuid AND cft.rn = 1     
-- 解决人所属ft
LEFT JOIN 
(
  SELECT t.uuid,
         d.emp_name,
         d.emp_email,
         d.dept_name,
         d.org_start_date,
         d.org_end_date,
         row_number()over(PARTITION by t.uuid order by d.org_end_date asc) rn
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
      AND c.new_task_field_value IN ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug')
  )r1
  ON t.uuid = r1.task_uuid AND r1.rn = 1
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df d  
  ON t.task_solver_email = d.emp_email
  WHERE d.is_valid = 1 AND d.d = DATE_ADD(CURRENT_DATE(), -1) AND IF(d.org_start_date <= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)) AND d.org_end_date >= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)),1,0) = 1
)sft
ON t.task_solver_email = sft.emp_email AND t.uuid = sft.uuid AND sft.rn = 1 
-- 重新激活类型次数
LEFT JOIN 
(
  SELECT c.task_uuid,
         case when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) = '已修复-激活' then '已修复-激活'
              when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) = '已关闭-激活' then '已关闭-激活'
              when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) not in ('已修复-激活','已关闭-激活') then '异常reopen'
         end as reopen_type,
         COUNT(c.new_task_field_value) as reopen_type_count
  FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
  WHERE c.task_process_field = 'field005' -- 状态
    AND c.new_task_field_value IN ('激活')
 GROUP BY c.task_uuid,case when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) = '已修复-激活' then '已修复-激活'
                           when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) = '已关闭-激活' then '已关闭-激活'
                           when CONCAT(c.old_task_field_value,'-',c.new_task_field_value) not in ('已修复-激活','已关闭-激活') then '异常reopen' end
)rt
ON t.uuid = rt.task_uuid
-- 重新激活总次数
LEFT JOIN 
(
  SELECT c.task_uuid,
         COUNT(c.new_task_field_value) as reopen_times
  FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
  WHERE c.task_process_field = 'field005' -- 状态
    AND c.new_task_field_value IN ('激活')
 GROUP BY c.task_uuid
)rt1
ON t.uuid = rt1.task_uuid
-- 总修复时长、总验证时长
LEFT JOIN
(
  SELECT tmp.uuid,
         SUM(case when tt.process_type = '首次修复' then (unix_timestamp(tmp.task_process_time_1)-unix_timestamp(tmp.task_create_time))
              when tt.process_type = '首次延期修复' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_create_time))
              when tt.process_type = '后续修复' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_process_time_1)) else 0 end) as total_solve_duration,
         SUM(case when tt.process_type = '正常验证' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_process_time_1)) else 0 end) as total_verify_duration
  FROM
  (
    SELECT t.uuid,
           t.task_create_time,
           c1.task_process_time as task_process_time_1,
           c1.old_task_field_value as old_task_field_value1,
           c1.new_task_field_value as new_task_field_value1,
           c1.rn as rn1,
           c2.task_process_time as task_process_time_2,
           c2.old_task_field_value as old_task_field_value2,
           c2.new_task_field_value as new_task_field_value2,
           c2.rn as rn2,
           case when c1.rn = 1 AND c1.old_task_field_value in ('激活') AND c1.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('激活') then '首次修复,后续修复'
                when c1.rn = 1 AND c1.old_task_field_value in ('激活') AND c1.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('已关闭') then '首次修复,正常验证'
                when c1.rn = 1 AND c1.old_task_field_value in ('激活') AND c1.new_task_field_value in ('延期修复') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') then '首次延期修复'
                when c1.old_task_field_value in ('已关闭') AND c1.new_task_field_value in ('激活') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') then '后续修复'
                when c1.old_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c1.new_task_field_value in ('激活') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug','延期修复') then '后续修复'
                when c1.rn != 1 AND c1.old_task_field_value in ('激活','延期修复') AND c1.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('已关闭') then '正常验证'
                when c1.rn != 1 AND c1.old_task_field_value in ('激活','延期修复') AND c1.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('激活') then '后续修复'
                when c1.rn != 1 AND c1.old_task_field_value in ('激活') AND c1.new_task_field_value in ('延期修复') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') then '后续修复'
                when c1.old_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c1.new_task_field_value in ('已关闭','激活') AND c2.old_task_field_value = c1.new_task_field_value AND c2.new_task_field_value in ('已关闭','激活') then '正常验证'
                when c1.rn = 1 AND c1.old_task_field_value in ('激活') AND c1.new_task_field_value in ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug') AND c2.old_task_field_value is null then '首次修复'
           end as process_type
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT *,row_number()over(PARTITION by c.task_uuid order by c.task_process_time asc) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005'
    )c1
    ON t.uuid = c1.task_uuid
    LEFT JOIN 
    (
      SELECT *,row_number()over(PARTITION by c.task_uuid order by c.task_process_time asc) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005'
    )c2
    ON c1.task_uuid = c2.task_uuid AND c1.rn = c2. rn - 1
    WHERE c1.task_process_field = 'field005' -- 状态
      AND t.issue_type_cname ='缺陷'
  )tmp
  LATERAL VIEW posexplode(split(tmp.process_type,',')) tt as single_id_index,process_type
  GROUP BY tmp.uuid
)tsvd
ON t.uuid = tsvd.uuid
-- 最后修复时间
LEFT JOIN 
(
  SELECT *
  FROM 
  ( 
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
      AND c.new_task_field_value IN ('单功能通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中','无法重现','重复Bug')
  )c1
  WHERE c1.rn = 1
)ls
ON t.uuid = ls.task_uuid
-- 最后验证时间
LEFT JOIN 
(
  SELECT *
  FROM 
  ( 
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
      AND c.new_task_field_value IN ('已关闭')
  )c1
  WHERE c1.rn = 1
)lv
ON t.uuid = lv.task_uuid
-- 是否移动
LEFT JOIN 
(
  SELECT *
  FROM 
  ( 
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field011' -- 所属迭代
  )c1
  WHERE c1.rn = 1
)ir
ON t.uuid = ir.task_uuid
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
  AND t.issue_type_cname = '缺陷';
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"