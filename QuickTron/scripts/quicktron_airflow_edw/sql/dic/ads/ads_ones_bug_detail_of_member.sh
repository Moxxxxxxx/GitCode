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
--ones缺陷人员明细表 ads_ones_bug_detail_of_member

INSERT overwrite table ${ads_dbname}.ads_ones_bug_detail_of_member
SELECT '' as id, -- 主键
       c.team_member, -- 人员
       c.team_ft, -- 一级部门
       c.team_group, -- 二级部门
       c.team_sub_group, -- 三级部门
       c.emp_position, -- 职位
       c.is_job, -- 是否在职
       c.hired_date, -- 入职时间
       c.quit_date, -- 离职时间
       t.\`number\` as ones_work_id, -- ones工作项编码
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       t.summary as ones_summary, -- ones工作项标题
       t.task_desc as ones_desc, -- ones工作项描述
       t.task_priority_value as work_priority, -- 工作项优先级
       t.severity_level as critical_level, -- 缺陷严重等级
       case when e.old_task_field_value = '非Bug' then '非Bug'
            when e.old_task_field_value = '重复Bug' then '重复Bug'
            else NULL end as ineffective_type, -- 无效bug类型
       t.task_owner_cname as task_create_member, -- 工作项创建人
       rt.reopen_times as reopen_times, -- 重启次数
       tsvd.total_solve_duration as total_solve_duration, -- 总修复时长
       tsvd.total_verify_duration as total_verify_duration, -- 总验证时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t
ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid
-- 人员
LEFT JOIN 
(
  SELECT DISTINCT t.uuid,
         r1.task_process_user_uuid,
         r1.task_process_user_email,
         r1.task_process_user as team_member,
         m.dept_name as team_ft,
         m.team_org_name_map['team1'] as team_group,
         m.team_org_name_map['team2'] as team_sub_group,
         te.emp_position,
         te.is_job,
         te.hired_date,
         te.quit_date
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN 
  (
    SELECT c.task_uuid,
           c.task_process_user_uuid,
           c.task_process_user,
           c.task_process_user_email,
           c.task_process_time
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
  )r1
  ON t.uuid = r1.task_uuid
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
  ON r1.task_process_user_email = m.emp_email
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON m.emp_id = te.emp_id AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  WHERE m.is_valid = 1 AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND IF(m.org_start_date <= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)) AND m.org_end_date >= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)),1,0) = 1
)c
ON t.uuid = c.uuid
-- 工作项关闭时间、bug是否有效、bug类型
LEFT JOIN 
(
  SELECT *,
         IF(c.old_task_field_value IN ('非Bug','重复Bug'),'无效Bug','有效Bug') as is_effective,
         row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
  FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
  WHERE c.task_process_field = 'field005' -- 状态 
    AND c.new_task_field_value = '已关闭'
)e
ON t.uuid = e.task_uuid AND e.rn = 1
-- 重启次数
LEFT JOIN 
(
  SELECT t.uuid,
         c1.task_process_user_uuid,
         c1.task_process_user,
         COUNT(c1.uuid) as reopen_times
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
  WHERE t.issue_type_cname ='缺陷' 
    AND c2.new_task_field_value = '激活'
  GROUP BY t.uuid,c1.task_process_user_uuid,c1.task_process_user
)rt
ON t.uuid = rt.uuid AND c.task_process_user_uuid = rt.task_process_user_uuid
-- 总修复时长、总验证时长
LEFT JOIN
(
  SELECT tmp.uuid,
         IF(tt.process_type = '首次修复',tmp.task_process_user_uuid1,tmp.task_process_user_uuid2) as task_process_user_uuid,
         IF(tt.process_type = '首次修复',tmp.task_process_user1,tmp.task_process_user2) as task_process_user,
         SUM(case when tt.process_type = '首次修复' then (unix_timestamp(tmp.task_process_time_1)-unix_timestamp(tmp.task_create_time))
              when tt.process_type = '首次延期修复' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_create_time))
              when tt.process_type = '后续修复' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_process_time_1)) else 0 end) as total_solve_duration,
         SUM(case when tt.process_type = '正常验证' then (unix_timestamp(tmp.task_process_time_2)-unix_timestamp(tmp.task_process_time_1)) else 0 end) as total_verify_duration
  FROM
  (
    SELECT t.uuid,
           t.task_create_time,
           c1.task_process_user_uuid as task_process_user_uuid1,
           c1.task_process_user as task_process_user1,
           c1.task_process_time as task_process_time_1,
           c1.old_task_field_value as old_task_field_value1,
           c1.new_task_field_value as new_task_field_value1,
           c1.rn as rn1,
           c2.task_process_user_uuid as task_process_user_uuid2,
           c2.task_process_user as task_process_user2,
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
    WHERE t.issue_type_cname ='缺陷'
  )tmp
  LATERAL VIEW posexplode(split(tmp.process_type,',')) tt as single_id_index,process_type
  GROUP BY tmp.uuid,IF(tt.process_type = '首次修复',tmp.task_process_user_uuid1,tmp.task_process_user_uuid2),IF(tt.process_type = '首次修复',tmp.task_process_user1,tmp.task_process_user2)
)tsvd
ON t.uuid = tsvd.uuid AND c.task_process_user_uuid = tsvd.task_process_user_uuid
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
  AND t.issue_type_cname in ('缺陷')
  AND c.uuid is not NULL -- 剔除状态未流转
;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"