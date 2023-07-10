--ones需求明细 ads_ones_demand_detail

INSERT overwrite table ${ads_dbname}.ads_ones_demand_detail
SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       t.issue_type_cname as work_type, -- 工作项类型
       t.\`number\` as ones_work_id, -- ones工作项编码
       t.task_status_cname as work_status, -- 工作项状态
       t.summary as work_summary, -- 工作项标题
       date(t.task_create_time) as ones_create_date, -- ones工作项创建日期
       date(t.server_update_time) as ones_update_date, -- ones工作项更新日期
       t.task_priority_value as work_priority, -- 工作项优先级
       t.task_owner_cname as task_create_member, -- 工作项创建人
       IF(cft.dept_name is null,'未知',cft.dept_name) as task_create_member_ft, -- 工作项创建人所属ft
       t.task_assign_cname as respons_user, -- 响应负责人
       IF(rft.dept_name is null,'未知',rft.dept_name) as respons_user_ft, -- 响应负责人所属ft
       t.dev_member_names as developers, -- 开发成员
       IF(it.is_test is null,0,it.is_test) as is_test, -- 是否提测
       date_format(t.measure_close_time, 'yyyy-MM-dd HH:mm:ss') as test_deadline, -- 提测截止日期
       date_format(td.task_process_time, 'yyyy-MM-dd HH:mm:ss') as test_datetime, -- 最后已提测状态日期
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t
ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid
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
-- 响应负责人所属ft
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
    WHERE c.task_process_field = 'field004' -- 负责人
  )r1
  ON t.uuid = r1.task_uuid AND r1.rn = 1
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df d  
  ON t.task_assign_email = d.emp_email
  WHERE d.is_valid = 1 AND d.d = DATE_ADD(CURRENT_DATE(), -1) AND IF(d.org_start_date <= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)) AND d.org_end_date >= IF(date(r1.task_process_time) is null,date(t.task_create_time),date(r1.task_process_time)),1,0) = 1
)rft
ON t.task_assign_email = rft.emp_email AND t.uuid = rft.uuid AND rft.rn = 1  
-- 最后已提测状态日期
LEFT JOIN 
(
  SELECT c.task_uuid,
         case when c.old_task_field_value = '已提测' AND c.new_task_field_value ='研发中' then NULL
              when c.new_task_field_value = '已提测' then c.task_process_time end as task_process_time
  FROM 
  (
    SELECT *,row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn 
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
      AND ((c.old_task_field_value = '已提测' AND c.new_task_field_value ='研发中') OR c.new_task_field_value = '已提测')
  )c
  WHERE c.rn = 1
)td
ON t.uuid = td.task_uuid
-- 是否提测
LEFT JOIN 
(
  SELECT c.task_uuid,
         IF(c.task_uuid is not null,1,0) as is_test
  FROM 
  (
    SELECT *,row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn 
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
      AND (c.old_task_field_value = '已提测' OR c.new_task_field_value = '已提测')
  )c
  WHERE c.rn = 1
)it
ON t.uuid = it.task_uuid
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
  AND t.issue_type_cname = '需求'
;