--项目工单类型数量统计 ads_project_workorder_type_count

--workorder_detail 工单明细
--project_view_detail 项目大表

with workorder_detail as 
(
    SELECT t1.ticket_id,
           t1.case_status,
           t1.project_code,
           date_format(t1.created_time, 'yyyy-MM-dd HH:mm:ss') as created_time,
           date_format(t2.respond_time, 'yyyy-MM-dd HH:mm:ss') as respond_time,
           date_format(t2.solve_time, 'yyyy-MM-dd HH:mm:ss') as solve_time,
           date_format(t2.close_time, 'yyyy-MM-dd HH:mm:ss') as close_time,
           case when t1.close_name = '售后' or t1.close_name = '实施' then '设备故障工单'
                when t1.close_name = '技术支持' or (t1.close_name = '研发' and t3.issue_type_cname = '任务') then '恢复工单'
                when t1.close_name = '研发' and t3.issue_type_cname = '缺陷' then '缺陷工单'
                when t1.close_name = '硬件自动化' then '硬件自动化工单'
           else IF(t1.ticket_id is not null,'其他工单',NULL) end as work_order_type -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df t1
    --工单响应关闭信息
    LEFT JOIN
    (
      SELECT ticket_id,
             MAX(case when new_change_value = '已响应' then modify_user end) as respond_user,
             MAX(case when new_change_value = '已响应' then updated_time end) as respond_time,
             MAX(case when new_change_value like '工单：%' then updated_time end) as solve_time,
             MAX(case when new_change_value = '已关闭' then modify_user end) as close_user,
             MAX(case when new_change_value = '已关闭' then updated_time end) as close_time,
             MAX(case when new_change_value = '转研发' then updated_time end) as to_rb_time
      FROM 
      (
        SELECT ticket_id,
               modify_user,
               updated_time,
               old_change_value,
               new_change_value,
               ROW_NUMBER() over (partition by ticket_id,new_change_value order by updated_time desc) rk
        FROM ${dwd_dbname}.dwd_ones_work_order_change_record_df
        WHERE 1 = 1
          AND d = DATE_ADD(CURRENT_DATE(), -1) 
          AND order_change_type = '案列状态'
        ORDER BY ticket_id,updated_time
      ) t
      WHERE t.rk = 1
      GROUP BY ticket_id
    ) t2 
    ON t2.ticket_id = t1.ticket_id
    --工单对应的ones信息
    LEFT JOIN
    (
      SELECT t1.uuid,
             t1.issue_type_cname,
             t2.field_value as work_order_id
      FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful t2
      ON t2.task_uuid = t1.uuid AND t2.field_uuid = 'S993wZTA' AND t2.field_value is not null --工单号属性:field_uuid ='S993wZTA'
    )t3
    ON t3.work_order_id = t1.ticket_id
    WHERE 1 = 1 
      AND t1.d = DATE_ADD(CURRENT_DATE(), -1) AND t1.project_code is not null AND t1.work_order_status != '已驳回' AND lower(t1.project_code) not regexp 'test|tese'
),
project_view_detail as 
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name
  FROM 
  (
    SELECT b.project_code as true_project_code, -- 项目编码
           b.project_sale_code as true_project_sale_code, -- 售前编码
           b2.project_code,
           b2.project_sale_code,
           b.project_name, -- 项目名称
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
      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
      AND b.project_type_id IN (0,1,4,7,8,9) -- 只保留外部项目/公司外部项目/售前项目/硬件部项目/纯硬件项目/自营仓项目
      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
  )tt
  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
)

INSERT overwrite table ${ads_dbname}.ads_project_workorder_type_count
-- 周
SELECT '' as id,
       string(tw.week_scope) as date_scope, -- 统计范围
       date(tw.week_first_day) as date_scope_fisrt_day, -- 统计范围首天
       '周' as run_type, -- 数据类型
       d.project_code,
       d.project_sale_code,
       d.project_name,
       wot.work_order_type, -- 工单类型
       IF(tn.new_workorder_num is null,0,tn.new_workorder_num) as new_workorder_num, -- 新增工单数量
       IF(ts.solve_workorder_num is null,0,ts.solve_workorder_num) as solve_workorder_num, -- 解决工单数量
       IF(ts.solve_duration is null,0,ts.solve_duration) as solve_duration, -- 工单解决时长
       IF(tc.close_workorder_num is null,0,tc.close_workorder_num) as close_workorder_num, -- 关闭工单数量
       IF(tc.close_duration is null,0,tc.close_duration) as close_duration, -- 工单关闭时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM project_view_detail d
LEFT JOIN 
(
  SELECT DISTINCT CONCAT(date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),'~',date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)) as week_scope,
                  date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end) as week_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) 
)tw
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT a.week_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.new_workorder_num) as new_workorder_num
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.week_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.new_workorder_num,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.week_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT CONCAT(date_add(to_date(wd.created_time), 1 - case when dayofweek(to_date(wd.created_time)) = 1 then 7 else dayofweek(to_date(wd.created_time)) - 1 end),'~',date_add(to_date(wd.created_time), 7 - case when dayofweek(to_date(wd.created_time)) = 1 then 7 else dayofweek(to_date(wd.created_time)) - 1 end)) as week_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as new_workorder_num
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY CONCAT(date_add(to_date(wd.created_time), 1 - case when dayofweek(to_date(wd.created_time)) = 1 then 7 else dayofweek(to_date(wd.created_time)) - 1 end),'~',date_add(to_date(wd.created_time), 7 - case when dayofweek(to_date(wd.created_time)) = 1 then 7 else dayofweek(to_date(wd.created_time)) - 1 end)),
                 wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.week_scope,a.true_project_code,a.work_order_type
)tn
ON d.project_code = tn.true_project_code AND tw.week_scope = tn.week_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT a.week_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.solve_workorder_num) as solve_workorder_num,
         SUM(a.solve_duration) as solve_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.week_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.solve_workorder_num,
             tmp.solve_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.week_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT CONCAT(date_add(to_date(wd.solve_time), 1 - case when dayofweek(to_date(wd.solve_time)) = 1 then 7 else dayofweek(to_date(wd.solve_time)) - 1 end),'~',date_add(to_date(wd.solve_time), 7 - case when dayofweek(to_date(wd.solve_time)) = 1 then 7 else dayofweek(to_date(wd.solve_time)) - 1 end)) as week_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as solve_workorder_num,
               SUM(cast(round((unix_timestamp(wd.solve_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as solve_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY CONCAT(date_add(to_date(wd.solve_time), 1 - case when dayofweek(to_date(wd.solve_time)) = 1 then 7 else dayofweek(to_date(wd.solve_time)) - 1 end),'~',date_add(to_date(wd.solve_time), 7 - case when dayofweek(to_date(wd.solve_time)) = 1 then 7 else dayofweek(to_date(wd.solve_time)) - 1 end)),
                 wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.week_scope,a.true_project_code,a.work_order_type
)ts
ON d.project_code = ts.true_project_code AND tw.week_scope = ts.week_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT a.week_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.close_workorder_num) as close_workorder_num,
         SUM(a.close_duration) as close_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.week_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.close_workorder_num,
             tmp.close_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.week_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT CONCAT(date_add(to_date(wd.close_time), 1 - case when dayofweek(to_date(wd.close_time)) = 1 then 7 else dayofweek(to_date(wd.close_time)) - 1 end),'~',date_add(to_date(wd.close_time), 7 - case when dayofweek(to_date(wd.close_time)) = 1 then 7 else dayofweek(to_date(wd.close_time)) - 1 end)) as week_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as close_workorder_num,
               SUM(cast(round((unix_timestamp(wd.close_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as close_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY CONCAT(date_add(to_date(wd.close_time), 1 - case when dayofweek(to_date(wd.close_time)) = 1 then 7 else dayofweek(to_date(wd.close_time)) - 1 end),'~',date_add(to_date(wd.close_time), 7 - case when dayofweek(to_date(wd.close_time)) = 1 then 7 else dayofweek(to_date(wd.close_time)) - 1 end)),
                 wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.week_scope,a.true_project_code,a.work_order_type
)tc
ON d.project_code = tc.true_project_code AND tw.week_scope = tc.week_scope AND wot.work_order_type = tc.work_order_type
	
union all
 
--月    
SELECT '' as id,
       string(tm.month_scope) as date_scope, -- 统计范围
       date(tm.month_first_day) as date_scope_fisrt_day, -- 统计范围首天
       '月' as run_type, -- 数据类型
       d.project_code,
       d.project_sale_code,
       d.project_name,
       wot.work_order_type, -- 工单类型
       IF(tn.new_workorder_num is null,0,tn.new_workorder_num) as new_workorder_num, -- 新增工单数量
       IF(ts.solve_workorder_num is null,0,ts.solve_workorder_num) as solve_workorder_num, -- 解决工单数量
       IF(ts.solve_duration is null,0,ts.solve_duration) as solve_duration, -- 工单解决时长
       IF(tc.close_workorder_num is null,0,tc.close_workorder_num) as close_workorder_num, -- 关闭工单数量
       IF(tc.close_duration is null,0,tc.close_duration) as close_duration, -- 工单关闭时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM project_view_detail d
LEFT JOIN 
(
  SELECT DISTINCT substr(days, 1, 7) as month_scope,
                  concat(substr(days, 1, 7), '-01') as month_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) 
)tm
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT a.month_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.new_workorder_num) as new_workorder_num
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.month_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.new_workorder_num,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.month_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT substr(wd.created_time, 1, 7) as month_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as new_workorder_num
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY substr(wd.created_time, 1, 7),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.month_scope,a.true_project_code,a.work_order_type
)tn
ON d.project_code = tn.true_project_code AND tm.month_scope = tn.month_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT a.month_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.solve_workorder_num) as solve_workorder_num,
         SUM(a.solve_duration) as solve_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.month_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.solve_workorder_num,
             tmp.solve_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.month_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT substr(wd.solve_time, 1, 7) as month_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as solve_workorder_num,
               SUM(cast(round((unix_timestamp(wd.solve_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as solve_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY substr(wd.solve_time, 1, 7),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.month_scope,a.true_project_code,a.work_order_type
)ts
ON d.project_code = ts.true_project_code AND tm.month_scope = ts.month_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT a.month_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.close_workorder_num) as close_workorder_num,
         SUM(a.close_duration) as close_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.month_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.close_workorder_num,
             tmp.close_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.month_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT substr(wd.close_time, 1, 7) as month_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as close_workorder_num,
               SUM(cast(round((unix_timestamp(wd.close_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as close_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY substr(wd.close_time, 1, 7),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.month_scope,a.true_project_code,a.work_order_type
)tc
ON d.project_code = tc.true_project_code AND tm.month_scope = tc.month_scope AND wot.work_order_type = tc.work_order_type

	 
union all
 
--季 
SELECT '' as id,
       string(tq.quarter_scope) as date_scope, -- 统计范围
       date(tq.quarter_first_day) as date_scope_fisrt_day, -- 统计范围首天
       '季' as run_type, -- 数据类型
       d.project_code,
       d.project_sale_code,
       d.project_name,
       wot.work_order_type, -- 工单类型
       IF(tn.new_workorder_num is null,0,tn.new_workorder_num) as new_workorder_num, -- 新增工单数量
       IF(ts.solve_workorder_num is null,0,ts.solve_workorder_num) as solve_workorder_num, -- 解决工单数量
       IF(ts.solve_duration is null,0,ts.solve_duration) as solve_duration, -- 工单解决时长
       IF(tc.close_workorder_num is null,0,tc.close_workorder_num) as close_workorder_num, -- 关闭工单数量
       IF(tc.close_duration is null,0,tc.close_duration) as close_duration, -- 工单关闭时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM project_view_detail d
LEFT JOIN  
(
  SELECT DISTINCT concat(year(days), '-', quarter(days)) as quarter_scope,
                  case when quarter(days) = 1 then concat(year(days), '-01-01')
                       when quarter(days) = 2 then concat(year(days), '-04-01')
                       when quarter(days) = 3 then concat(year(days), '-07-01')
                       when quarter(days) = 4 then concat(year(days), '-10-01') end as quarter_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) 
)tq
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT a.quarter_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.new_workorder_num) as new_workorder_num
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.quarter_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.new_workorder_num,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.quarter_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT concat(year(wd.created_time), '-', quarter(wd.created_time)) as quarter_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as new_workorder_num
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY concat(year(wd.created_time), '-', quarter(wd.created_time)),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.quarter_scope,a.true_project_code,a.work_order_type
)tn
ON d.project_code = tn.true_project_code AND tq.quarter_scope = tn.quarter_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT a.quarter_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.solve_workorder_num) as solve_workorder_num,
         SUM(a.solve_duration) as solve_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.quarter_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.solve_workorder_num,
             tmp.solve_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.quarter_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT concat(year(wd.solve_time), '-', quarter(wd.solve_time)) as quarter_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as solve_workorder_num,
               SUM(cast(round((unix_timestamp(wd.solve_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as solve_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY concat(year(wd.solve_time), '-', quarter(wd.solve_time)),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.quarter_scope,a.true_project_code,a.work_order_type
)ts
ON d.project_code = ts.true_project_code AND tq.quarter_scope = ts.quarter_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT a.quarter_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.close_workorder_num) as close_workorder_num,
         SUM(a.close_duration) as close_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.quarter_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.close_workorder_num,
             tmp.close_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.quarter_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT concat(year(wd.close_time), '-', quarter(wd.close_time)) as quarter_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as close_workorder_num,
               SUM(cast(round((unix_timestamp(wd.close_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as close_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY concat(year(wd.close_time), '-', quarter(wd.close_time)),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.quarter_scope,a.true_project_code,a.work_order_type
)tc
ON d.project_code = tc.true_project_code AND tq.quarter_scope = tc.quarter_scope AND wot.work_order_type = tc.work_order_type


union all

--年
SELECT '' as id,
       string(ty.year_scope) as date_scope, -- 统计范围
       date(ty.year_first_day) as date_scope_fisrt_day, -- 统计范围首天
       '年' as run_type, -- 数据类型
       d.project_code,
       d.project_sale_code,
       d.project_name,
       wot.work_order_type, -- 工单类型
       IF(tn.new_workorder_num is null,0,tn.new_workorder_num) as new_workorder_num, -- 新增工单数量
       IF(ts.solve_workorder_num is null,0,ts.solve_workorder_num) as solve_workorder_num, -- 解决工单数量
       IF(ts.solve_duration is null,0,ts.solve_duration) as solve_duration, -- 工单解决时长
       IF(tc.close_workorder_num is null,0,tc.close_workorder_num) as close_workorder_num, -- 关闭工单数量
       IF(tc.close_duration is null,0,tc.close_duration) as close_duration, -- 工单关闭时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM project_view_detail d
LEFT JOIN  
(
  SELECT DISTINCT year(days) as year_scope,
                  concat(year(days), '-01-01') as year_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) 
)ty
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT a.year_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.new_workorder_num) as new_workorder_num
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.year_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.new_workorder_num,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.year_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT year(wd.created_time) as year_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as new_workorder_num
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY year(wd.created_time),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.year_scope,a.true_project_code,a.work_order_type
)tn
ON d.project_code = tn.true_project_code AND ty.year_scope = tn.year_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT a.year_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.solve_workorder_num) as solve_workorder_num,
         SUM(a.solve_duration) as solve_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.year_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.solve_workorder_num,
             tmp.solve_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.year_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT year(wd.solve_time) as year_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as solve_workorder_num,
               SUM(cast(round((unix_timestamp(wd.solve_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as solve_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY year(wd.solve_time),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.year_scope,a.true_project_code,a.work_order_type
)ts
ON d.project_code = ts.true_project_code AND ty.year_scope = ts.year_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT a.year_scope,
         a.true_project_code,
         a.work_order_type,
         SUM(a.close_workorder_num) as close_workorder_num,
         SUM(a.close_duration) as close_duration
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.year_scope,
             tmp.project_code,
             tmp.work_order_type,
             tmp.close_workorder_num,
             tmp.close_duration,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code,tmp.work_order_type,tmp.year_scope order by h.start_time desc)rn
      FROM 
      (
        SELECT year(wd.close_time) as year_scope,
               wd.project_code,
               wd.work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
               COUNT(DISTINCT wd.ticket_id) as close_workorder_num,
               SUM(cast(round((unix_timestamp(wd.close_time) - unix_timestamp(wd.created_time))/ 3600, 2) as decimal(10, 2))) as close_duration
        FROM workorder_detail wd
        WHERE wd.ticket_id is not null
        GROUP BY year(wd.close_time),wd.project_code,wd.work_order_type
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.year_scope,a.true_project_code,a.work_order_type
)tc
ON d.project_code = tc.true_project_code AND ty.year_scope = tc.year_scope AND wot.work_order_type = tc.work_order_type;