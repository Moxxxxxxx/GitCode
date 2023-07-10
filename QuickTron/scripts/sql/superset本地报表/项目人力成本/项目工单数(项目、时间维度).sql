--ads_project_work_order_new    --项目工单数统计

INSERT overwrite table ${ads_dbname}.ads_project_work_order_new
SELECT '' as id, -- 主键
       td.days as cur_date, -- 统计日期
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       if(i.work_order_num is null,0,i.work_order_num) as work_order_new_num, -- 工单数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dim_dbname}.dim_day_date td
LEFT JOIN  
(
  SELECT b.project_code,
         b.project_name
  FROM ${dwd_dbname}.dwd_share_project_base_info_df b
  WHERE b.d = DATE_ADD(CURRENT_DATE(), -1) 
    AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
    AND b.project_type_id IN (0,1,4,7,8,9)
) b
LEFT JOIN
(
  SELECT i.project_code,date(i.created_time) as work_order_create_time,count(DISTINCT i.ticket_id) as work_order_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df i
  WHERE i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.work_order_status != '已驳回'
  GROUP BY i.project_code,date(i.created_time) 
) i
ON b.project_code = i.project_code AND td.days = i.work_order_create_time
LEFT JOIN 
(
  SELECT k.string2 as project_code,
         date(k.enddate) as end_date
  FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful k
  LEFT JOIN  dwd.dwd_bpm_es_flow_info_ful e 
  ON k.flowid = e.flow_id 
  WHERE k.oFlowModelID='81695' and e.flow_status = 30
) k
ON b.project_code = k.project_code 
WHERE td.days >= '2021-01-01' AND td.days <= IF(k.end_date is null,DATE_ADD(CURRENT_DATE(), -1),k.end_date)
ORDER by td.days desc;