#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
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
--项目工单类型数量统计 ads_project_workorder_type_count

--workorder_detail 工单明细

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
        WHERE  d = '${pre1_date}' AND order_change_type = '案列状态'
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
      ON t2.task_uuid = t1.uuid 
      WHERE t2.field_uuid = 'S993wZTA' AND t2.field_value is not null --工单号属性:field_uuid ='S993wZTA'
    )t3
    ON t3.work_order_id = t1.ticket_id
    WHERE t1.d = '${pre1_date}' AND t1.project_code is not null AND t1.work_order_status != '已驳回' AND lower(t1.project_code) not regexp 'test|tese'
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
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
LEFT JOIN 
(
  SELECT CONCAT(date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),'~',date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)) as week_scope,
         date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end) as week_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= '${pre1_date}'
  GROUP BY CONCAT(date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),'~',date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)),
           date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)
)tw
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT tmp.week_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.new_workorder_num) as new_workorder_num
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.week_scope,s.project_code,tmp.work_order_type
)tn
ON d.project_code = tn.project_code AND tw.week_scope = tn.week_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT tmp.week_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.solve_workorder_num) as solve_workorder_num,
         SUM(tmp.solve_duration) as solve_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.week_scope,s.project_code,tmp.work_order_type
)ts
ON d.project_code = ts.project_code AND tw.week_scope = ts.week_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT tmp.week_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.close_workorder_num) as close_workorder_num,
         SUM(tmp.close_duration) as close_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.week_scope,s.project_code,tmp.work_order_type
)tc
ON d.project_code = tc.project_code AND tw.week_scope = tc.week_scope AND wot.work_order_type = tc.work_order_type
	
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
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
LEFT JOIN 
(
  SELECT DISTINCT substr(days, 1, 7) as month_scope,
                  concat(substr(days, 1, 7), '-01') as month_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= '${pre1_date}'
)tm
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT tmp.month_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.new_workorder_num) as new_workorder_num
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.month_scope,s.project_code,tmp.work_order_type
)tn
ON d.project_code = tn.project_code AND tm.month_scope = tn.month_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT tmp.month_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.solve_workorder_num) as solve_workorder_num,
         SUM(tmp.solve_duration) as solve_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.month_scope,s.project_code,tmp.work_order_type
)ts
ON d.project_code = ts.project_code AND tm.month_scope = ts.month_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT tmp.month_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.close_workorder_num) as close_workorder_num,
         SUM(tmp.close_duration) as close_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.month_scope,s.project_code,tmp.work_order_type
)tc
ON d.project_code = tc.project_code AND tm.month_scope = tc.month_scope AND wot.work_order_type = tc.work_order_type

	 
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
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
LEFT JOIN  
(
  SELECT DISTINCT concat(year(days), '-', quarter(days)) as quarter_scope,
                  case when quarter(days) = 1 then concat(year(days), '-01-01')
                       when quarter(days) = 2 then concat(year(days), '-04-01')
                       when quarter(days) = 3 then concat(year(days), '-07-01')
                       when quarter(days) = 4 then concat(year(days), '-10-01') end as quarter_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= '${pre1_date}'
)tq
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT tmp.quarter_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.new_workorder_num) as new_workorder_num
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.quarter_scope,s.project_code,tmp.work_order_type
)tn
ON d.project_code = tn.project_code AND tq.quarter_scope = tn.quarter_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT tmp.quarter_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.solve_workorder_num) as solve_workorder_num,
         SUM(tmp.solve_duration) as solve_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.quarter_scope,s.project_code,tmp.work_order_type
)ts
ON d.project_code = ts.project_code AND tq.quarter_scope = ts.quarter_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT tmp.quarter_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.close_workorder_num) as close_workorder_num,
         SUM(tmp.close_duration) as close_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.quarter_scope,s.project_code,tmp.work_order_type
)tc
ON d.project_code = tc.project_code AND tq.quarter_scope = tc.quarter_scope AND wot.work_order_type = tc.work_order_type


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
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
LEFT JOIN  
(
  SELECT DISTINCT year(days) as year_scope,
                  concat(year(days), '-01-01') as year_first_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-07-01' AND days <= '${pre1_date}'
)ty
LEFT JOIN 
(
  SELECT explode(split('设备故障工单,恢复工单,缺陷工单,硬件自动化工单,其他工单',',')) as work_order_type
)wot
-- 新增工单数量
LEFT JOIN 
(
  SELECT tmp.year_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.new_workorder_num) as new_workorder_num
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.year_scope,s.project_code,tmp.work_order_type
)tn
ON d.project_code = tn.project_code AND ty.year_scope = tn.year_scope AND wot.work_order_type = tn.work_order_type
-- 解决工单数量及时长
LEFT JOIN 
(
  SELECT tmp.year_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.solve_workorder_num) as solve_workorder_num,
         SUM(tmp.solve_duration) as solve_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.year_scope,s.project_code,tmp.work_order_type
)ts
ON d.project_code = ts.project_code AND ty.year_scope = ts.year_scope AND wot.work_order_type = ts.work_order_type
-- 关闭工单数量及时长
LEFT JOIN 
(
  SELECT tmp.year_scope,
         s.project_code,
         tmp.work_order_type,
         SUM(tmp.close_workorder_num) as close_workorder_num,
         SUM(tmp.close_duration) as close_duration
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
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s
  ON s.d = '${pre1_date}'and (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code)
  WHERE tmp.project_code is not null
  GROUP BY tmp.year_scope,s.project_code,tmp.work_order_type
)tc
ON d.project_code = tc.project_code AND ty.year_scope = tc.year_scope AND wot.work_order_type = tc.work_order_type;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"