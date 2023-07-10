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
-------------------------------------------------------------------------------------------------------------00
--团队成员工作项状态分布 ads_team_ft_member_issue_status

INSERT overwrite table ${ads_dbname}.ads_team_ft_member_issue_status
-- 周维度
SELECT ''                                                      as id,
       tuw.team_ft,
       tuw.team_group,
	   tuw.team_sub_group,
       tuw.user_name                                           as team_member,
       tuw.is_job,
       tuw.role_type,
       '周'                                                     as run_type,
       cast(tuw.week_first_day as date)                        as time_value,
       t.work_status,
       cast(nvl(t.handle_ones_num, 0) as bigint)               as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
         tu.team_sub_group,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tw.week_scope,
         tw.week_first_day
  FROM 
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT CONCAT(DATE_ADD(TO_DATE(days), 1 - case when DAYOFWEEK(TO_DATE(days)) = 1 then 7 else DAYOFWEEK(TO_DATE(days)) - 1 end), '~', DATE_ADD(TO_DATE(days), 7 - case when DAYOFWEEK(TO_DATE(days)) = 1 then 7 else DAYOFWEEK(TO_DATE(days)) - 1 end)) as week_scope,
                    DATE_ADD(TO_DATE(days), 1 - case when DAYOFWEEK(TO_DATE(days)) = 1 then 7 else DAYOFWEEK(TO_DATE(days)) - 1 end) as week_first_day
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) AND DATE_ADD(TO_DATE(days), 1 - case when DAYOFWEEK(TO_DATE(days)) = 1 then 7 else DAYOFWEEK(TO_DATE(days)) - 1 end) >= '2021-01-01'
  ) tw
) tuw
LEFT JOIN
(
  SELECT tt.week_scope,
         tt.week_first_day,
         tt.ones_user_uuid,
	     tt.user_email,
         tt.work_status,
         COUNT(DISTINCT tt.task_uuid) as handle_ones_num
  FROM 
  (
    SELECT DISTINCT t1.week_scope,
                    t1.week_first_day,
                    t1.week_last_day,
                    t1.ones_user_uuid,
                    t1.ones_user_name,
                    t1.task_uuid,
                    t1.user_email,
                    COALESCE(FIRST_VALUE(t2.new_task_field_value) over (PARTITION BY t1.task_uuid,t1.ones_user_uuid order by t2.task_process_time desc),'未开始') as work_status
    FROM
    --时间段内全集
    (
      SELECT DISTINCT CONCAT(DATE_ADD(TO_DATE(th.task_process_time), 1 - case when DAYOFWEEK(TO_DATE(th.task_process_time)) = 1 then 7 else DAYOFWEEK(TO_DATE(th.task_process_time)) - 1 end), '~', DATE_ADD(TO_DATE(th.task_process_time), 7 - case when DAYOFWEEK(TO_DATE(th.task_process_time)) = 1 then 7 else DAYOFWEEK(TO_DATE(th.task_process_time)) - 1 end)) as week_scope,
                      DATE_ADD(TO_DATE(th.task_process_time), 1 - case when DAYOFWEEK(TO_DATE(th.task_process_time)) = 1 then 7 else DAYOFWEEK(TO_DATE(th.task_process_time)) - 1 end) as week_first_day,
                      DATE_ADD(TO_DATE(th.task_process_time), 7 - case when DAYOFWEEK(TO_DATE(th.task_process_time)) = 1 then 7 else DAYOFWEEK(TO_DATE(th.task_process_time)) - 1 end) as week_last_day,
                      th.task_assign_uuid as ones_user_uuid,
                      th.task_assign_name as ones_user_name,
                      th.task_uuid,
                      t1.task_assign_email as user_email
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his th
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 
      ON t1.uuid = th.task_uuid
      WHERE 1 = 1 
        AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求')
        AND TO_DATE(th.task_process_time) >= '2021-01-01' AND TO_DATE(th.task_process_time) <= DATE_ADD(CURRENT_DATE(), -1)
    ) t1
    LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his t2
    ON t2.task_uuid = t1.task_uuid and t2.task_assign_uuid = t1.ones_user_uuid AND task_process_field_type = '状态' and TO_DATE(t2.task_process_time) <= TO_DATE(T1.week_last_day)
  ) tt
  GROUP BY tt.week_scope,tt.week_first_day,tt.ones_user_uuid,tt.user_email,tt.work_status
) t
ON t.week_scope = tuw.week_scope and t.week_first_day = tuw.week_first_day and t.user_email = tuw.user_email

UNION ALL

-- 月维度
SELECT ''                                                      as id,
       tum.team_ft,
       tum.team_group,
	   tum.team_sub_group,
       tum.user_name                                           as team_member,
       tum.is_job,
       tum.role_type,
       '月'                                                     as run_type,
       cast(tum.month_first_day as date)                       as time_value,
       t.work_status,
       cast(nvl(t.handle_ones_num, 0) as bigint)               as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
         tu.team_sub_group,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tm.month_scope,
         tm.month_first_day
  FROM 
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT substr(days, 1, 7)                as month_scope,
                    concat(substr(days, 1, 7), '-01') as month_first_day
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) tm
) tum
LEFT JOIN
(
  SELECT tt.month_scope,
         tt.month_first_day,
         tt.ones_user_uuid,
	     tt.user_email,
         tt.work_status,
         COUNT(DISTINCT tt.task_uuid) as handle_ones_num
  FROM 
  (
    SELECT DISTINCT t1.month_scope,
                    t1.month_first_day,
                    t1.month_last_day,
                    t1.ones_user_uuid,
                    t1.ones_user_name,
                    t1.task_uuid,
					t1.user_email,
                    COALESCE(FIRST_VALUE(t2.new_task_field_value) over (PARTITION BY t1.task_uuid,ones_user_uuid order by t2.task_process_time desc),'未开始') as work_status
    FROM
    --时间段内全集
    (
      SELECT DISTINCT substr(th.task_process_time, 1, 7)                as month_scope,
                      concat(substr(th.task_process_time, 1, 7), '-01') as month_first_day,
                      LAST_DAY(th.task_process_time)                    as month_last_day,
                      th.task_assign_uuid                               as ones_user_uuid,
                      th.task_assign_name                               as ones_user_name,
                      th.task_uuid,
				      t1.task_assign_email as user_email
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his th
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 
      ON t1.uuid = th.task_uuid
      WHERE 1 = 1
        AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求')
        AND TO_DATE(th.task_process_time) >= '2021-01-01' AND TO_DATE(th.task_process_time) <= DATE_ADD(CURRENT_DATE(), -1)
    ) t1
    LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his t2
    ON t2.task_uuid = t1.task_uuid AND t2.task_assign_uuid = t1.ones_user_uuid AND task_process_field_type = '状态' AND TO_DATE(t2.task_process_time) <= TO_DATE(T1.month_last_day)
  ) tt
  GROUP BY tt.month_scope,tt.month_first_day,tt.ones_user_uuid,tt.user_email,tt.work_status
) t
ON t.month_scope = tum.month_scope AND t.month_first_day = tum.month_first_day AND t.user_email = tum.user_email

UNION ALL

-- 季维度
SELECT ''                                                      as id,
       tuq.team_ft,
       tuq.team_group,
	   tuq.team_sub_group,
       tuq.user_name                                           as team_member,
       tuq.is_job,
       tuq.role_type,
       '季'                                                     as run_type,
       cast(tuq.quarter_first_day as date)                     as time_value,
       t.work_status,
       cast(nvl(t.handle_ones_num, 0) as bigint)               as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
		 tu.team_sub_group,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tq.quarter_scope,
         tq.quarter_first_day
  FROM 
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT concat(year(days), '-', quarter(days)) as quarter_scope,
                    case when quarter(days) = 1 then concat(year(days), '-01-01')
                         when quarter(days) = 2 then concat(year(days), '-04-01')
                         when quarter(days) = 3 then concat(year(days), '-07-01')
                         when quarter(days) = 4 then concat(year(days), '-10-01') end as quarter_first_day
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) tq
) tuq
LEFT JOIN
(
  SELECT tt.quarter_scope,
         tt.quarter_first_day,
         tt.ones_user_uuid,
	     tt.user_email,
         tt.work_status,
         COUNT(DISTINCT tt.task_uuid) as handle_ones_num
  FROM 
  (
    SELECT DISTINCT t1.quarter_scope,
                    t1.quarter_first_day,
                    t1.quarter_last_day,
                    t1.ones_user_uuid,
                    t1.ones_user_name,
                    t1.task_uuid,
					t1.user_email,
                    COALESCE(FIRST_VALUE(t2.new_task_field_value) over (PARTITION BY t1.task_uuid,ones_user_uuid order by t2.task_process_time desc),'未开始') as work_status
    FROM
    --时间段内全集
    (
      SELECT DISTINCT concat(year(th.task_process_time), '-', quarter(th.task_process_time)) as quarter_scope,
                      case when quarter(th.task_process_time) = 1 then concat(year(th.task_process_time), '-01-01')
                           when quarter(th.task_process_time) = 2 then concat(year(th.task_process_time), '-04-01')
                           when quarter(th.task_process_time) = 3 then concat(year(th.task_process_time), '-07-01')
                           when quarter(th.task_process_time) = 4 then concat(year(th.task_process_time), '-10-01') end as quarter_first_day,
                      DATE_ADD(case when quarter(add_months(th.task_process_time, 3)) = 1 then concat(year(add_months(th.task_process_time, 3)), '-01-01')
                                    when quarter(add_months(th.task_process_time, 3)) = 2 then concat(year(add_months(th.task_process_time, 3)), '-04-01')
                                    when quarter(add_months(th.task_process_time, 3)) = 3 then concat(year(add_months(th.task_process_time, 3)), '-07-01')
                                    when quarter(add_months(th.task_process_time, 3)) = 4 then concat(year(add_months(th.task_process_time, 3)), '-10-01') end, -1) as quarter_last_day,
                      th.task_assign_uuid as ones_user_uuid,
                      th.task_assign_name as ones_user_name,
                      th.task_uuid,
				      t1.task_assign_email as user_email
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his th
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 
      ON t1.uuid = th.task_uuid
      where 1 = 1
        AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求')
        AND to_date(th.task_process_time) >= '2021-01-01' AND to_date(th.task_process_time) <= DATE_ADD(CURRENT_DATE(), -1)
    ) t1
    LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his t2
    ON t2.task_uuid = t1.task_uuid AND t2.task_assign_uuid = t1.ones_user_uuid AND task_process_field_type = '状态' AND to_date(t2.task_process_time) <= TO_DATE(T1.quarter_last_day)
  ) tt
  GROUP BY tt.quarter_scope,tt.quarter_first_day,tt.ones_user_uuid,tt.user_email,tt.work_status
) t
ON t.quarter_scope = tuq.quarter_scope AND t.quarter_first_day = tuq.quarter_first_day AND t.user_email = tuq.user_email;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"