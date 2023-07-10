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
-- ones异常工时日统计 ads_ones_unusual_workhour_daily 

INSERT overwrite table ${ads_dbname}.ads_ones_unusual_workhour_daily
SELECT ''                                                      as id,
       tud.days                                                 as cur_date,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       SUM(IF(tmp.work_hour is null,0,tmp.work_hour))          as unusual_work_hour,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
(
  SELECT tu.team_ft,
         tu.team_group,
	     tu.team_sub_group,
         tu.emp_id,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tu.is_need_fill_manhour,
         tu.emp_position,
         td.days,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type    
  FROM
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job,
                    tmp.is_need_fill_manhour,
                    te.hired_date,
                    te.quit_date,
                    te.emp_position
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司' AND tmp.is_need_fill_manhour = 1
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days = DATE_ADD(CURRENT_DATE(), -1)
  ) td
) tud
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         tou.user_email,
         sum(t.task_spend_hours) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON tou.user_email = te.email AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
    AND (IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null),'未知项目编码',t1.external_project_code) = '未知项目编码'
         OR IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null),'未知项目编码',t1.project_bpm_code) = '未知项目编码')
    AND t1.project_type_name != '技术&管理工作' 
    AND to_date(t.task_start_time) >= te.hired_date
  GROUP BY to_date(t.task_start_time),tou.user_email
) tmp
ON tmp.user_email = tud.user_email AND tmp.stat_date <= tud.days
GROUP BY tud.days,tud.team_ft,tud.team_group,tud.team_sub_group;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"