  SELECT 'A' as project_code_class,
         mb.project_area,
         mb.d as month_scope,
         (mb.pm_num_month_begin + IF(me.pm_num_month_end is null,now.pm_num_month_end,me.pm_num_month_end))/2 as pm_num, -- （月初人数+月末人数/当前人数）/2
         (mb.pe_num_month_begin + IF(me.pe_num_month_end is null,now.pe_num_month_end,me.pe_num_month_end))/2 as pe_num -- （月初人数+月末人数/当前人数）/2
  -- 月初人员数量
  FROM
  (
    SELECT date_format(te.d,'yyyy-MM') as d,
           tg.project_area,
           SUM(CASE WHEN te.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') then 1 else 0 end) as pm_num_month_begin,
           SUM(CASE WHEN te.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') then 1 else 0 end) as pe_num_month_begin
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      m.org_start_date,
                      m.org_end_date,
                      m.is_job
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND te.d >= tmp.org_start_date AND te.d <= tmp.org_end_date
    LEFT JOIN 
    (
      SELECT tg.org_id,
             tg.d,
             CASE WHEN tg.dept_org_name = '海外事业部' THEN '海外' 
                  WHEN tg.parent_org_name = '华北大区' THEN '华北'
                  WHEN tg.parent_org_name = '西南大区' THEN '西南'
                  WHEN tg.parent_org_name = '华东大区' THEN '华东'
                  WHEN tg.parent_org_name = '华南大区' THEN '华南'
                  WHEN tg.parent_org_name = '华中大区' THEN '华中'
                  else '总部'
                  end as project_area
      FROM ${dim_dbname}.dim_dtk_org_history_info_df tg 
    )tg
    ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
    LEFT JOIN ${dim_dbname}.dim_day_date td
    ON te.d = td.days
    WHERE te.org_company_name = '上海快仓智能科技有限公司' AND te.d >= '2022-01-01' AND te.d <= '2022-06-12' AND te.is_active = 1 AND td.is_month_begin = 1  
      AND tg.project_area is not null 
    GROUP BY date_format(te.d,'yyyy-MM'),tg.project_area
  )mb
  -- 月底人员数量
  LEFT JOIN 
  (
    SELECT date_format(te.d,'yyyy-MM') as d,
           tg.project_area,
           SUM(CASE WHEN te.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') then 1 else 0 end) as pm_num_month_end,
           SUM(CASE WHEN te.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') then 1 else 0 end) as pe_num_month_end
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      m.org_start_date,
                      m.org_end_date,
                      m.is_job
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND te.d >= tmp.org_start_date AND te.d <= tmp.org_end_date
    LEFT JOIN 
    (
      SELECT tg.org_id,
             tg.d,
             CASE WHEN tg.dept_org_name = '海外事业部' THEN '海外' 
                  WHEN tg.parent_org_name = '华北大区' THEN '华北'
                  WHEN tg.parent_org_name = '西南大区' THEN '西南'
                  WHEN tg.parent_org_name = '华东大区' THEN '华东'
                  WHEN tg.parent_org_name = '华南大区' THEN '华南'
                  WHEN tg.parent_org_name = '华中大区' THEN '华中'
                  else '总部'
                  end as project_area
      FROM ${dim_dbname}.dim_dtk_org_history_info_df tg 
    )tg
    ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
    LEFT JOIN ${dim_dbname}.dim_day_date td
    ON te.d = td.days
    WHERE te.org_company_name = '上海快仓智能科技有限公司' AND te.d >= '2022-01-01' AND te.d <= '2022-06-12' AND te.is_active = 1 AND td.is_month_end = 1  
      AND tg.project_area is not null 
    GROUP BY date_format(te.d,'yyyy-MM'),tg.project_area
  )me
  ON mb.d = me.d AND mb.project_area = me.project_area
  -- 当前人员数量
  LEFT JOIN 
  (
    SELECT date_format(te.d,'yyyy-MM') as d,
           tg.project_area,
           SUM(CASE WHEN te.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') then 1 else 0 end) as pm_num_month_end,
           SUM(CASE WHEN te.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') then 1 else 0 end) as pe_num_month_end
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      m.org_start_date,
                      m.org_end_date,
                      m.is_job
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND te.d >= tmp.org_start_date AND te.d <= tmp.org_end_date
    LEFT JOIN 
    (
      SELECT tg.org_id,
             tg.d,
             CASE WHEN tg.dept_org_name = '海外事业部' THEN '海外' 
                  WHEN tg.parent_org_name = '华北大区' THEN '华北'
                  WHEN tg.parent_org_name = '西南大区' THEN '西南'
                  WHEN tg.parent_org_name = '华东大区' THEN '华东'
                  WHEN tg.parent_org_name = '华南大区' THEN '华南'
                  WHEN tg.parent_org_name = '华中大区' THEN '华中li'
                  else '总部'
                  end as project_area
      FROM ${dim_dbname}.dim_dtk_org_history_info_df tg 
    )tg
    ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
    LEFT JOIN ${dim_dbname}.dim_day_date td
    ON te.d = td.days
    WHERE te.org_company_name = '上海快仓智能科技有限公司' AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.is_active = 1 
      AND tg.project_area is not null 
    GROUP BY date_format(te.d,'yyyy-MM'),tg.project_area
  )now
  ON mb.d = now.d AND mb.project_area = now.project_area