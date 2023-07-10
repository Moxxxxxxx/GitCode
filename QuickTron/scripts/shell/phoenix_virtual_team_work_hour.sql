SELECT tt2.work_date,SUM(tt1.`SUM(current_lines_count)`) as back_end_codelines
FROM
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE work_date < STR_TO_DATE('2022-02-15', '%Y-%m-%d')
  AND ((second_level_directory = 'phoenix'
        and git_repository NOT LIKE '%software/phoenix/web%'))
GROUP BY work_date
)tt1
LEFT JOIN
(
SELECT work_date,sum(current_lines_count) AS `SUM(current_lines_count)`
FROM ads.ads_project_git_detail
WHERE work_date < STR_TO_DATE('2022-02-15', '%Y-%m-%d')
  AND ((second_level_directory = 'phoenix'
        and git_repository NOT LIKE '%software/phoenix/web%'))
GROUP BY work_date
)tt2
on tt1.work_date <= tt2.work_date
GROUP BY tt2.work_date









SELECT 
	work_date,
	case 
	when d.team_member = '曹琛' then '曹琛(前端)'
	when d.team_member = '张莉' then '张莉(前端)'
	when d.team_member = '李治宇' then '李治宇(前端)'
	when d.team_member = '李强' then '李强(前端)'
	when d.team_member = '张晖' then '张晖(前端)'
	when d.team_member = '高陈华' then '高陈华(前端)'
	else d.team_member
	end as team_member,
	add_lines_count,
	e.team_ft,
	e.module_branch
FROM	
(
SELECT
	work_date,
	team_member,
	SUM(add_lines_count) as add_lines_count
FROM
(
SELECT 
days as work_date, team_member, add_lines_count
FROM
(SELECT 
	DISTINCT emp_name as team_member, 0 as add_lines_count
FROM 
	ads.ads_virtual_org_emp_info_offline
WHERE 
	role_type = '研发') a,
(
select days from ads.dim_day_date where days between '2021-11-13' and current_date()
) b
UNION ALL
SELECT work_date,team_member,add_lines_count
FROM ads.ads_team_ft_member_git_detail
WHERE second_level_directory = 'phoenix' and team_member not in ('王元元','艾纯亮','张志军','娄帅帅','刘延凯','徐郁青')
ORDER BY team_member,work_date desc
) c
GROUP BY work_date,team_member
) d
left join 
(
SELECT emp_name as team_member,org_name as team_ft,module_branch from ads.ads_virtual_org_emp_info_offline where virtual_org_name = '凤凰项目' group by emp_name,org_name,module_branch
) e on d.team_member=e.team_member







CASE WHEN WEEKDAY(work_check_date) = '0' THEN '周一'
           WHEN WEEKDAY(work_check_date) = '1' THEN '周二'
           WHEN WEEKDAY(work_check_date) = '2' THEN '周三'
           WHEN WEEKDAY(work_check_date) = '3' THEN '周四'
           WHEN WEEKDAY(work_check_date) = '4' THEN '周五'
           WHEN WEEKDAY(work_check_date) = '5' THEN '周六'
           WHEN WEEKDAY(work_check_date) = '6' THEN '周日'
       END

CASE 
WHEN SUM(work_hour)>10 and weekday(work_check_date) not in ('5','6') THEN '过饱和'
WHEN SUM(work_hour)>=6 and SUM(work_hour)<=10 and weekday(work_check_date) not in ('5','6') THEN '饱和'
WHEN SUM(work_hour)<6 and weekday(work_check_date) not in ('5','6') THEN '未饱和'
WHEN weekday(work_check_date) in ('5','6') THEN '*加班'
END

CASE 
WHEN WEEKDAY(work_check_date) = '0' THEN '周一'
WHEN WEEKDAY(work_check_date) = '1' THEN '周二'
WHEN WEEKDAY(work_check_date) = '2' THEN '周三'
WHEN WEEKDAY(work_check_date) = '3' THEN '周四'
WHEN WEEKDAY(work_check_date) = '4' THEN '周五'
WHEN WEEKDAY(work_check_date) = '5' THEN '周六'
WHEN WEEKDAY(work_check_date) = '6' THEN '周日'
END


INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_manhour_detail
SELECT ''                                                      as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.user_name                                           as team_member,
       tud.role_type,
       tud.module_branch,
       tud.virtual_org_name,
       IF(tt.project_classify_name is null,t2.project_classify_name,tt.project_classify_name) as project_classify_name,
       tt.sprint_classify_name                                 as sprint_classify_name,
       cast(tt.stat_date as date)                              as work_create_date,
       tt.work_id,
       tt.summary                                              as work_summary,
       tt.task_desc                                            as work_desc,
       tt.work_type                                            as work_type,
       tt.work_status,
       cast(t2.stat_date as date)                              as work_check_date,
       cast(nvl(t2.work_hour, 0) as decimal(10, 2))            as work_hour,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM (SELECT DISTINCT tg.org_name_2 as team_ft,
                      tg.org_name_3 as team_group,
                      tg.org_name_4 as team_sub_group,
                      te.emp_id,
                      te.emp_name   as user_name,
                      te.email      as user_email,
                      tt.role_type,
                      tt.module_branch,
                      tt.virtual_org_name
      FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
      LEFT JOIN
      (
      SELECT i.emp_code,
             i.role_type,
             i.module_branch,
             i.virtual_org_name
      FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i)tt -- 多选一
      ON tt.emp_code = te.emp_id
      LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg on tg.org_id = SUBSTRING_INDEX(te.org_ids,',',1) 
      WHERE 1 = 1
        and te.d = DATE_ADD(CURRENT_DATE(), -1) and te.org_company_name = '上海快仓智能科技有限公司'
) tud
left join (select to_date(t1.task_create_time) as stat_date,
                         t1.uuid,
                         t1.\`number\` as work_id,
                         t1.summary,
                         t1.task_desc,
                         t1.task_assign        as user_uuid,
                         t1.project_classify_name,
                         t1.sprint_classify_name,
			             tou.user_email,
			             case when t1.issue_type_uuid = 'Esc1h8Fw' then '缺陷'
                              when t1.issue_type_uuid = 'TcR9MH7K' then '任务'
                              when t1.issue_type_uuid = 'GyZUtv3N' then '需求' end as work_type,
                         t3.task_cname        as work_status
                  from ${dwd_dbname}.dwd_ones_task_info_ful t1
	              left join ${dwd_dbname}.dwd_ones_org_user_info_ful tou on tou.uuid=t1.task_assign and tou.user_status = 1
	              left join ${dim_dbname}.dim_ones_issue_type t2 on t2.uuid = case when t1.sub_issue_type_uuid is null or t1.sub_issue_type_uuid = '' then t1.issue_type_uuid else t1.sub_issue_type_uuid end
	              left join ${dim_dbname}.dim_ones_task_status t3 on t3.uuid = t1.status_uuid
                  where 1 = 1
                  and t1.status = 1
                  and t2.issue_type_cname regexp '缺陷|任务|需求'
) tt on tt.user_email = tud.user_email
left join (select to_date(t.task_start_time)                              as stat_date,
                         t.task_uuid,
                         t.user_uuid,
                         t.project_classify_name,
						 tou.user_email,
                         round(COALESCE(sum(t.task_spend_hours) / 100000, 0), 2) as work_hour
                  from ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
				  left join ${dwd_dbname}.dwd_ones_org_user_info_ful tou on tou.uuid=t.user_uuid and tou.user_status = 1
                  where 1=1
				  and t.task_type=1
				  and t.status=1
				  and t.user_uuid is not null
                  group by to_date(t.task_start_time),t.task_uuid,t.user_uuid,t.project_classify_name,tou.user_email
) t2 on t2.user_email = tud.user_email and t2.task_uuid = tt.uuid and t2.project_classify_name = tt.project_classify_name
WHERE tt.work_id is not null; 





