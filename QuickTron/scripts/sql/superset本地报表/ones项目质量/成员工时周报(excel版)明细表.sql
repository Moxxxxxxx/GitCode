#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads


# �������������ڰ���ȡ�������ڣ����û��������ȡ��ǰʱ���ǰһ��
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- ��Ա��ʱ�ܱ�(excel��)��ϸ�� ads_member_work_detail_report 


with work_day as 
(
  SELECT nvl(tmp1.originator_user_id,nvl(tmp2.originator_user_id,tmp3.originator_user_id)) as originator_user_id,
         nvl(tmp1.stat_date,nvl(tmp2.stat_date,tmp3.stat_date)) as stat_date,
         tmp1.create_time as travel_create_time,
         tmp1.travel_type as travel_type,
         tmp2.create_time as work_home_create_time,
         tmp2.travel_type as work_home_type,
         tmp3.create_time as attend_bus_create_time,
         tmp3.travel_type as attend_bus_type
  FROM
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days 
    FROM 
    (
      SELECT t.originator_user_id,
             t.create_time,
             cast(t.travel_date as date) as stat_date,
             CASE when t.period_type = 'ȫ��' THEN 'ȫ�����'
                  when t.period_type = '����' THEN '�°������'
                  when t.period_type = '����' THEN '�ϰ������' end as travel_type,
             t.every_days as travel_days,
             row_number()over(PARTITION by t.originator_user_id,cast(t.travel_date as date) order by t.create_time desc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
    )t1
    WHERE t1.rn = 1 
  )tmp1
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days    
    FROM 
    (
      SELECT w.originator_user_id,
             w.create_time,
             cast(w.work_home_date as date) as stat_date,
             CASE when w.period_type = 'ȫ��' THEN 'ȫ��Ӽ�'
                  when w.period_type = '����' THEN '�°���Ӽ�'
                  when w.period_type = '����' THEN '�ϰ���Ӽ�' end as travel_type,
             w.every_days as travel_days,
             row_number()over(PARTITION by w.originator_user_id,cast(w.work_home_date as date) order by w.create_time desc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_for_home_dayily_info_df w
      WHERE w.is_valid = 1 AND w.d = '${pre1_date}' AND w.approval_result = 'agree' AND w.approval_status = 'COMPLETED' 
    )t1
    WHERE t1.rn = 1 
  )tmp2
  ON tmp1.originator_user_id = tmp2.originator_user_id AND tmp1.stat_date = tmp2.stat_date
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days    
    FROM 
    (
      SELECT w.originator_user_id,
             w.create_time,
             cast(w.attend_bus_date as date) as stat_date,
             CASE when w.period_type = 'ȫ��' THEN 'ȫ�칫��'
                  when w.period_type = '����' THEN '�°��칫��'
                  when w.period_type = '����' THEN '�ϰ��칫��' end as travel_type,
             w.every_days as travel_days,
             row_number()over(PARTITION by w.originator_user_id,cast(w.attend_bus_date as date) order by w.create_time desc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_attendance_business_dayily_info_df w
      WHERE w.is_valid = 1 AND w.d = '${pre1_date}' AND w.approval_result = 'agree' AND w.approval_status = 'COMPLETED' 
    )t1
    WHERE t1.rn = 1 
  )tmp3
  ON tmp1.originator_user_id = tmp3.originator_user_id AND tmp1.stat_date = tmp3.stat_date
)


INSERT overwrite table ${ads_dbname}.ads_member_work_detail_report
SELECT  '' as id,
	   IF(tud.is_job = 0,tud.team_ft,tud.team_sub_group) as org_name, -- ��ְ���һ�����ţ���ְȡ��ײ�
       tud.user_name as team_member,
       tud.emp_id as dtk_emp_id,
	   tud.emp_position,
       cast(tud.days as date) as work_date,
       tud.day_type,
       cast(nvl(t1.work_hour, 0) as decimal(10, 2)) as work_hour, -- �Ǽǹ�ʱ
       cast(nvl(t2.clock_in_work_hour, 0) as decimal(10, 2)) as clock_in_hour, -- �򿨹�ʱ
       cast(nvl(t3.leave_days, 0) as decimal(10, 2)) as leave_days, -- �������
       cast(nvl(t4.travel_days, 0) as decimal(10, 2)) as travel_days, -- ��������
       cast(nvl(t5.home_office_days, 0) as decimal(10, 2)) as home_office_days, -- �ӼҰ칫����
       cast(nvl(t6.demand_qty, 0) as bigint) as demand_qty, -- ��������
       t6.demand_ones_ids, -- ����id
       cast(nvl(t7.bug_qty, 0) as bigint) as bug_qty, -- ȱ������
       t7.bug_ones_ids, -- ȱ��id
       cast(nvl(t8.task_qty, 0) as bigint) as task_qty, -- ��������
       t8.task_ones_ids, -- ����id
       cast(nvl(t9.work_order_qty, 0) as bigint) as work_order_qty, -- ��������
       t9.work_order_ones_ids, -- ����id
       cast(nvl(t10.code_quantity, 0) as bigint) as add_lines_count, -- ���빱����
       cast(nvl(t11.internal_wh, 0) as decimal(10, 2)) as internal_wh, -- �ڲ��з���ʱ
       nvl(cast(nvl(t11.internal_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as internal_occ, -- �ڲ���ʱռ��
       cast(nvl(t12.external_wh, 0) as decimal(10, 2)) as external_wh, -- �ⲿ�з���ʱ
       nvl(cast(nvl(t12.external_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as external_occ, -- �ⲿ��ʱռ��
       cast(nvl(t13.mgmt_wh, 0) as decimal(10, 2)) as mgmt_wh, -- ����&��������ʱ
       nvl(cast(nvl(t13.mgmt_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as mgmt_occ, -- ����ʱռ��
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.team_ft,
	     tu.team_sub_group,
         tu.emp_id,
         tu.emp_name as user_name,
         tu.email as user_email,
         tu.is_job,
         tu.emp_position,
         td.days,
         CASE when td.week_date = 1 then '��һ'
              when td.week_date = 2 then '�ܶ�'
              when td.week_date = 3 then '����'
              when td.week_date = 4 then '����'
              when td.week_date = 5 then '����'
              when td.week_date = 6 then '����'
              when td.week_date = 7 then '����' end as day_type    
  FROM
  (
    SELECT te.emp_id,
           te.emp_name,
           te.email,
           te.emp_position,
           te.is_job,
           te.hired_date,
           te.quit_date,
           m.dept_name as team_ft,
           m.org_name as team_sub_group
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT m.emp_id,
             m.emp_name,
             m.org_start_date,
             m.org_end_date,
             m.dept_name,
             m.org_name,
             row_number()over(PARTITION by m.emp_id order by m.org_end_date desc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.is_need_fill_manhour = 1 AND (m.org_end_date = '9999-01-01' OR m.org_end_date <= DATE_ADD(CURRENT_DATE(), -1))
    )m
    ON te.emp_id = m.emp_id AND m.rn = 1 
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾' 
      AND (te.is_job = 1 OR te.quit_date <= '${pre1_date}')
      AND m.emp_id is not NULL 
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type,
                    week_date
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= '${pre1_date}'
  ) td
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') 
)tud
-- �Ǽǹ�ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳���Ч��ʱ��Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
)t1
ON t1.user_email = tud.user_email AND t1.stat_date = tud.days
-- �򿨹�ʱ
LEFT JOIN 
(
  SELECT e.emp_id,
         e.emp_name,
         e.att_checkin_work_date as attendance_work_date,
         cast((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(e.att_checkin_start_time))/3600 as decimal(10,1)) as clock_in_work_hour
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di e
)t2
ON tud.emp_id = t2.emp_id AND tud.days = t2.attendance_work_date
-- ���ͳ��
LEFT JOIN 
(
  SELECT l1.originator_user_id,
         l1.stat_date,
         case when l2.leave_type is null THEN l1.leave_type else 'ȫ�����' END as leave_type,
         case when l2.leave_type is null THEN l1.leave_days else 1 END as leave_days
  FROM 
  (
    SELECT l.originator_user_id,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                when l.period_type = '����' THEN '�°������'
                when l.period_type = '����' THEN '�ϰ������' 
                when l.period_type = '����' THEN '�����' end as leave_type,
           l.every_days as leave_days,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                       when l.period_type = '����' THEN '�°������'
                                                                                                       when l.period_type = '����' THEN '�ϰ������' 
                                                                                                       when l.period_type = '����' THEN '�����' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l1
  LEFT JOIN 
  (
    SELECT l.originator_user_id,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                when l.period_type = '����' THEN '�°������'
                when l.period_type = '����' THEN '�ϰ������' 
                when l.period_type = '����' THEN '�����' end as leave_type,
           l.every_days as leave_days,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                       when l.period_type = '����' THEN '�°������'
                                                                                                       when l.period_type = '����' THEN '�ϰ������' 
                                                                                                       when l.period_type = '����' THEN '�����' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
  WHERE l1.rn = 1 
)t3
ON t3.originator_user_id = tud.emp_id AND t3.stat_date = tud.days
-- ����ͳ��
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like 'ȫ��%' THEN 1
              when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%����%' THEN 0.5 end as travel_days
  FROM work_day
  WHERE split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%����'
)t4
ON t4.originator_user_id = tud.emp_id AND t4.stat_date = tud.days
-- �Ӽ�ͳ��
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like 'ȫ��%' THEN 1
              when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%����%' THEN 0.5 end as home_office_days
  FROM work_day
  WHERE split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%�Ӽ�'
)t5
ON t5.originator_user_id = tud.emp_id AND t5.stat_date = tud.days
-- �Ǽǹ�ʱ������
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as demand_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as demand_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = '����' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t6
ON tud.user_email = t6.user_email AND tud.days = t6.stat_date
-- �Ǽǹ�ʱ��ȱ��
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as bug_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as bug_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = 'ȱ��' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t7
ON tud.user_email = t7.user_email AND tud.days = t7.stat_date
-- �Ǽǹ�ʱ������
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as task_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as task_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = '����' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t8
ON tud.user_email = t8.user_email AND tud.days = t8.stat_date
-- �Ǽǹ�ʱ�Ĺ���
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as work_order_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as work_order_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_classify_name = '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t9
ON tud.user_email = t9.user_email AND tud.days = t9.stat_date
-- ������ͳ��
LEFT JOIN 
(
  SELECT to_date(t1.git_commit_date) as stat_date,
         IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) as true_email,
         SUM(IF(t1.add_lines_count >= 2000,2000,t1.add_lines_count)) as code_quantity
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON te.email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) AND te.d = '${pre1_date}' AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾'
  LEFT JOIN ${dim_dbname}.dim_git_auth_user t2
  ON t2.git_user_email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email)
  WHERE t2.ones_user_uuid is not null AND t1.git_repository NOT LIKE '%software/phoenix/aio/phoenix-rcs-aio.git'
  GROUP BY to_date(t1.git_commit_date),IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email)
)t10
ON t10.true_email = tud.user_email AND t10.stat_date = tud.days
-- �ڲ��з����ܹ�ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as internal_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_type_name = '�ڲ��з���Ŀ' 
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t11
ON tud.user_email = t11.user_email AND tud.days = t11.stat_date
-- �ⲿ��Ŀ���ܹ�ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as external_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_type_name = '�ⲿ�ͻ���Ŀ' 
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t12
ON tud.user_email = t12.user_email AND tud.days = t12.stat_date
-- ����&��������ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as mgmt_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_type_name = '����&������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)t13
ON tud.user_email = t13.user_email AND tud.days = t13.stat_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"





echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_member_work_detail_report;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## ��ӿڱ�������� #######----------------------------------------------------------------------------------------------- "


##����Ա��ʱ�ܱ�(excel��)��ϸ�� ads_member_work_detail_report
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_member_work_detail_report \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_member_work_detail_report \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,org_name,team_member,dtk_emp_id,emp_position,work_date,day_type,work_hour,clock_in_hour,leave_days,travel_days,home_office_days,demand_qty,demand_ones_ids,bug_qty,bug_ones_ids,task_qty,task_ones_ids,work_order_qty,work_order_ones_ids,add_lines_count,internal_wh,internal_occ,external_wh,external_occ,mgmt_wh,mgmt_occ,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





