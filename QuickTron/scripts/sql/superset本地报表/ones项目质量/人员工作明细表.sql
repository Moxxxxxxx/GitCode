#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads




    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- ��Ա������ϸ�� ads_member_work_detail 

INSERT overwrite table ${ads_dbname}.ads_member_work_detail
SELECT '' as id, -- ����
       main.emp_id as dtk_user_id, -- �����û�id
       main.emp_name as team_member, -- ��Ա����
       main.team_group as org_path, -- ��֯�ܹ�
       main.emp_position as emp_position, -- ��Աְλ
       main.days as work_date, -- ����
       IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) as ones_work_hour, -- �Ǽǹ�ʱ��
       IF(ciwh.clock_in_work_hour IS NULL,0,ciwh.clock_in_work_hour) as clock_in_work_hour, -- �򿨹�ʱ��
       IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) - IF(ciwh.clock_in_work_hour IS NULL,0,ciwh.clock_in_work_hour) as workhour_deviation, -- ƫ�ʱ��
       IF(IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) - IF(ciwh.clock_in_work_hour IS NULL,0,ciwh.clock_in_work_hour) > 0,1,0) as is_deviation, -- �Ƿ�ƫ��
       IF(ib.business_days IS NULL,0,ib.business_days) as is_business, -- �Ƿ����
       IF(il.leave_days IS NULL,0,il.leave_days) as is_leave, -- �Ƿ����
       IF(ho.checkin_date IS NULL,0,1) as is_home_office, -- �Ƿ�ӼҰ칫
       ho.home_office_location, -- �ӼҰ칫�ص�
       doi.demand_ones_id, -- �Ǽǹ�ʱ������id
       toi.task_ones_id, -- �Ǽǹ�ʱ������id
       boi.bug_ones_id, -- �Ǽǹ�ʱ��ȱ��id
       woi.work_order_ones_id, -- �Ǽǹ�ʱ�Ĺ���id
       IF(mwh.mgmt_work_hour IS NULL,0,mwh.mgmt_work_hour) as mgmt_work_hour, -- ����&��������ʱ
       IF(iwh.ineffective_work_hour IS NULL,0,iwh.ineffective_work_hour) as ineffective_work_hour, -- ��Ч��ʱ
       CASE WHEN IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) < 6 THEN 'δ����'
            WHEN IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) >= 6 AND IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) <= 10 THEN '����'
            WHEN IF(owh.ones_work_hour IS NULL,0,owh.ones_work_hour) > 10 THEN '������' END as saturation, -- ���Ͷ�
       IF(cewh.code_error_work_hour IS NULL,0,cewh.code_error_work_hour) as code_error_work_hour, -- �ⲿ�ͻ���Ŀ�����쳣��ʱ
       IF(owh.wh_check_times IS NULL,0,owh.wh_check_times) as wh_check_times, -- ��ʱ�ǼǴ���
       IF(owh.unusual_wh_check_times IS NULL,0,owh.unusual_wh_check_times) as unusual_wh_check_times, -- ���εǼǴ���6Сʱ����
       IF(ipwh.internal_project_wh IS NULL,0,ipwh.internal_project_wh) as internal_project_wh, -- �ڲ��з����ܹ�ʱ
       ipwh.internal_project_summary, -- �ڲ���Ŀ����Ŀ�ۺ���ϸ
       IF(epwh.external_project_wh IS NULL,0,epwh.external_project_wh) as external_project_wh, -- �ⲿ��Ŀ���ܹ�ʱ
       epwh.external_project_summary, -- �ⲿ��Ŀ����Ŀ�ۺ���ϸ
       odd.ones_demand_detail, -- ones����������ϸ
       otd.ones_task_detail, -- ones����������ϸ
       obd.ones_bug_detail, -- onesȱ�ݹ�������ϸ
       owd.ones_work_detail, -- ones������������ϸ
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT dtk.*,
         td.*
  -- ��Ա������
  FROM
  (
    SELECT te.emp_id,
           te.emp_name,
           te.email,
           te.emp_position,
           te.is_job,
           te.hired_date,
           te.quit_date,
           m.team_group
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT m.emp_id,
             m.emp_name,
             m.org_start_date,
             m.org_end_date,
             CONCAT(m.dept_name,IF(m.team_org_name_map['team1'] is null,'','-'),IF(m.team_org_name_map['team1'] is null,'',m.team_org_name_map['team1'])) as team_group,
             row_number()over(PARTITION by m.emp_id order by m.org_end_date desc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.is_need_fill_manhour = 1 AND (m.org_end_date = '9999-01-01' OR m.org_end_date <= DATE_ADD(CURRENT_DATE(), -1))
    )m
    ON te.emp_id = m.emp_id AND m.rn = 1 
    WHERE te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾' 
      AND (te.is_job = 1 OR te.quit_date <= DATE_ADD(CURRENT_DATE(), -1))
      AND m.emp_id is not NULL 
  )dtk
  -- ����
  LEFT JOIN 
  (
    SELECT days
	FROM ${dim_dbname}.dim_day_date
	WHERE days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  )td
  ON td.days >= dtk.hired_date AND td.days <= IF(dtk.is_job = 1,DATE_ADD(CURRENT_DATE(), -1),dtk.quit_date)
)main
-- �Ǽǹ�ʱ������ʱ�ǼǴ���
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as ones_work_hour,
         COUNT(DISTINCT t.uuid) as wh_check_times,
         SUM(IF(t.task_spend_hours > 6,1,0)) as unusual_wh_check_times 
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)owh
ON main.email = owh.user_email AND main.days = owh.stat_date
-- �򿨹�ʱ��
LEFT JOIN 
(
  SELECT e.emp_id,
         e.emp_name,
         e.attendance_work_date,
         e.attendance_working_time,
         e.attendance_off_time,
         cast((unix_timestamp(e.attendance_off_time) - unix_timestamp(e.attendance_working_time))/3600 as decimal(10,1)) as clock_in_work_hour
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di e
)ciwh
ON main.emp_id = ciwh.emp_id AND main.days = ciwh.attendance_work_date
-- �Ƿ����
LEFT JOIN 
(
  SELECT t.originator_user_id,
         cast(t.travel_date as date) as stat_date,
         t.every_days as business_days
  FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
  WHERE t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED'
)ib
ON main.emp_id = ib.originator_user_id AND main.days = ib.stat_date
-- �Ƿ����
LEFT JOIN 
(
  SELECT l.originator_user_id,
         cast(l.leave_date as date) as stat_date,
         l.every_days as leave_days
  FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
  WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) AND l.period_type != '����'
)il
ON main.emp_id = il.originator_user_id AND main.days = il.stat_date
-- �Ƿ�ӼҰ칫���ӼҰ칫�ص�
LEFT JOIN 
(
  SELECT c.emp_id,
         c.emp_name,
         c.checkin_date,
         c.first_checkin_detail_place,
         c.last_checkin_detail_place,
         IF(c.first_checkin_detail_place = c.last_checkin_detail_place,c.first_checkin_detail_place,IF(c.last_checkin_detail_place is null,c.first_checkin_detail_place,CONCAT(c.first_checkin_detail_place,',', c.last_checkin_detail_place))) as home_office_location
  FROM ${dwd_dbname}.dwd_dtk_group_day_checkin_info_di c
)ho
ON main.emp_id = ho.emp_id AND main.days = ho.checkin_date
-- �Ǽǹ�ʱ������
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as demand_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = '����' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)doi
ON main.email = doi.user_email AND main.days = doi.stat_date
-- �Ǽǹ�ʱ������
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as task_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = '����' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)toi
ON main.email = toi.user_email AND main.days = toi.stat_date
-- �Ǽǹ�ʱ��ȱ��
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as bug_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND i.issue_type_cname = 'ȱ��' AND t.project_classify_name != '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)boi
ON main.email = boi.user_email AND main.days = boi.stat_date
-- �Ǽǹ�ʱ�Ĺ���
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as work_order_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_classify_name = '�����������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)woi
ON main.email = woi.user_email AND main.days = woi.stat_date
-- ����&��������ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as mgmt_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
    AND t.project_type_name = '����&������'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)mwh
ON main.email = mwh.user_email AND main.days = mwh.stat_date
-- ��Ч��ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as ineffective_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is null -- ��Ч��ʱ
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
  GROUP BY to_date(t.task_start_time),tou.user_email
)iwh
ON main.email = iwh.user_email AND main.days = iwh.stat_date
-- �ⲿ�ͻ���Ŀ�����쳣��ʱ
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as code_error_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name = '�ⲿ�ͻ���Ŀ' -- ֻ�����ⲿ�ͻ���Ŀ
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    AND i.external_project_code IS NULL 
  GROUP BY to_date(t.task_start_time),tou.user_email
)cewh
ON main.email = cewh.user_email AND main.days = cewh.stat_date
-- �ڲ��з����ܹ�ʱ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         SUM(tmp.internal_project_wh) as internal_project_wh,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.internal_project_summary,'\$\$\$',tmp.internal_project_wh) as string))) as internal_project_summary
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as internal_project_wh,
           CONCAT(IF(i.project_bpm_code is null,'δ֪��Ŀ����',i.project_bpm_code),'-',IF(b.project_name is null,'δ֪��Ŀ����',b.project_name)) as internal_project_summary
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
    ON IF(i.project_bpm_code is null,'δ֪��Ŀ����',i.project_bpm_code) = b.project_code AND b.project_type IN ('�ڲ���Ŀ','��˾����Ŀ','Ӳ������Ŀ','�������Ŀ') AND b.project_status NOT IN ('9.��Ŀ��ͣ','10.��Ŀ��ͣ','11.��Ŀȡ��')
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND t.project_type_name = '�ڲ��з���Ŀ' 
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,CONCAT(IF(i.project_bpm_code is null,'δ֪��Ŀ����',i.project_bpm_code),'-',IF(b.project_name is null,'δ֪��Ŀ����',b.project_name))
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)ipwh
ON main.email = ipwh.user_email AND main.days = ipwh.stat_date
-- �ⲿ��Ŀ���ܹ�ʱ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         SUM(tmp.external_project_wh) as external_project_wh,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.external_project_summary,'\$\$\$',tmp.external_project_wh) as string))) as external_project_summary
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as external_project_wh,
           CONCAT(IF(i.external_project_code is null,'δ֪��Ŀ����',i.external_project_code),'-',IF(b.project_name is null,'δ֪��Ŀ����',b.project_name)) as external_project_summary
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
    ON IF(i.external_project_code like 'S-%',SUBSTRING(i.external_project_code,3) = b.project_code,IF(i.external_project_code is null,'δ֪��Ŀ����',i.external_project_code) = b.project_code) AND b.d = DATE_ADD(CURRENT_DATE(), -1)
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND t.project_type_name = '�ⲿ�ͻ���Ŀ' 
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,CONCAT(IF(i.external_project_code is null,'δ֪��Ŀ����',i.external_project_code),'-',IF(b.project_name is null,'δ֪��Ŀ����',b.project_name))
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)epwh
ON main.email = epwh.user_email AND main.days = epwh.stat_date
-- ones����������ϸ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_demand_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND i.issue_type_cname = '����' AND i.project_classify_name != '�����������'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)odd
ON main.email = odd.user_email AND main.days = odd.stat_date
-- ones����������ϸ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_task_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND i.issue_type_cname = '����' AND i.project_classify_name != '�����������'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)otd
ON main.email = otd.user_email AND main.days = otd.stat_date
-- onesȱ�ݹ�������ϸ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_bug_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND i.issue_type_cname = 'ȱ��' AND I.project_classify_name != '�����������'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)obd
ON main.email = obd.user_email AND main.days = obd.stat_date
-- ones������������ϸ
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_work_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- �޳���Ч��ʱ
      AND i.project_classify_name = '�����������'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- �޳�Υ��Ǽ�
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)owd
ON main.email = owd.user_email AND main.days = owd.stat_date;
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

truncate table ads_member_work_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## ��ӿڱ�������� #######----------------------------------------------------------------------------------------------- "


##����Ա������ϸ�� ads_member_work_detail
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_member_work_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_member_work_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,dtk_user_id,team_member,org_path,emp_position,work_date,ones_work_hour,clock_in_work_hour,workhour_deviation,is_deviation,is_business,is_leave,is_home_office,home_office_location,demand_ones_id,task_ones_id,bug_ones_id,work_order_ones_id,mgmt_work_hour,ineffective_work_hour,saturation,code_error_work_hour,wh_check_times,unusual_wh_check_times,internal_project_wh,internal_project_summary,external_project_wh,external_project_summary,ones_demand_detail,ones_task_detail,ones_bug_detail,ones_work_detail,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





