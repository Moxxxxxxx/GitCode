#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim
tmp_dbname=tmp

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
--ads_pms_project_profit_detail    --pms��Ŀ�����

-- ��ʱ��ϸ��
with manhour_detail as
(
  SELECT tud.team_ft,
         tud.team_group,
	     tud.team_sub_group,
         tud.user_name as team_member,
         tud.emp_position,
         tud.is_job,
         tud.hired_date,
         tud.quit_date,
         tud.is_need_fill_manhour,
         tud.org_role_type,
         tud.virtual_role_type,
         tud.module_branch,
         tud.virtual_org_name,
         tt.org_name_1 as project_org_name,
         tt.project_classify_name as project_classify_name,
         tt.sprint_classify_name as sprint_classify_name,
         tt.external_project_code,
         tt.external_project_name,
         tt.project_bpm_code,
         tt.project_bpm_name,
         tt.project_type_name,
         cast(tt.stat_date as date) as work_create_date,
         tt.work_id,
         tt.summary as work_summary,
         tt.task_desc as work_desc,
         tt.work_type as work_type,
         tt.work_status,
         cast(tud.days as date) as work_check_date,
         IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) as day_type,
         cast(nvl(tt3.travel_days, 0) as decimal(10, 2)) as travel_days,
         cast(nvl(t2.work_hour, 0) as decimal(10, 2)) as work_hour,
         IF(tt.work_id is null,0,t2.actual_date) as actual_date,
         CASE WHEN tt.project_type_name is null and tt.work_id is not null and t2.actual_date > 7 THEN '��Ч��ʱ&Υ��Ǽ�'
              WHEN (tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����')) and t2.actual_date > 7 THEN '�����쳣&Υ��Ǽ�'
              WHEN tt.project_type_name is null and tt.work_id is not null THEN '��Ч��ʱ'
              WHEN t2.actual_date > 7 THEN 'Υ��Ǽ�'
              WHEN tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����') THEN '�����쳣'
         ELSE '���쳣' END as error_type,
         IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2)))            as work_hour_total,
         nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0) as work_hour_rate,
         CASE WHEN tud.team_ft = '���첿' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 700 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '���첿' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 700 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '���첿' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 700 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'Ӳ���Զ���' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1000 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'Ӳ���Զ���' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1000 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'Ӳ���Զ���' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1000 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'ϵͳ��̨' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'ϵͳ��̨' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'ϵͳ��̨' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '��ʽFT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '��ʽFT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '��ʽFT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '���ܰ���FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '���ܰ���FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '���ܰ���FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '����ְ' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('������','����','ȫ��Ӱ�','������-�����','����-�����') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '����ְ' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°���Ӱ�','�ϰ���Ӱ�') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '����ְ' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '�����' AND tud.day_type not in('��ĩ','�ڼ���'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '�����' AND tud.day_type in('��ĩ','�ڼ���'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('�°������','�ϰ������','�°������-�����','�ϰ������-�����') AND (tud.day_type = '������' or tud.day_type = '����') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '����&������' AND (tt.external_project_code ='δ֪��Ŀ����' or tt.project_bpm_code = 'δ֪��Ŀ����'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
         ELSE 0 END as cost_amount
  FROM 
  (
    SELECT IF(tu.is_job = 0,'����ְ',tu.team_ft) as team_ft,
           tu.team_group,
           tu.team_sub_group,
           tu.emp_id,
           tu.user_name,
           tu.user_email,
           tu.org_role_type,
           tu.virtual_role_type,
           tu.module_branch,
           tu.virtual_org_name,
           tu.is_job,
           tu.is_need_fill_manhour,
           tu.hired_date,
           tu.quit_date,
           tu.emp_position,
           td.days,
           CASE when td.day_type = 0 then '������'
                when td.day_type = 1 then '��ĩ'
                when td.day_type = 2 then '�ڼ���'
                when td.day_type = 3 then '����' end as day_type
    FROM 
    (
      SELECT DISTINCT tg.org_name_2 as team_ft,
                      tg.org_name_3 as team_group,
                      tg.org_name_4 as team_sub_group,
                      te.emp_id,
                      te.emp_name   as user_name,
                      te.email      as user_email,
                      tmp.org_role_type as org_role_type,
                      tt.role_type as virtual_role_type,
                      tt.module_branch,
                      tt.virtual_org_name,
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
                        row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc,m.org_id asc)rn
        FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
        WHERE m.org_company_name = '�Ϻ�������ܿƼ����޹�˾' AND m.is_valid = 1
      )tmp
      ON te.emp_id = tmp.emp_id AND tmp.rn = 1
      LEFT JOIN 
      (
        SELECT i.emp_code,
               i.role_type,
               i.module_branch,
               i.virtual_org_name
        FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i
        WHERE i.is_active = 1 AND i.virtual_org_name = '�����Ŀ'
      )tt
      ON tt.emp_code = te.emp_id
      LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
      ON tg.org_id = tmp.org_id AND tg.org_company_name = '�Ϻ�������ܿƼ����޹�˾'  
      WHERE te.d = '${pre1_date}' AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾'
        AND (tg.org_name_2 IN ('AMR FT','���ܰ���FT','Ӳ���Զ���','��ʽFT','ϵͳ��̨','���첿') OR (tg.org_name_2 is NULL AND te.is_job = 0))
    )tu
    LEFT JOIN 
    (
      SELECT DISTINCT days,
                      day_type
      FROM ${dim_dbname}.dim_day_date
      WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
     )td
     WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') 
  )tud
  -- ��ʱͳ��
  LEFT JOIN 
  (
    SELECT to_date(t.task_start_time) as stat_date,
           t.user_uuid,
		   tou.user_email,
		   round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
    ON t.task_uuid = t1.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
  	  AND t1.status = 1 AND t1.issue_type_cname in ('ȱ��','����','����')
      AND (IF((t1.project_type_name = '�ⲿ�ͻ���Ŀ' AND t1.external_project_code is null),'δ֪��Ŀ����',t1.external_project_code) != 'δ֪��Ŀ����'
	       OR IF((t1.project_type_name = '�ڲ��з���Ŀ' AND t1.project_bpm_code is null),'δ֪��Ŀ����',t1.project_bpm_code) != 'δ֪��Ŀ����'
	       OR t1.project_type_name = '����&������')
    GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
  )t1
  ON t1.user_email = tud.user_email AND t1.stat_date = tud.days
  -- ��ʱ��ϸ
  LEFT JOIN 
  (
    SELECT to_date(t.task_start_time) as stat_date,
           t.task_uuid,
           t.user_uuid,
           t.project_classify_name,
		   tou.user_email,
	  	   DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) as actual_date,
           t.task_spend_hours as work_hour
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou on tou.uuid = t.user_uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
  )t2 
  ON t2.user_email = tud.user_email AND t2.stat_date = tud.days
  -- ones������Ϣ
  LEFT JOIN 
  (
    SELECT to_date(t1.task_create_time) as stat_date,
           t1.uuid,
           t1.\`number\` as work_id,
           t1.summary,
           t1.task_desc,
           t1.project_classify_name,
           t1.sprint_classify_name,
		   t1.issue_type_cname as work_type,
           t1.task_status_cname as work_status,
           t1.org_name_1,
           IF((t1.project_type_name = '�ⲿ�ͻ���Ŀ' AND t1.external_project_code is null) or t1.project_type_name = '����&������','δ֪��Ŀ����',t1.external_project_code) as external_project_code,
           IF((t1.project_type_name = '�ⲿ�ͻ���Ŀ' AND t1.external_project_code is null) or t1.project_type_name = '����&������','δ֪��Ŀ����',IF(t1.external_project_code = 'A00000','�̻���Ŀ',b1.project_name)) as external_project_name,
           IF((t1.project_type_name = '�ڲ��з���Ŀ' AND t1.project_bpm_code is null) or t1.project_type_name = '����&������','δ֪��Ŀ����',t1.project_bpm_code) as project_bpm_code,
           IF((t1.project_type_name = '�ڲ��з���Ŀ' AND t1.project_bpm_code is null) or t1.project_type_name = '����&������','δ֪��Ŀ����',b2.project_name) as project_bpm_name,
           t1.project_type_name
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b1
    ON b1.d = '${pre1_date}' AND IF(t1.external_project_code like 'S-%',SUBSTRING(t1.external_project_code,3) = b1.project_code,(t1.external_project_code = b1.project_code OR t1.external_project_code = b1.project_sale_code))
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b2
    ON b2.d = '${pre1_date}' AND (t1.project_bpm_code = b2.project_code OR t1.project_bpm_code = b2.project_sale_code) AND ((b2.data_source = 'BPM' AND b2.project_type_name IN ('�ڲ���Ŀ','��˾����Ŀ','Ӳ������Ŀ','�������Ŀ') AND b2.project_operation_state NOT IN ('9.��Ŀ��ͣ','10.��Ŀ��ͣ','11.��Ŀȡ��')) OR (b2.data_source = 'PMS' AND b2.is_external_project = 0))
    WHERE t1.status = 1 AND t1.issue_type_cname in ('ȱ��','����','����')
  )tt 
ON tt.uuid = t2.task_uuid 
  -- �������
  LEFT JOIN 
  (
    SELECT l1.originator_user_id,
           l1.stat_date,
           case when l2.leave_type is null THEN l1.leave_type else 'ȫ�����' END as leave_type
    FROM 
    (
      SELECT l.originator_user_id,
             cast(l.leave_date as date) as stat_date,
             CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                  when l.period_type = '����' THEN '�°������'
                  when l.period_type = '����' THEN '�ϰ������' 
                  when l.period_type = '����' THEN '�����' end as leave_type,
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
             row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                         when l.period_type = '����' THEN '�°������'
                                                                                                         when l.period_type = '����' THEN '�ϰ������' 
                                                                                                         when l.period_type = '����' THEN '�����' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
      WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
    )l2
    ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
    WHERE l1.rn = 1 
  ) tt1
  ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
  -- �Ӱ�����
  LEFT JOIN  
  (
    SELECT l1.applicant_userid,
           l1.stat_date,
           case when l2.work_overtime_type is null THEN l1.work_overtime_type else 'ȫ��Ӱ�' END as work_overtime_type
    FROM 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = 'ȫ��' THEN 'ȫ��Ӱ�'
                  when l.period_type = '����' THEN '�°���Ӱ�'
                  when l.period_type = '����' THEN '�ϰ���Ӱ�' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ��Ӱ�'
                                                                                                          when l.period_type = '����' THEN '�°���Ӱ�'
                                                                                                          when l.period_type = '����' THEN '�ϰ���Ӱ�' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
    )l1
    LEFT JOIN 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = 'ȫ��' THEN 'ȫ��Ӱ�'
                  when l.period_type = '����' THEN '�°���Ӱ�'
                  when l.period_type = '����' THEN '�ϰ���Ӱ�' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ��Ӱ�'
                                                                                                          when l.period_type = '����' THEN '�°���Ӱ�'
                                                                                                          when l.period_type = '����' THEN '�ϰ���Ӱ�' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
    )l2
    ON l1.applicant_userid = l2.applicant_userid AND l1.stat_date = l2.stat_date AND l1.work_overtime_type != l2.work_overtime_type
    WHERE l1.rn = 1 
  ) tt2
  ON tt2.applicant_userid = tud.emp_id AND tt2.stat_date = tud.days
  -- ����ʱ��
  LEFT JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           case when t2.travel_type is null THEN t1.travel_type else 'ȫ�����' END as travel_type,
           case when (case when t2.travel_type is null THEN t1.travel_type else 'ȫ�����' END) = 'ȫ�����' THEN 1 
                when (case when t2.travel_type is null THEN t1.travel_type else 'ȫ�����' END) like '%�������' THEN 0.5 
           else 0 end as travel_days    
    FROM 
    (
      SELECT t.originator_user_id,
             cast(t.travel_date as date) as stat_date,
             CASE when t.period_type = 'ȫ��' THEN 'ȫ�����'
                  when t.period_type = '����' THEN '�°������'
                  when t.period_type = '����' THEN '�ϰ������' end as travel_type,
             row_number()over(PARTITION by t.originator_user_id,cast(t.travel_date as date) order by CASE when t.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                          when t.period_type = '����' THEN '�°������'
                                                                                                          when t.period_type = '����' THEN '�ϰ������' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
    )t1
    LEFT JOIN 
    (
      SELECT t.originator_user_id,
             cast(t.travel_date as date) as stat_date,
             CASE when t.period_type = 'ȫ��' THEN 'ȫ�����'
                  when t.period_type = '����' THEN '�°������'
                  when t.period_type = '����' THEN '�ϰ������' end as travel_type,
             row_number()over(PARTITION by t.originator_user_id,cast(t.travel_date as date) order by CASE when t.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                          when t.period_type = '����' THEN '�°������'
                                                                                                          when t.period_type = '����' THEN '�ϰ������' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
    )t2
    ON t1.originator_user_id = t2.originator_user_id AND t1.stat_date = t2.stat_date AND t1.travel_type != t2.travel_type
    WHERE t1.rn = 1 
  ) tt3
  ON tt3.originator_user_id = tud.emp_id AND tt3.stat_date = tud.days
)

INSERT overwrite table ${ads_dbname}.ads_pms_project_profit_detail
SELECT '' as id, -- ����
       t1.project_code, -- ��Ŀ����
       t1.project_sale_code, -- ��ǰ����
       t1.project_name, -- ��Ŀ����
       t1.project_priority, -- ��Ŀ�ȼ�
       t1.project_dispaly_state_group, -- ��Ŀ�׶���
       t1.project_ft, -- ��Ŀ����ft
       t1.project_area, -- ��Ŀ��������ص�
       t1.online_process_approval_time, -- �����·�
       t1.final_inspection_process_approval_time, --�����·�
       t1.post_project_date, -- �����·�
       nvl(t1.project_income,0) as project_income, -- ��Ŀ����
       nvl(t2.agv_num,0) as agv_num, -- agv���� => Ӳ�� => ��Ŀ����
       nvl(t7.agv_cost,0) as agv_cost, -- agv�ɱ� => Ӳ�� => ��Ŀ����
       nvl(t8.bucket_cost,0) as bucket_cost, -- ���ܷ��� => Ӳ�� => ��Ŀ����
       nvl(t9.charging_cost,0) as charging_cost, -- ���׮���� => Ӳ�� => ��Ŀ����
       nvl(t13.project_other_matters_cost,0) as project_other_matters_cost, -- ��Ŀ�������� => Ӳ�� => ��Ŀ����
       nvl(t10.export_packing_cost,0) as export_packing_cost, -- ���ڰ�װ�� => ��Ŀ����
       nvl(t11.transportation_cost,0) as transportation_cost, -- ����� => ��Ŀ����
       nvl(t12.ectocyst_software_cost,0) as ectocyst_software_cost, -- ���������� => ��Ŀ����
       nvl(t15.ectocyst_hardware_cost,0) as ectocyst_hardware_cost, -- ���Ӳ������ => ��Ŀ����
       nvl(t3.pe_cost,0) as pe_cost, --PE => �˹����� => ��Ŀ����
       nvl(t18.mt_service_cost,0) as mt_service_cost, -- ά�� => �˹����� => ��Ŀ����
       nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) as io_service_cost, -- ���ʵʩ���� => �˹����� => ��Ŀ����
       nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) as op_service_cost, -- �����ά���� => �˹����� => ��Ŀ����
       nvl(t19.te_cost,0) as te_cost, -- �з� => �˹����� => ��Ŀ����
       nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as ctrip_amount, -- Я������ => ���÷� => ��Ŀ����
       nvl(t6.reimburse_amount,0) as reimburse_amount, -- ���˱��� => ���÷� => ��Ŀ����
       nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as cost_sum, -- �ɱ����úϼ�
       nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0)) as project_gp, -- ��Ŀë��
       nvl((nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0))) / nvl(t1.project_income,0),0) as project_gp_rate, -- ��Ŀë����
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
-- ��Ŀ������Ϣ
(
  SELECT b.project_code,
         b.project_sale_code,
         b.project_name,
         b.project_priority,
         b.project_dispaly_state_group,
         b.project_ft,
         b.project_area,
         b.online_process_month as online_process_approval_time,
         b.final_inspection_process_month as final_inspection_process_approval_time,
         date_format(b.post_project_date,'yyyy-MM') as post_project_date,
         b.amount as project_income
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
)t1
-- AGV����
LEFT JOIN
(
  SELECT b.project_code,
         SUM(nvl(tmp.actual_sale_num,0)) as agv_num
  FROM 
  (
    SELECT so.project_code,
           nvl(so.real_qty,0) - nvl(sr.real_qty,0) as actual_sale_num
    FROM 
    -- ����
    (
      SELECT so.project_code,
             SUM(nvl(so.real_qty,0)) as real_qty -- ��������
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- ��������Ϊagv
        AND m.document_status = 'C' -- ����״̬�����
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- �˻�
    (
      SELECT sr.project_code,
             SUM(nvl(sr.real_qty,0)) as real_qty -- �˻�����
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- ��������Ϊagv
        AND m.document_status = 'C' -- ����״̬�����
        AND sr.project_code is not NULL
      GROUP BY sr.project_code
   )sr
   ON so.project_code = sr.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t2
ON t1.project_code = t2.project_code
-- PE����
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.pe_cost) as pe_cost
  FROM
  (
    SELECT tt.project_code,
           SUM(case when tt.day_type = '������' or tt.day_type = '�ڼ���' or tt.day_type = '����' or tt.day_type = '��ĩ' then 1
                    when tt.day_type = '�ϰ������' or tt.day_type = '�°������' then 0.5
                    when tt.day_type = 'ȫ�����' then 0 end) as pe_day,
           SUM(case when (tt.day_type = '������' or tt.day_type = '�ڼ���' or tt.day_type = '����' or tt.day_type = '��ĩ') AND tt.org_name_2 = '��ʽFT' then 1*700
                    when (tt.day_type = '������' or tt.day_type = '�ڼ���' or tt.day_type = '����' or tt.day_type = '��ĩ') AND tt.org_name_2 = 'Ӫ������' AND (tt.org_name_3 = '��������' or tt.org_name_3 = '���д���' or tt.org_name_3 = '��������' or tt.org_name_3 = '���ϴ���' or  tt.org_name_3 = '���ϴ���' or  tt.org_name_3 = '�����ӹ�˾') then 1*700
                    when (tt.day_type = '������' or tt.day_type = '�ڼ���' or tt.day_type = '����' or tt.day_type = '��ĩ') AND tt.org_name_2 = '��Ŀ��' then 1*500
                    when (tt.day_type = '������' or tt.day_type = '�ڼ���' or tt.day_type = '����' or tt.day_type = '��ĩ') AND tt.org_name_2 = '������ҵ��' then 1*1500
                    when (tt.day_type = '�ϰ������' or tt.day_type = '�°������') AND tt.org_name_2 = '��ʽFT' then 0.5*700
                    when (tt.day_type = '�ϰ������' or tt.day_type = '�°������') AND tt.org_name_2 = 'Ӫ������' AND (tt.org_name_3 = '��������' or tt.org_name_3 = '���д���' or tt.org_name_3 = '��������' or tt.org_name_3 = '���ϴ���' or  tt.org_name_3 = '���ϴ���' or  tt.org_name_3 = '�����ӹ�˾') then 0.5*700
                    when (tt.day_type = '�ϰ������' or tt.day_type = '�°������') AND tt.org_name_2 = '��Ŀ��' then 0.5*500
                    when (tt.day_type = '�ϰ������' or tt.day_type = '�°������') AND tt.org_name_2 = '������ҵ��' then 0.5*1500
                    when tt.day_type = 'ȫ�����' then 0 end) as pe_cost
    FROM 
    (
      SELECT tud.org_name_2,
             tud.org_name_3,
             IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- ��������
             t1.log_date, -- ��־����
             t1.project_code -- ��Ŀ����
      FROM 
      (
        SELECT tu.org_name_2,
               tu.org_name_3,
               tu.emp_id,
               tu.emp_name,
               tu.emp_position,
               tu.is_job,
               tu.hired_date,
               tu.quit_date,
               td.days,
               CASE when td.day_type = 0 then '������'
                    when td.day_type = 1 then '��ĩ'
                    when td.day_type = 2 then '�ڼ���'
                    when td.day_type = 3 then '����' end as day_type   
        FROM
        (
          SELECT tmp.org_name_2,
                 tmp.org_name_3,
                 tmp.emp_id,
                 tmp.emp_name,
                 tmp.emp_position,
                 tmp.is_job,
                 tmp.hired_date,
                 tmp.quit_date
          FROM
          (
            SELECT DISTINCT split(tg.org_path_name,'/')[1] as org_name_2,
                            split(tg.org_path_name,'/')[2] as org_name_3,
                            te.emp_id,
                            te.emp_name,
                            te.emp_position,
                            te.prg_path_name,
                            te.is_job,
                            date(te.hired_date) as hired_date,
                            date(te.quit_date) as quit_date,
                            row_number()over(PARTITION by te.emp_id order by split(tg.org_path_name,'/')[1] asc,split(tg.org_path_name,'/')[2] asc)rn
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
              WHERE m.org_company_name = '�Ϻ�������ܿƼ����޹�˾' AND m.d = '${pre1_date}' AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
            )tmp
            ON te.emp_id = tmp.emp_id
            LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
            ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01','${pre1_date}',IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
            WHERE te.d = '${pre1_date}' AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾' AND te.is_active = 1 AND te.emp_function_role = 'PE'
          )tmp
          WHERE tmp.rn =1
        )tu  
        LEFT JOIN
        (
          SELECT DISTINCT days,
                          day_type
          FROM ${dim_dbname}.dim_day_date
          WHERE days >= '2021-07-01' AND days <= '${pre1_date}'
        )td
        ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,'${pre1_date}',tu.quit_date)
      )tud
      LEFT JOIN 
      (
        SELECT p.log_date, -- ��־����
               p.project_code, -- ��Ŀ����
               p.applicant_user_id -- ��Ա����
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.role_type = 'PE'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )t1
      ON t1.applicant_user_id = tud.emp_id AND tud.days = t1.log_date 
      LEFT JOIN 
      (
        SELECT l1.originator_user_id as emp_id,
               l1.stat_date,
               case when l2.leave_type is null THEN l1.leave_type else 'ȫ�����' END as leave_type
        FROM 
        (
          SELECT l.originator_user_id,
                 cast(l.leave_date as date) as stat_date,
                 CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                      when l.period_type = '����' THEN '�°������'
                      when l.period_type = '����' THEN '�ϰ������' 
                      when l.period_type = '����' THEN '�����' end as leave_type,
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
                 row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = 'ȫ��' THEN 'ȫ�����'
                                                                                                             when l.period_type = '����' THEN '�°������'
                                                                                                             when l.period_type = '����' THEN '�ϰ������' 
                                                                                                             when l.period_type = '����' THEN '�����' end asc)rn
          FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
          WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
        )l2
        ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
        WHERE l1.rn = 1 
      )t12 
      ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
    )tt
    WHERE tt.project_code is not NULL 
    GROUP BY tt.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t3
ON t1.project_code = t3.project_code
-- �������-��ά���� 2022��֮�������
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.service_cost) as op_service_cost
  FROM
  (
    SELECT tt.project_code,
       	   SUM(tt.service_cost) as service_cost
   	FROM 
    (
      SELECT tt1.cur_date,
             tt1.project_code,
           	 tt1.originator_user_name as member_name,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT DATE(a.checkin_time) as cur_date, -- ͳ��ʱ��
               a.business_id, -- �������
               a.project_code, -- ��Ŀ���
               a.originator_dept_name, -- �Ŷ�����
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) as originator_user_name, -- ��Ա����
               '��ά����' as service_type, -- ��������
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- ����ʱ����Сʱ��,
               a.checkin_time, -- ����ǩ��ʱ��
               a.checkout_time, -- ����ǩ��ʱ��
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) order by a.checkin_time)rn
       FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
       WHERE (a.service_type = '��ά�����������' OR a.service_type is null) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- �޳�S��ͷ��Ŀ��ŵ��ۺ�����,����״̬:�ѽ���,�������:��ͨ��,��Ŀ��<��Чƥ�伴1>��Ϊ׼,ֻȡ2022��֮�������
      )tt1
      LEFT JOIN 
      (
      	SELECT DATE(a.checkin_time) as cur_date, -- ͳ��ʱ��
               a.business_id, -- �������
               a.project_code, -- ��Ŀ���
               a.originator_dept_name, -- �Ŷ�����
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) as originator_user_name, -- ��Ա����
               '��ά����' as service_type, -- ��������
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- ����ʱ����Сʱ��,
               a.checkin_time, -- ����ǩ��ʱ��
               a.checkout_time, -- ����ǩ��ʱ��
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) order by a.checkin_time)rn
      	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE (a.service_type = '��ά�����������' OR a.service_type is null) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- �޳�S��ͷ��Ŀ��ŵ��ۺ�����,����״̬:�ѽ���,�������:��ͨ��,��Ŀ��<��Чƥ�伴1>��Ϊ׼��ֻȡ2022��֮�������
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
    
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) as log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) as check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'OPS'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
    )tt
    GROUP BY tt.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t4
ON t1.project_code = t4.project_code
-- �������-ʵʩ���� 2022��֮�������
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.service_cost) as io_service_cost
  FROM
  (
    SELECT tt.project_code,
           SUM(tt.service_cost) as service_cost
    FROM 
    (
      SELECT tt1.cur_date,
             tt1.project_code,
           	 tt1.originator_user_name as member_name,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT DATE(a.checkin_time) as cur_date, -- ͳ��ʱ��
               a.business_id, -- �������
               a.project_code, -- ��Ŀ���
               a.originator_dept_name, -- �Ŷ�����
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) as originator_user_name, -- ��Ա����
               'ʵʩ����' as service_type, -- ��������
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- ����ʱ����Сʱ��,
               a.checkin_time, -- ����ǩ��ʱ��
               a.checkout_time, -- ����ǩ��ʱ��
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) order by a.checkin_time)rn
        FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = 'ʵʩ����' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- �޳�S��ͷ��Ŀ��ŵ��ۺ�����,����״̬:�ѽ���,�������:��ͨ��,��Ŀ��<��Чƥ�伴1>��Ϊ׼��ֻȡ2022��֮�������
      )tt1
      LEFT JOIN 
      (
     	SELECT DATE(a.checkin_time) as cur_date, -- ͳ��ʱ��
               a.business_id, -- �������
               a.project_code, -- ��Ŀ���
               a.originator_dept_name, -- �Ŷ�����
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) as originator_user_name, -- ��Ա����
               'ʵʩ����' as service_type, -- ��������
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- ����ʱ����Сʱ��,
               a.checkin_time, -- ����ǩ��ʱ��
               a.checkout_time, -- ����ǩ��ʱ��
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'��',1),a.originator_user_name) order by a.checkin_time)rn
     	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = 'ʵʩ����' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- �޳�S��ͷ��Ŀ��ŵ��ۺ�����,����״̬:�ѽ���,�������:��ͨ��,��Ŀ��<��Чƥ�伴1>��Ϊ׼��ֻȡ2022��֮�������
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
    
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) as log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) as check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'IMP'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
    )tt
    GROUP BY tt.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t5
ON t1.project_code = t5.project_code
-- ���˱���
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.reimburse_amount) as reimburse_amount
  FROM
  -- ���˱���
  (
    SELECT p.project_code,
           SUM(nvl(p.reimburse_amount,0)) as reimburse_amount -- �������
    FROM
    (
      SELECT p.flow_id,
             i.project_code,
             i.total_amount as reimburse_amount -- �������
      FROM ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful p
      LEFT JOIN ${dwd_dbname}.dwd_bpm_personal_expense_account_item_info_ful i
      ON p.flow_id = i.flow_id
      WHERE p.approve_status = 30 AND p.project_code is not NULL AND p.currency_code = 'PRE001' -- ȡ��������Ϊ����ҵ�
    )p
    GROUP BY p.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t6
ON t1.project_code = t6.project_code
-- agv����
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as agv_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    -- ����
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc -- ���ռ۸�
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- ��������Ϊagv
        AND m.document_status = 'C' -- ����״̬�����
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- �˻�
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc -- ���ռ۸�
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- ��������Ϊagv
        AND m.document_status = 'C' -- ����״̬�����
        AND sr.project_code is not NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t7
ON t1.project_code = t7.project_code
-- ���ܷ���
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as bucket_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT04%' -- ����
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT04%' -- ����
        AND sr.project_code is not NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t8
ON t1.project_code = t8.project_code
-- ���׮����
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as charging_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT03%' -- ���׮
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- ��������
               SUM(b.end_period_number) as end_period_number, -- ��ĩ����
               SUM(b.end_period_amount) as end_period_amount, -- ��ĩ���
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- ���ϵ����ɱ���
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT03%' -- ���׮
        AND sr.project_code is not NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t9
ON t1.project_code = t9.project_code
-- ���ڰ�װ��
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as export_packing_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE g.materia_number ='P' -- ��װ
        AND m.document_status = 'C' -- ����״̬�����
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE g.materia_number ='P' -- ��װ
        AND m.document_status = 'C' -- ����״̬�����
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t10
ON t1.project_code = t10.project_code
-- �����
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as transportation_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- ���������ѡ�����������
        AND m.document_status = 'C' -- ����״̬�����
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- ���������ѡ�����������
        AND m.document_status = 'C' -- ����״̬�����
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t11
ON t1.project_code = t11.project_code
-- ������
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ectocyst_software_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE m.material_number in ('S99000046K010','S99L04660K010') -- ������
        AND m.document_status = 'C' -- ����״̬�����
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE m.material_number in ('S99000046K010','S99L04660K010') -- ������
        AND m.document_status = 'C' -- ����״̬�����
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t12
ON t1.project_code = t12.project_code
-- �������Ϸ���
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as project_other_matters_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(nvl(so.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox != 1 -- �������Բ�Ϊagv 
        AND m.material_number not like 'RT04%' -- �ų�����
        AND m.material_number not like 'RT03%' -- �ų����׮
        AND m.material_group != '111370' -- �ų����Ϸ���ΪP����װ��
        AND m.material_group != '111373' -- �ų����Ϸ���ΪS�������
        AND m.material_number not like 'R5S%'
        AND m.material_number not like 'R6S%'
        AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- ��������
        AND m.document_status = 'C' -- ����״̬�����
        AND so.project_code is not NULL 
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(nvl(sr.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox != 1 -- �������Բ�Ϊagv 
        AND m.material_number not like 'RT04%' -- �ų�����
        AND m.material_number not like 'RT03%' -- �ų����׮
        AND m.material_group != '111370' -- �ų����Ϸ���ΪP����װ��
        AND m.material_group != '111373' -- �ų����Ϸ���ΪS�������
        AND m.material_number not like 'R5S%'
        AND m.material_number not like 'R6S%'
        AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- ��������
        AND m.document_status = 'C' -- ����״̬�����
        AND sr.project_code is not NULL 
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t13
ON t1.project_code = t13.project_code
-- ���Ӳ��
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ectocyst_hardware_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518')
        AND m.document_status = 'C' -- ����״̬�����
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518' )
        AND m.document_status = 'C' -- ����״̬�����
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t15
ON t1.project_code = t15.project_code
-- �������-ʵʩ���� 2022��֮ǰ������
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as io_service_cost_ago
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
      ON po.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- ������������
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- ������������������
        AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- ��ά����
        AND m.document_status = 'C' -- ����״̬�����
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL 
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
      ON pm.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- ������������
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- ������������������
        AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- ��ά����
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL 
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t16
ON t1.project_code = t16.project_code
-- �������-��ά���� 2022��֮ǰ������
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as op_service_cost_ago
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
      ON po.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- ������������
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- ������������������
        AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- ��ά����
        AND m.document_status = 'C' -- ����״̬�����
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
      ON pm.id = p.id AND p.d = '${pre1_date}' 
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- ������������
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- ������������������
        AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- ��ά����
        AND m.document_status = 'C' -- ����״̬�����
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t17
ON t1.project_code = t17.project_code
-- ά������
LEFT JOIN
( 
  SELECT b.project_code,
         SUM(tmp.actual_cost) as mt_service_cost
  FROM
  (
    SELECT m.project_code,
           SUM(nvl(m.actual_cost,0)) as actual_cost
    FROM
    (
      SELECT m.log_date, -- ��־����
             m.originator_user_name, -- ������
             m.project_code, -- ��Ŀ����
             SUM(IF(m.working_hours is null,0,m.working_hours)) as working_hours, -- ����ʱ��
             SUM(tmp.working_hours_total) as working_hours_total, -- ������ʱ��
             case when m.log_date < '2022-06-01' then SUM(IF(m.working_hours is null,0,m.working_hours)) * 0 -- 2022.6.1֮ǰ�����ݷ���Ϊ0
                  when m.log_date >= '2022-06-01' and SUM(tmp.working_hours_total) <= 8 then SUM(IF(m.working_hours is null,0,m.working_hours)) * 60 -- 2022.6.1֮������� ��ʱ��С�ڵ���8Сʱ Сʱ����60*����ʱ��
                  when m.log_date >= '2022-06-01' and SUM(tmp.working_hours_total) > 8 then 480 / SUM(tmp.working_hours_total) * SUM(IF(m.working_hours is null,0,m.working_hours)) -- 2022.6.1֮������� ��ʱ������8Сʱ ����/��ʱ��*����ʱ��
             end as actual_cost -- ά������
      FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
      LEFT JOIN 
      (
        SELECT m.originator_user_name,
               m.log_date,
               SUM(IF(m.working_hours is null,0,m.working_hours)) as working_hours_total
        FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
        WHERE m.org_name = '����' AND m.project_code is not NULL AND m.d = '${pre1_date}' AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
        GROUP BY m.log_date,m.originator_user_name
      )tmp
      ON tmp.originator_user_name = m.originator_user_name AND tmp.log_date = m.log_date
      WHERE m.org_name = '����' AND m.project_code is not NULL AND m.d = '${pre1_date}' AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
      GROUP BY m.log_date,m.originator_user_name,m.project_code
    )m
    GROUP BY m.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t18
ON t1.project_code = t18.project_code
-- �з�����
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as te_cost
  FROM
  (
    SELECT md.external_project_code as project_code,
           SUM(md.cost_amount) as actual_cost
    FROM manhour_detail md
    WHERE md.cost_amount != 0 AND md.project_type_name IN ('�ⲿ�ͻ���Ŀ')
    GROUP BY md.external_project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t19
ON t1.project_code = t19.project_code
-- Я���ó�����
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_car_cost
  FROM
  (
    SELECT c.project_code,
           SUM(IF(c.real_amount_haspost is null,0,c.real_amount_haspost)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_car_account_check_info_di c
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON c.d = td.days
    WHERE c.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY c.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t20
ON t1.project_code = t20.project_code
-- Я�̻�Ʊ����
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_flight_cost
  FROM
  (
    SELECT f.project_code,
           SUM(IF(f.real_amount is null,0,f.real_amount)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di f
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON f.d = td.days
    WHERE f.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY f.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t21
ON t1.project_code = t21.project_code
-- Я�̾Ƶ����
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_hotel_cost
  FROM
  (
    SELECT h.project_code,
           SUM(IF(h.amount is null,0,h.amount)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di h
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON h.d = td.days
    WHERE h.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY h.project_code
  )tmp
  -- �ϲ�FH��A��Ŀ����
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t22
ON t1.project_code = t22.project_code;
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

truncate table ads_pms_project_profit_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## ��ӿڱ�������� #######----------------------------------------------------------------------------------------------- "


##��ads_pms_project_profit_detail    --pms��Ŀ�����
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_project_profit_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_project_profit_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code,project_sale_code,project_name,project_priority,project_dispaly_state_group,project_ft,project_area,online_process_approval_time,final_inspection_process_approval_time,post_project_date,project_income,agv_num,agv_cost,bucket_cost,charging_cost,project_other_matters_cost,export_packing_cost,transportation_cost,ectocyst_software_cost,ectocyst_hardware_cost,pe_cost,mt_service_cost,io_service_cost,op_service_cost,te_cost,ctrip_amount,reimburse_amount,cost_sum,project_gp,project_gp_rate,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





