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
-- ads_pms_process_pe_log_detail    --pms PE��־

INSERT overwrite table ${ads_dbname}.ads_pms_process_pe_log_detail
SELECT '' as id,
       tud.org_name_2, -- ��������
       tud.org_name_3, -- ��������
       tud.emp_name, -- ��Ա����
       tud.emp_position, -- ��Աְλ
       tud.is_job, -- �Ƿ���ְ
       tud.days, -- ����
       IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- ��������
       IF(t1.log_date is not null or t12.leave_type is not null,'�Ѵ�','δ��') as ischeck, -- �Ƿ��
       t1.work_status, -- ����״̬
       t1.business_id, -- �������
       t1.log_date, -- ��־����
       t1.project_code, -- ��Ŀ����
       t1.project_name, -- ��Ŀ����
       t1.project_manage, -- ��Ŀ����
       t1.project_area, -- ����-PM
       t1.project_ft, -- ����/FT => <������������>ft
       t1.project_priority, -- ��Ŀ�ȼ�
       t1.project_progress_stage, -- ��Ŀ�׶�
       t1.job_content, -- ��������
       t1.working_hours, -- ����ʱ��
       t1.unusual_res, -- �쳣ԭ��
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
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
      WHERE 1 = 1
        AND te.d = '${pre1_date}' AND te.org_company_name = '�Ϻ�������ܿƼ����޹�˾' AND te.is_active = 1 AND te.emp_function_role = 'PE'
    )tmp
    WHERE tmp.rn =1
  )tu  
  LEFT JOIN
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-07-01' AND days <= '${pre1_date}'
  )td
  ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,'${pre1_date}',tu.quit_date)
)tud
LEFT JOIN 
(
  SELECT p.applicant_user_id,
         p.work_status, 
         p.job_content,
         p.log_date,
         IF(p.working_hours is null AND p.log_date is not null,0,p.working_hours) as working_hours,
         nvl(pvd.project_code,p.project_code) as project_code,
         p.project_manager as project_manage,
         IF(p.data_source = 'dtk',p.id,p.process_instance_id) as business_id,
         p.applicant_user_name as originator_user_name,
         p.data_source,
         pvd.project_name,
         pvd.project_area,
         pvd.project_ft,
         pvd.project_priority,
         pvd.project_progress_stage,
         case when p.project_code is not null AND pvd.project_code is null then '��Ŀ�����쳣' end as unusual_res
  FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
  ON p.project_code = pvd.project_code OR p.project_code = pvd.project_sale_code
  WHERE p.d = '${pre1_date}' AND p.role_type = 'PE'
)t1
ON t1.applicant_user_id = tud.emp_id AND tud.days = t1.log_date 
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
) t12 
ON t12.originator_user_id = tud.emp_id AND t12.stat_date = tud.days;
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

truncate table ads_pms_process_pe_log_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## ��ӿڱ�������� #######----------------------------------------------------------------------------------------------- "


##��ads_pms_process_pe_log_detail    --pms PE��־
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_process_pe_log_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_process_pe_log_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,org_name_2,org_name_3,emp_name,emp_position,is_job,days,day_type,ischeck,work_status,business_id,log_date,project_code,project_name,project_manage,project_area,project_ft,project_priority,project_progress_stage,job_content,working_hours,unusual_res,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





