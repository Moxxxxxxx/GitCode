#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim
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
-- ads_pms_process_service_log_detail    --pms 外包劳务日志

INSERT overwrite table ${ads_dbname}.ads_pms_process_service_log_detail
SELECT '' as id, --主键 
       tt1.cur_date,
       tt1.business_id,
       nvl(pvd.project_code,tt1.project_code) as project_code,
       pvd.project_name,
       pvd.project_dispaly_state as project_operation_state,
       pvd.project_area,
       pvd.project_ft,
       pvd.project_priority,
       pvd.project_progress_stage,
       tt1.originator_dept_name as team_name,
       tt1.originator_user_name as member_name,
       tt1.service_type,
       '劳务' as member_function, -- 职能【劳务】
       tt1.check_duration,
       case when tt1.check_duration < 4 then '0天'
            when tt1.check_duration >= 4 and tt1.check_duration < 8 then '0.5天'
            when tt1.check_duration >= 8 and tt1.check_duration <= 10 then '1天'
            when tt1.check_duration > 10 then CONCAT('1天',(tt1.check_duration - 10),'小时') END as check_duration_day,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT TO_DATE(a.checkin_time) as cur_date, -- 统计时间
         a.business_id, -- 审批编号
         a.project_code, -- 项目编号
         a.originator_dept_name, -- 团队名称
         IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
         IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
         IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
         a.checkin_time, -- 考勤签到时间
         a.checkout_time, -- 考勤签退时间
         row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
  FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
  WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
)tt1
LEFT JOIN 
(
  SELECT TO_DATE(a.checkin_time) as cur_date, -- 统计时间
         a.business_id, -- 审批编号
         a.project_code, -- 项目编号
         a.originator_dept_name, -- 团队名称
         IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
         IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
         IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
         a.checkin_time, -- 考勤签到时间
         a.checkout_time, -- 考勤签退时间
         row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
  FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
  WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
)tt2
ON nvl(tt1.cur_date,'unknown1') = nvl(tt2.cur_date,'unknown2') AND nvl(tt1.project_code,'unknown1') = nvl(tt2.project_code,'unknown2') AND nvl(tt1.originator_user_name,'unknown1') = nvl(tt2.originator_user_name,'unknown2') AND tt1.rn = tt2.rn + 1
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON nvl(pvd.project_code,'unknown1') = nvl(tt1.project_code,'unknown2') OR nvl(pvd.project_sale_code,'unknown1') = nvl(tt1.project_code,'unknown2')
WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time

UNION ALL

SELECT '' as id,
       TO_DATE(p.log_date) as cur_date,
       p.process_instance_id as business_id,
       nvl(pvd.project_code,p.project_code) as project_code,
       pvd.project_name,
       pvd.project_dispaly_state as project_operation_state,
       pvd.project_area,
       pvd.project_ft,
       pvd.project_priority,
       pvd.project_progress_stage,
       p.org_path_name as team_name,
       p.applicant_user_name as member_name,
       IF(p.role_type = 'IMP','实施劳务','运维劳务') as service_type,
       '劳务' as member_function,
       p.working_hours as check_duration,
       case when p.working_hours < 4 then '0天'
            when p.working_hours >= 4 and p.working_hours < 8 then '0.5天'
            when p.working_hours >= 8 and p.working_hours <= 10 then '1天'
            when p.working_hours > 10 then CONCAT('1天',(p.working_hours - 10),'小时') END as check_duration_day,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON p.project_code = pvd.project_code OR p.project_code = pvd.project_sale_code
WHERE p.d = '${pre1_date}' AND p.role_type IN ('IMP','OPS');
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

truncate table ads_pms_process_service_log_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_pms_process_service_log_detail    --pms 外包劳务日志
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_process_service_log_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_process_service_log_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,business_id,project_code,project_name,project_operation_state,project_area,project_ft,project_priority,project_progress_stage,team_name,member_name,service_type,member_function,check_duration,check_duration_day,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





