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
--ads_dtk_implementers_attendamce_error    --快仓实施运维人员考勤异常记录信息表

INSERT overwrite table ${ads_dbname}.ads_dtk_implementers_attendamce_error
-- 考勤 项目编码不存在
SELECT '' as id,
       DATE(a.checkin_time) as cur_date, -- 统计时间
       a.business_id, -- 审批编号
       a.old_project_code as project_code, -- 项目编号
       '未知' as project_name, -- 项目名称
       '未知' as project_ft, -- 所属产品线
       '未知' as project_operation_state, -- 项目运营阶段
       a.originator_dept_name as team_name, -- 团队名称
       IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as member_name, -- 成员名称
       IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
       '项目编码不存在' as errpr_type,-- 异常类型
       cast(IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as decimal(10,2)) as check_duration_hour, -- 考勤时长（小时）,
       case when IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) < 4 then 0
            when IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) >= 4 and IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) < 8 then '0.5天'
            when IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) >= 8 and IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5'))<= 10 then '1天'
            when IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) > 10 then CONCAT('1天',(IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) - 10),'小时') END as check_duration_day,
       a.checkin_time, -- 考勤签到时间
       a.checkout_time, -- 考勤签退时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b 
ON b.d = '${pre1_date}' AND IF(a.project_code like 'S-%',SUBSTRING(a.project_code,3) = b.project_code,a.project_code = b.project_code OR a.project_code = b.project_sale_code)
WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' -- 审批状态:已结束,审批结果:已通过
  AND b.project_code is null

UNION ALL 

-- 考勤 考勤时间异常(1天14H以上)
SELECT '' as id,
       tmp.cur_date,
       tmp.business_id,
       tmp.project_code,
       tmp.project_name,
       tmp.project_ft,
       tmp.project_operation_state,
       tmp.team_name,
       tmp.member_name,
       tmp.service_type,
       '考勤时间异常(1天14H以上)' as errpr_type,-- 异常类型
       tmp.check_duration_hour,
       tmp.check_duration_day,
       tmp.checkin_time,
       tmp.checkout_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tt1.cur_date,
         tt1.business_id,
         tt1.project_code,
         tt1.project_name,
         tt1.project_ft,
         tt1.project_operation_state,
         tt1.originator_dept_name as team_name,
         tt1.originator_user_name as member_name,
         tt1.service_type,
         SUM(tt1.check_duration) as check_duration_hour,
         case when SUM(tt1.check_duration) < 4 then 0
              when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
              when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
              when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day,
         tt1.checkin_time, -- 考勤签到时间
         tt1.checkout_time -- 考勤签退时间
  FROM  
  (
    SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
           a.business_id, -- 审批编号
           a.project_code, -- 项目编号
           b.project_name, -- 项目名称
           nvl(b.project_attr_ft,'未知') as project_ft, -- 所属产品线
           b.project_operation_state, -- 项目运营阶段
           a.originator_dept_name, -- 团队名称
           IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
           IF(a.service_type is null,'未知',a.service_type) as service_type, -- 劳务类型
           IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）
           a.checkin_time, -- 考勤签到时间
           a.checkout_time -- 考勤签退时间
    FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
    ON b.d = '${pre1_date}' AND IF(a.project_code like 'S-%',SUBSTRING(a.project_code,3) = b.project_code,a.project_code = b.project_code OR a.project_code = b.project_sale_code)
    WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' -- 审批状态:已结束,审批结果:已通过
      AND b.project_code is not null
  )tt1
  GROUP BY tt1.cur_date,tt1.business_id,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type,tt1.checkin_time,tt1.checkout_time
)tmp
WHERE tmp.check_duration_hour > 24;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"