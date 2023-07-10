#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-14 创建
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
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
--制造部人员工作效能表 tmp_team_ft_engineer_member_work_efficiency

INSERT overwrite table ${tmp_dbname}.tmp_team_ft_engineer_member_work_efficiency
SELECT '' AS id,
       tud.team_ft, -- 一级部门
       tud.team_group, -- 二级部门
	   tud.team_sub_group, -- 三级部门
       tud.team_last_group, -- 四级部门
	   tud.emp_position, -- 职位
       tud.user_name AS team_member, -- 人员姓名
	   tud.emp_id, -- 人员编码
       tud.is_job, -- 是否在职
       CAST(tud.days AS DATE) AS work_date, -- 工作日期
       tud.week_type, -- 星期
       IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) AS day_type, -- 日期类型
       tt2.attendance_working_place, -- 上班打卡地点
       tt2.attendance_off_place, -- 下班打卡地点
       tt2.attendance_working_time, -- 考勤开始时间
       tt2.attendance_off_time, -- 考勤结束时间
       tt2.total_work_hour, -- 实际出勤工时
       CASE WHEN IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.attendance_working_time IS NOT NULL THEN 0
            ELSE tt2.normal_work_hour 
       END AS normal_work_hour, -- 正常出勤工时
       CASE WHEN IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.attendance_working_time IS NOT NULL THEN nvl(tt2.overtime_duration,0) 
            WHEN IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type)))  IN ('周末','节假日') AND tt2.attendance_working_time IS NOT NULL THEN 0 
       END AS workday_overtime, -- 工作日加班
       CASE WHEN IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.attendance_working_time IS NOT NULL THEN nvl(tt2.overtime_duration,0) 
            WHEN IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.attendance_working_time IS NOT NULL THEN 0 
       END AS weekend_overtime, -- 周末加班
       tt2.business_id, -- 加班审批单编号
       CASE WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '考勤异常&迟到&早退&加班异常'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
            THEN '考勤异常&迟到&早退'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '考勤异常&迟到&加班异常'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '考勤异常&早退&加班异常'
            WHEN ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '迟到&早退&加班异常'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
            THEN '考勤异常&早退'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
            THEN '考勤异常&迟到'
            WHEN (tt2.attendance_working_time = tt2.attendance_off_time OR IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '考勤异常&加班异常'
            WHEN ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
            THEN '迟到&早退'
            WHEN ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '迟到&加班异常'
            WHEN ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')))
             AND (((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800))))
            THEN '早退&加班异常'
            WHEN tt2.attendance_working_time = tt2.attendance_off_time OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日','全天请假') AND tt2.attendance_working_time IS NULL)
            THEN '考勤异常'
            WHEN (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('上半天请假') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','13:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('上半天请假','周末','节假日') AND tt2.attendance_working_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','08:30:00')) 
            THEN '迟到'
            WHEN (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('下半天请假') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','12:00:00')) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('下半天请假','周末','节假日') AND tt2.attendance_off_time < CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','17:30:00')) 
            THEN '早退'
            WHEN ((tt2.attendance_off_time > CONCAT(SUBSTR(tt2.attendance_working_time,1,10),' ','18:00:00') AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND tt2.business_id IS NULL) OR (tt2.attendance_working_time IS NOT NULL AND IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND tt2.business_id IS NULL)) OR ((IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) NOT IN ('周末','节假日') AND (tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)) OR (IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))) IN ('周末','节假日') AND (tt2.attendance_working_time > tt2.overtime_start_time OR tt2.attendance_off_time < tt2.overtime_end_time OR unix_timestamp(tt2.attendance_off_time) > unix_timestamp(tt2.overtime_end_time) + 1800)))
            THEN '加班异常'
       END AS error_type, -- 异常原因
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT tu.team_ft, -- 一级部门
         tu.team_group, -- 二级部门
         tu.team_sub_group, -- 三级部门
         tu.team_last_group, -- 四级部门
         tu.emp_id, -- 人员编码
         tu.user_name, -- 人员名称
         tu.user_email, -- 人员邮箱
         tu.role_type, -- 组织角色类型
         tu.is_job, -- 是否在职
         tu.is_need_fill_manhour, -- 是否需要填写工时
         tu.emp_position, -- 成员职位
         td.days, -- 日期
         CASE WHEN td.week_date = 1 THEN '星期一'
              WHEN td.week_date = 2 THEN '星期二'
              WHEN td.week_date = 3 THEN '星期三'
              WHEN td.week_date = 4 THEN '星期四'
              WHEN td.week_date = 5 THEN '星期五'
              WHEN td.week_date = 6 THEN '星期六'
              WHEN td.week_date = 7 THEN '星期日' END AS week_type, -- 星期
         CASE WHEN td.day_type = 0 THEN '工作日'
              WHEN td.day_type = 1 THEN '周末'
              WHEN td.day_type = 2 THEN '节假日'
              WHEN td.day_type = 3 THEN '调休' END AS day_type -- 日期类型
  FROM
  (
    SELECT split(tg.org_path_name,'/')[1] AS team_ft, -- 一级部门
           split(tg.org_path_name,'/')[2] AS team_group, -- 二级部门
           split(tg.org_path_name,'/')[3] AS team_sub_group, -- 三级部门
           split(tg.org_path_name,'/')[4] AS team_last_group, -- 四级部门
           te.emp_id, -- 人员编码
           te.emp_name AS user_name, -- 人员名称
           te.email AS user_email, -- 人员邮箱
           tmp.org_role_type AS role_type, -- 组织角色类型
           te.is_job, -- 是否在职
           tmp.is_need_fill_manhour, -- 是否需要填写工时
           te.hired_date, -- 入职日期
           te.quit_date, -- 离职日期
           te.emp_position -- 成员职位
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te -- 钉钉人员基础信息表
    LEFT JOIN 
    (
      SELECT m.emp_id, -- 人员编码
             m.emp_name, -- 人员名称
             m.org_id, -- 组织编码
             m.org_role_type, -- 组织角色类型
             m.is_need_fill_manhour, -- 是否需要填写工时
             m.org_start_date, -- 组织开始日期
             m.org_end_date, -- 组织结束日期
             m.is_job -- 是否在职
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m -- 钉钉员工历史组织架构记录表
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = '${pre1_date}' 
      AND m.is_valid = 1 -- 取有效的（区分多组织）
      AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date) -- 在职的取最新组织，离职的最后组织
      GROUP BY m.emp_id,m.emp_name,m.org_id,m.org_role_type,m.is_need_fill_manhour,m.org_start_date,m.org_end_date,m.is_job
    )tmp
    ON te.emp_id = tmp.emp_id
    LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg -- 钉钉组织架构历史组织表
    ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01','${pre1_date}',IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date)) -- 如果在职取最新天的组织架构，如果离职取离职前一天的，否则取离职当天的
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司' 
    AND te.is_active = 1 -- 取人员有效的 
    AND ((split(tg.org_path_name,'/')[1] IN ('制造部') AND split(tg.org_path_name,'/')[2] IN ('生产') AND split(tg.org_path_name,'/')[3] IN ('零组件') AND split(tg.org_path_name,'/')[4] IN ('部品','线束'))
      OR (split(tg.org_path_name,'/')[1] IN ('制造部') AND split(tg.org_path_name,'/')[2] IN ('生产') AND split(tg.org_path_name,'/')[3] IN ('潜伏式') AND split(tg.org_path_name,'/')[4] IN ('包装','调试','总装'))
      OR (split(tg.org_path_name,'/')[1] IN ('制造部') AND split(tg.org_path_name,'/')[2] IN ('生产') AND split(tg.org_path_name,'/')[3] IN ('生产测试') AND split(tg.org_path_name,'/')[4] IN ('潜伏式','箱式'))
      OR (split(tg.org_path_name,'/')[1] IN ('制造部') AND split(tg.org_path_name,'/')[2] IN ('生产') AND split(tg.org_path_name,'/')[3] IN ('箱式') AND split(tg.org_path_name,'/')[4] IN ('叉臂','底盘','调试','总装')))
    GROUP BY split(tg.org_path_name,'/')[1],split(tg.org_path_name,'/')[2],split(tg.org_path_name,'/')[3],split(tg.org_path_name,'/')[4],te.emp_id,te.emp_name,te.email,tmp.org_role_type,te.is_job,tmp.is_need_fill_manhour,te.hired_date,te.quit_date,te.emp_position
  )tu
  LEFT JOIN 
  (
    SELECT days, -- 日期
           day_type, -- 日期类型
           week_date -- 星期
    FROM ${dim_dbname}.dim_day_date -- 日期维表
    WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
  ) td
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') -- 取入职日期作为开始补零时间，取离职日期作为结束补零时间，未离职的取至今
)tud
-- 请假统计
LEFT JOIN 
(
  SELECT l1.originator_user_id,
         l1.stat_date,
         CASE WHEN l2.leave_type IS NULL THEN l1.leave_type ELSE '全天请假' END AS leave_type
  FROM 
  (
    SELECT l.originator_user_id,
           CAST(l.leave_date AS DATE) AS stat_date,
           CASE WHEN l.period_type = '全天' THEN '全天请假'
                WHEN l.period_type = '下午' THEN '下半天请假'
                WHEN l.period_type = '上午' THEN '上半天请假' 
                WHEN l.period_type = '其它' THEN '哺乳假' END AS leave_type,
           ROW_NUMBER()OVER(PARTITION BY l.originator_user_id,CAST(l.leave_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天请假'
                                                                                                       WHEN l.period_type = '下午' THEN '下半天请假'
                                                                                                       WHEN l.period_type = '上午' THEN '上半天请假' 
                                                                                                       WHEN l.period_type = '其它' THEN '哺乳假' END ASC)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l1
  LEFT JOIN 
  (
    SELECT l.originator_user_id,
           CAST(l.leave_date AS DATE) AS stat_date,
           CASE WHEN l.period_type = '全天' THEN '全天请假'
                WHEN l.period_type = '下午' THEN '下半天请假'
                WHEN l.period_type = '上午' THEN '上半天请假' 
                WHEN l.period_type = '其它' THEN '哺乳假' END AS leave_type,
           ROW_NUMBER()OVER(PARTITION BY l.originator_user_id,CAST(l.leave_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天请假'
                                                                                                       WHEN l.period_type = '下午' THEN '下半天请假'
                                                                                                       WHEN l.period_type = '上午' THEN '上半天请假' 
                                                                                                       WHEN l.period_type = '其它' THEN '哺乳假' END ASC)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
  WHERE l1.rn = 1 
)tt1
ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
LEFT JOIN  
(
  -- 未加班
  SELECT NULL AS business_id, -- 加班审批单
         e.emp_id, -- 人员编码
         e.emp_name, -- 人员名称
         e.att_checkin_work_date AS attendance_work_date, -- 考勤日期
         IF(e.start_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_start_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_start_place,IF(e.start_att_checkin_type = '钉钉签到',e.att_checkin_start_place,e.start_att_checkin_type)) as attendance_working_place, -- 考勤签到地点
         IF(e.end_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_end_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_end_place,IF(e.end_att_checkin_type = '钉钉签到',e.att_checkin_end_place,e.end_att_checkin_type)) as attendance_off_place, -- 考勤签退地点
         e.att_checkin_start_time AS attendance_working_time, -- 考勤签到时间
         CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') AS attendance_start_planing_time, -- 计划签到时间
         CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
              WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
              ELSE e.att_checkin_start_time 
         END AS attendance_start_actual_time, -- 实际签到时间
         e.att_checkin_end_time AS attendance_off_time, -- 考勤签退时间
         CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00') AS attendance_end_planing_time, -- 计划签退时间
         IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time) AS attendance_end_actual_time, -- 实际签腿时间
         CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(e.att_checkin_start_time))/3600 AS DECIMAL(10,1)) AS total_work_hour, -- 总考勤工时
         CASE WHEN IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1))) = 0 THEN 0
              WHEN CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                        WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                        ELSE e.att_checkin_start_time END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time) >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                               WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                               ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1))) - 1
              ELSE IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1)))
         END AS normal_work_hour, -- 实际工作时间
         NULL AS overtime_start_time, -- 加班申请开始时间
         NULL AS overtime_end_time, -- 加班申请结束时间
         NULL AS overtime_start_time_actual, -- 实际加班开始时间
         NULL AS overtime_end_time_actual, -- 实际加班结束时间
         NULL AS overtime_duration -- 加班时长
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di e
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY l.applicant_userid,SUBSTR(l.overtime_start_time,1,10) ORDER BY l.process_start_time DESC)rn
    FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df l
    WHERE l.d = '${pre1_date}' AND l.org_name ='宝仓' AND l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' 
  )l
  ON e.emp_id = l.applicant_userid AND e.att_checkin_work_date = SUBSTR(l.overtime_start_time,1,10) AND l.rn = 1
  WHERE l.business_id IS NULL
  
  UNION ALL
  -- 加班
  SELECT l.business_id, -- 加班审批单
         e.emp_id, -- 人员编码
         e.emp_name, -- 人员名称
         e.att_checkin_work_date AS attendance_work_date, -- 考勤日期
         IF(e.start_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_start_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_start_place,IF(e.start_att_checkin_type = '钉钉签到',e.att_checkin_start_place,e.start_att_checkin_type)) as attendance_working_place, -- 考勤签到地点
         IF(e.end_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_end_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_end_place,IF(e.end_att_checkin_type = '钉钉签到',e.att_checkin_end_place,e.end_att_checkin_type)) as attendance_off_place, -- 考勤签退地点
         e.att_checkin_start_time AS attendance_working_time, -- 考勤签到时间
         CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') AS attendance_start_planing_time, -- 计划签到时间
         CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
              WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
              ELSE e.att_checkin_start_time END AS attendance_start_actual_time, -- 实际签到时间
         e.att_checkin_end_time AS attendance_off_time, -- 考勤签退时间
         CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00') AS attendance_end_planing_time, -- 计划签退时间
         IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time) AS attendance_end_actual_time, -- 实际签腿时间
         CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(e.att_checkin_start_time))/3600 AS DECIMAL(10,1)) AS total_work_hour, -- 总考勤工时
         CASE WHEN IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1))) = 0 THEN 0
              WHEN CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                        WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                        ELSE e.att_checkin_start_time END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time) >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                               WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                               ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1))) - 1
              ELSE IF(e.att_checkin_start_time = e.att_checkin_end_time OR CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                ELSE e.att_checkin_start_time END >= IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time),0,CAST((unix_timestamp(IF(e.att_checkin_end_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00'),e.att_checkin_end_time)) - unix_timestamp(CASE WHEN e.att_checkin_start_time >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') AND e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN e.att_checkin_start_time <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00') THEN CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','08:30:00')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE e.att_checkin_start_time END)) / 3600 AS DECIMAL(10,1)))
         END AS normal_work_hour, -- 实际工作时间
         CONCAT(l.overtime_start_time,':00') AS overtime_start_time, -- 加班申请开始时间
         CONCAT(l.overtime_end_time,':00') AS overtime_end_time, -- 加班申请结束时间
         CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
              WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
              WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN e.att_checkin_start_time
              WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
              WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
              ELSE NULL
         END AS overtime_start_time_actual, -- 实际加班开始时间
         CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
              WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_end_time,':00')
              WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CONCAT(l.overtime_end_time,':00')
              WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
              WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
              ELSE NULL 
         END AS overtime_end_time_actual, -- 实际加班结束时间
         CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN 0 -- 单卡不计算
              WHEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN e.att_checkin_start_time
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        ELSE NULL
              END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') -- 签到时间小于12:00
              AND CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       ELSE NULL 
              END >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') -- 签退时间大于13:00
              AND CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN e.att_checkin_start_time
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                       ELSE NULL
              END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00') -- 签到时间小于17:30
              AND CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       ELSE NULL 
              END >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','18:00:00') -- 签退时间大于18:00 
              THEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN 0
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(e.att_checkin_start_time)) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   ELSE 0 END - 1.5 -- 减去午饭时间1小时和晚饭时间0.5小时
              WHEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN e.att_checkin_start_time
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        ELSE NULL
              END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','12:00:00') -- 签到时间小于12:00
              AND CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       ELSE NULL 
              END >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','13:00:00') -- 签退时间大于13:00
              THEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN 0
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(e.att_checkin_start_time)) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   ELSE 0 END - 1 -- 减去午饭时间1小时
              WHEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN e.att_checkin_start_time
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_start_time,':00')
                        ELSE NULL
              END <= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','17:30:00') -- 签到时间小于17:30
              AND CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN NULL
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CONCAT(l.overtime_end_time,':00')
                       WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN e.att_checkin_end_time
                       ELSE NULL 
              END >= CONCAT(SUBSTR(e.att_checkin_start_time,1,10),' ','18:00:00') -- 签退时间大于18:00 
              THEN CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN 0
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(e.att_checkin_start_time)) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                        WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   ELSE 0 END - 0.5 -- 减去晚饭时间0.5小时
         ELSE CASE WHEN e.att_checkin_start_time = e.att_checkin_end_time THEN 0
                   WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time >= CONCAT(l.overtime_end_time,':00') AND CONCAT(l.overtime_end_time,':00') >= e.att_checkin_start_time AND CONCAT(l.overtime_end_time,':00') <= e.att_checkin_end_time THEN CAST((unix_timestamp(CONCAT(l.overtime_end_time,':00')) - unix_timestamp(e.att_checkin_start_time)) / 3600 AS DECIMAL(10,1)) 
                   WHEN e.att_checkin_start_time <= CONCAT(l.overtime_start_time,':00')AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   WHEN e.att_checkin_start_time > CONCAT(l.overtime_start_time,':00') AND e.att_checkin_end_time < CONCAT(l.overtime_end_time,':00') THEN CAST((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(CONCAT(l.overtime_start_time,':00'))) / 3600 AS DECIMAL(10,1)) 
                   ELSE 0 END
         END AS overtime_duration -- 加班时长
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di e
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY l.applicant_userid,SUBSTR(l.overtime_start_time,1,10) ORDER BY l.process_start_time DESC)rn
    FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df l
    WHERE l.d = '${pre1_date}' AND l.org_name ='宝仓' AND l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' 
  )l
  ON e.emp_id = l.applicant_userid AND e.att_checkin_work_date = SUBSTR(l.overtime_start_time,1,10) AND l.rn = 1
  WHERE l.business_id IS NOT NULL
)tt2
ON tt2.emp_id = tud.emp_id AND tt2.attendance_work_date = tud.days;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      