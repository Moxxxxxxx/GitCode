#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-23 创建
#-- 2 wangyingying 2022-11-28 修改异常数据标识逻辑
#-- 3 wangyingying 2022-12-14 修改临时表来源
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
--制造部人员工作效能表 ads_team_ft_engineer_member_work_efficiency

INSERT overwrite table ${ads_dbname}.ads_team_ft_engineer_member_work_efficiency
SELECT '' AS id,
       team_ft, -- 一级部门
       team_group, -- 二级部门
	   team_sub_group, -- 三级部门
       team_last_group, -- 四级部门
	   emp_position, -- 职位
       team_member, -- 人员姓名
       is_job, -- 是否在职
       work_date, -- 工作日期
       week_type, -- 星期
       day_type, -- 日期类型
       attendance_working_place, -- 上班打卡地点
       attendance_off_place, -- 下班打卡地点
       attendance_working_time, -- 考勤开始时间
       attendance_off_time, -- 考勤结束时间
       total_work_hour, -- 实际出勤工时
       normal_work_hour, -- 正常出勤工时
       workday_overtime, -- 工作日加班
       weekend_overtime, -- 周末加班
       business_id, -- 加班审批单编号
       error_type, -- 异常原因
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${tmp_dbname}.tmp_team_ft_engineer_member_work_efficiency
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      