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
-- ads_pms_overseas_labour_service_detail    --pms 海外劳务考勤明细

INSERT overwrite table ${ads_dbname}.ads_pms_overseas_labour_service_detail
SELECT '' as id,
       s.start_date, -- 开始日期（周维度）
       s.service_company_name, -- 劳务公司名称
       nvl(pvd.project_code,s.project_code) as project_code, -- 项目编码
       nvl(pvd.project_name,s.project_name) as project_name, -- 项目名称
       s.project_manager, -- pm
       s.spm_name, -- spm
       pvd.project_dispaly_state as project_operation_state, -- 项目阶段
       pvd.project_area, -- 项目区域
       pvd.project_ft, -- 所属ft
       pvd.project_priority, -- 项目等级
       pvd.project_progress_stage, -- 项目进展阶段
       s.people_numbers, -- 劳务人数
       s.attendance_duration, -- 考勤时长
       s.overtime_duration, -- 加班时长
       s.rmb_cost, -- 劳务成本
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_pms_overseas_labour_service_info_df s
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON nvl(pvd.project_code,'unknown1') = nvl(s.project_code,'unknown2') OR nvl(pvd.project_sale_code,'unknown1') = nvl(s.project_code,'unknown2')
WHERE s.d = '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"