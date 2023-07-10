#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-25 创建
# ------------------------------------------------------------------------------------------------


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
--ads_dtk_process_work_overtime_detail    --钉钉加班申请明细表

INSERT overwrite table ${ads_dbname}.ads_dtk_process_work_overtime_detail
SELECT '' AS id, -- 主键
       w.org_name, -- 公司名称
       w.business_id, -- 审批流程编码
       w.approval_user_names, -- 审批人名称
       w.process_start_time, -- 流程开始时间
       w.process_end_time, -- 流程完成时间
       w.applicant_dept_name, -- 发起部门
       w.approval_result, -- 审批结果
       w.approval_status, -- 审批状态
       w.approval_title, -- 审批实例标题
       w.work_overtime_reason, -- 加班原因
       w.is_legal_holiday, -- 是否法定假日
       w.work_overtime_accounting_method, -- 加班核算方式
       w.overtime_person AS team_member, -- 加班人
       w.overtime_start_time, -- 开始时间
       w.overtime_end_time, -- 结束时间
       w.overtime_duration, -- 时长
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df w
WHERE w.d = '${pre1_date}' AND w.is_valid = 1 -- 剔除撤销记录和有更新前的记录
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"