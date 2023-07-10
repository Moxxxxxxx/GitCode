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
--ads_dtk_production_hands_show_detail    --钉钉举手单明细表

INSERT overwrite table ${ads_dbname}.ads_dtk_production_hands_show_detail
SELECT '' AS id, -- 主键
       h.org_name, -- 公司名称
       h.business_id, -- 审批流程编码
       h.approval_user_names, -- 审批人名称
       h.process_start_time, -- 流程开始时间
       h.process_end_time, -- 流程完成时间
       h.applicant_dept_name, -- 发起部门
       h.applicant_user_name AS team_member, -- 发起人
       h.approval_result, -- 审批结果
       h.approval_status, -- 审批状态
       h.approval_title, -- 审批实例标题
       h.work_order_number, -- 工单号
       h.work_order_type, -- 工单类型
       h.product_agv_type, -- 产品型号
       h.production_procedure, -- 生产工序
       h.influence_people_number, -- 影响人数
       h.problem_desc, -- 问题现象描述
       h.exception_picture_desc, -- 异常说明图片
       h.confirmation_response, -- 响应确认
       h.liability_judgment, -- 责任判定
       h.judgment_basis_description, -- 判定依据说明
       h.problem_cause, -- 问题原因分析
       h.interim_measures, -- 临时处理对策
       h.question_type, -- 问题类别
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_dtk_production_hands_show_info_df h -- 钉钉宝仓产线举手单信息记录
WHERE h.d = '${pre1_date}' AND h.is_valid = 1 -- 剔除撤销记录和有更新前的记录
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"