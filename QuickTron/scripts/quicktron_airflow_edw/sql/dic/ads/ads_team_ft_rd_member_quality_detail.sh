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
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--研发成员质量明细表 ads_team_ft_rd_member_quality_detail

INSERT overwrite table ${ads_dbname}.ads_team_ft_rd_member_quality_detail
SELECT '' AS id, -- 主键
       t.\`number\` AS work_id, -- 工作项编码
       t.summary AS work_summary, -- 工作项标题
       t.task_create_time, -- 工作项创建时间
       t.server_update_time AS task_update_time, -- 工作项更新时间
       t.task_status_cname AS task_status, -- 工作项状态
       t.severity_level, -- 严重等级
       t.defect_validity, -- 是否有效
       t.sprint_classify_name, -- 所属迭代
       t.project_classify_name, -- 项目分类姓名
       IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type, -- 缺陷类型：线上缺陷|线下缺陷
       t.defect_discovery_stage, -- 缺陷发现阶段
       t.defect_best_discovery_stage, -- 缺陷最佳发现阶段
       emp1.team_ft AS assign_team_ft, -- 负责人一级部门
       emp1.team_group AS assign_team_group, -- 负责人二级部门
       emp1.team_sub_group AS assign_team_sub_group, -- 负责人三级部门
       emp1.team_last_group AS assign_team_last_group, -- 负责人四级部门
       nvl(emp1.emp_name,t.task_assign_cname) AS assign_team_member, -- 负责人人员名称
       emp2.team_ft AS repair_team_ft, -- 修复人一级部门
       emp2.team_group AS repair_team_group, -- 修复人二级部门
       emp2.team_sub_group AS repair_team_sub_group, -- 修复人三级部门
       emp2.team_last_group AS repair_team_last_group, -- 修复人四级部门
       nvl(emp2.emp_name,t.task_repair_user_cname) AS repair_team_member, -- 修复人人员名称
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time     
FROM ${dwd_dbname}.dwd_ones_task_info_ful t
LEFT JOIN
(
  SELECT dept_name AS team_ft,
         team_org_name_map[\"team1\"] AS team_group,
         team_org_name_map[\"team2\"] AS team_sub_group,
         team_org_name_map[\"team3\"] AS team_last_group,
         emp_id,
         emp_name,
         email
  FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info 
  WHERE is_rd_emp = 1
)emp1
ON t.task_assign_email = emp1.email
LEFT JOIN
(
  SELECT dept_name AS team_ft,
         team_org_name_map[\"team1\"] AS team_group,
         team_org_name_map[\"team2\"] AS team_sub_group,
         team_org_name_map[\"team3\"] AS team_last_group,
         emp_id,
         emp_name,
         email
  FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info 
  WHERE is_rd_emp = 1
)emp2
ON t.task_repair_user_email = emp2.email
WHERE t.issue_type_cname ='缺陷' AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) AND t.status = 1 AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 严重等级为3级以上且有效的缺陷
  AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[硬件自动化]智驾车载软件、硬件开发')  
  AND t.task_create_time >= '2022-10-27'
  AND (emp1.email IS NOT NULL OR emp2.email IS NOT NULL);
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      