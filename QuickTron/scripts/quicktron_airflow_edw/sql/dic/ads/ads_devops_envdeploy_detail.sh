#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：devops环境部署时长
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_env_deploy_record_info_df
#-- 输出表 ：ads.ads_devops_envdeploy_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-06-06 CREATE
#-- 1 查博文 2022-08-01 增加组织架构
#-- 2 查博文 2022-08-12 增加离职人员ft
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads

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
-------------------------------------------------------------------------------------------------------------00
-- ads_devops_envdeploy_detail （devops环境部署明细）

with rec as (
SELECT 
	env_id
    ,env_deploy_name
    ,status_id
    ,deploy_status
    ,deploy_progress
    ,owner_id
    ,owner_name
    ,server_master_ip
    ,server_slave_ip
    ,deploy_date
    ,deploy_starttime
    ,deploy_endtime
    ,deploye_duration_s
from 
(
SELECT  
  id as env_id
  ,env_deploy_name
  ,deploy_status as status_id
  ,CASE 
  WHEN deploy_status = 0 THEN '正在创建中'
  WHEN deploy_status = 1 THEN '创建完成'
  WHEN deploy_status = 2 THEN '创建失败'
  WHEN deploy_status = 3 THEN '删除成功'
  WHEN deploy_status = 4 THEN '删除失败'
  WHEN deploy_status = 5 THEN '更新成功'
  WHEN deploy_status = 6 THEN '更新失败'
  WHEN deploy_status = 7 THEN '自动删除'
  ELSE concat('未知状态编号')
  END as deploy_status
  ,deploy_progress
  ,owner_id
  ,owner_name 
  ,server_master_ip
  ,server_slave_ip 
  ,DATE_FORMAT(create_time,'yyyy-MM-dd') as deploy_date
  ,create_time as deploy_starttime
  ,deploy_finish_time as deploy_endtime
  ,(unix_timestamp(deploy_finish_time) - unix_timestamp(create_time)) as deploye_duration_s
  ,rank() over(partition by id order by update_time) as operation_sequence 
from 
  ${dwd_dbname}.dwd_devops_env_deploy_record_info_df 
WHERE 
   d = '${pre1_date}'
   ) aa 
WHERE  operation_sequence = 1
),
emp as (
SELECT DISTINCT 
					CASE 
					WHEN tg.org_name_2 is null then linfo.team_ft
					ELSE tg.org_name_2
					END as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as org_role_type,
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
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
    LEFT JOIN (select
	emp_name
	,emp_id
	,team_ft 
	from
	(
	select 
	emp_name
	,emp_id
	,dept_name as team_ft
	,rank() over(partition by emp_id order by org_start_date desc) as rn 
	from 
	${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df 
	where 
	d = '${pre1_date}' and is_job = 0
	) a where rn = 1 ) linfo ON te.emp_id = linfo.emp_id
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      -- AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
)
INSERT overwrite table ${ads_dbname}.ads_devops_envdeploy_detail
--部署时长指标
select 
	rec.*
	,nvl(emp.user_name,'钉钉无相关信息') as dtk_user_name
	,nvl(emp.team_ft,'钉钉无相关信息') as team_ft
	,nvl(emp.team_group,'钉钉无相关信息') as team_group
	,nvl(emp.team_sub_group,'钉钉无相关信息') as team_sub_group
	,nvl(emp.emp_position,'钉钉无相关信息') as emp_position
from 
	rec 
	left join 
	(select * from dwd.dwd_devops_user_info_df where d = '${pre1_date}') a on rec.owner_id = a.id
	left join 
	emp on a.dingding_id = emp.emp_id



-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"