#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops用户信息表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_dtk_emp_info_df
#-- 输出表 ：ads.ads_devops_dtk_user_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-08-16 CREATE
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
--devops用户信息表 ads_devops_dtk_user_info 

INSERT overwrite table ${ads_dbname}.ads_devops_dtk_user_info
select
	emp_id
	,emp_name
	,split(a.name,'（')[0] as name_cleaned 
	,mobile_number
	,email_prefix
	,email
	,is_job
	,org_ids
	,is_leader
	,emp_position 
from 
(
select 
	emp_id
	,emp_name
	,case 
	when emp_name like '%v-%' then split(emp_name,'v-')[1]
	when emp_name like '%V-%' then split(emp_name,'V-')[1]
	else emp_name
	end as name 
	,mobile_number
	,split(email,'@')[0] as email_prefix
	,email
	,is_job
	,org_ids
	,is_leader
	,emp_position 
from 
	${dwd_dbname}.dwd_dtk_emp_info_df 
where  
	d = '${pre1_date}' and org_company_name = '上海快仓智能科技有限公司' 
	and is_hide = 0 
	and emp_name <> '测试' 
	and org_cnames not like '%保洁%' 
	and email is not null
) a;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"