#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads




    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
-- 虚拟组人员统计 ads_team_ft_virtual_member_count_info 

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_count_info
SELECT '' as id,
       DATE_ADD(CURRENT_DATE(), -1) as cur_date, -- 统计日期
       i.virtual_org_name, -- 虚拟组名称
       SUM(if(i.role_type = '产品',1,0)) as po_qty, --产品人数
       SUM(if(i.role_type = 'UED',1,0)) as ued_qty, -- UED人数
       SUM(if(i.role_type = '研发',1,0)) as dev_qty, -- 研发人数
       SUM(if(i.role_type = '测试',1,0)) as test_qty, -- 测试人数
       COUNT(DISTINCT i.emp_code) as total_qty, --总人数
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i 
WHERE i.is_active = 1 
GROUP BY i.virtual_org_name,DATE_ADD(CURRENT_DATE(), -1)
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"