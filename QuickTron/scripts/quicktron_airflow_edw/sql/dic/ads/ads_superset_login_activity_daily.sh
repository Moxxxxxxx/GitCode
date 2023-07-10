#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset看板和图表使用占比用表
#-- 注意 ： 
#-- 输入表 : dws.dws_report_user_login_daycount
#-- 输出表 ：ads.ads_superset_activity_occupation
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 美化
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
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
--逐日用户登录记录 ads_superset_login_activity_daily （superset看板活跃用户）

INSERT overwrite table ${ads_dbname}.ads_superset_login_activity_daily
SELECT 
       user_id                                                 as user_id,
       user_name                                               as user_name,
       date_node                                               as date_node,
       current_date_login_count                                as current_date_login_count,
       total_login_count                                       as total_login_count
FROM( 
	SELECT 
    		daily.user_id,
    		user_cname as user_name,
            start_date as date_node,
            current_date_login_count,
            total_login_count
     FROM( 
        	SELECT 
            	user_id,
            	user_cname,
                start_date,
                SUM(operation_count) as current_date_login_count
        	FROM 
            	${dws_dbname}.dws_report_user_login_daycount
        	GROUP BY 
            	user_id,
                user_cname,
                start_date
         ) as daily  
     LEFT JOIN 
         (
        	SELECT 
            	user_id,
                SUM(operation_count) as total_login_count
        	FROM 
            	${dws_dbname}.dws_report_user_login_daycount 
        	GROUP BY 
            	user_id,
                user_cname
       	 ) as totally 
     ON daily.user_id = totally.user_id
    ) t
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"