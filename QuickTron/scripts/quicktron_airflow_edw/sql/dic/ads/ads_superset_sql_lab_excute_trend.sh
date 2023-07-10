#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dws.dws_report_sql_edit_info_daycount
#-- 输出表 ：ads.ads_superset_sql_lab_excute_trend
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 daily_total字段名修改
#-- 3 查博文 2021-11-15 美化
#-- 1.1 查博文 2021-11-16 更新过滤条件，去除开发人员
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
--sqllab使用趋势 ads_superset_sql_lab_excute_trend （superset sql-lab执行次数）

INSERT overwrite table ${ads_dbname}.ads_superset_sql_lab_excute_trend
SELECT activity_date                                          as activity_date,
       activity_week                                          as activity_week,
       daily_total                                      	  as daily_total
FROM(
		SELECT 
        	start_date as activity_date, 
            start_week as activity_week, 
            SUM(operation_count) as daily_total
      	FROM 
        	${dws_dbname}.dws_report_sql_edit_info_daycount 
        WHERE 
          	user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
      	GROUP BY 
        	start_date,start_week
    ) t
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"