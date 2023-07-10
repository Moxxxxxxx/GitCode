#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset看板和图表使用占比用表
#-- 注意 ： 
#-- 输入表 : dws.dws_report_dashboard_daycount、dws.dws_report_slice_daycount
#-- 输出表 ：ads.ads_superset_activity_occupation
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 MODIFY 别名
#-- 3 查博文 2021-11-15 美化
#-- 4 查博文 2021-11-15 增加筛选条件，去除数据开发人人员
#-- 5 wangyingying 2023-01-28 优化日期
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
--看板和图表使用率 ads_superset_activity_occupation （superset看板和图标使用次数）

INSERT overwrite table ${ads_dbname}.ads_superset_activity_occupation
SELECT d.days AS activities_date,
       nvl(dashboards.dashboards_count,0) AS dashboards_count,
       nvl(charts.charts_count,0) AS charts_count
FROM 
(
  SELECT days
  FROM ${dim_dbname}.dim_day_date d
  WHERE d.days >= '2021-11-01' AND d.days <= '${pre1_date}'
)d
LEFT JOIN
(
  SELECT start_date,
         SUM(operation_count) AS dashboards_count
  FROM ${dws_dbname}.dws_report_dashboard_daycount
  WHERE dashboard_id != 'UNKNOWN' AND user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
  GROUP BY start_date 
) dashboards
ON d.days = dashboards.start_date
LEFT JOIN
(
  SELECT start_date,
         SUM(operation_count) AS charts_count
  FROM ${dws_dbname}.dws_report_slice_daycount
  WHERE user_cname NOT IN('admin','杨萍','王莹莹','马婧','王梓明','王莹莹','查博文')
  GROUP BY start_date
) charts
ON d.days = charts.start_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"