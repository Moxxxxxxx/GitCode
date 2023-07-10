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
--ads_project_work_order_new    --项目工单数统计

INSERT overwrite table ${ads_dbname}.ads_project_work_order_new
SELECT '' as id, -- 主键
       td.days as cur_date, -- 统计日期
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       if(i.work_order_num is null,0,i.work_order_num) as work_order_new_num, -- 工单数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dim_dbname}.dim_day_date td
LEFT JOIN  ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN
(
  SELECT i.project_code,
         date(i.created_time) as work_order_create_time,
         count(DISTINCT i.ticket_id) as work_order_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df i
  WHERE i.d = '${pre1_date}' AND i.work_order_status != '已驳回'
  GROUP BY i.project_code,date(i.created_time) 
) i
ON (b.project_code = i.project_code OR b.project_sale_code = i.project_code) AND td.days = i.work_order_create_time 
WHERE td.days >= '2021-01-01' AND td.days <= nvl(b.post_project_date,'${pre1_date}')
ORDER by td.days desc;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"