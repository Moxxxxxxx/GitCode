#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-29创建
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
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--生产制造部生产数据损失明细表 ads_team_ft_engineer_member_production_losing_detail

INSERT overwrite table ${ads_dbname}.ads_team_ft_engineer_member_production_losing_detail
SELECT '' AS id, -- 主键
       l.process_instance_id,
       l.business_id,
       l.production_date,
       l.work_order_number,
       l.product_part_number,
       l.product_process,
       l.model_code,
       l.product_name,
       l.project_code,
       b.project_name, -- 项目名称
       b.project_attr_ft AS project_ft, -- 项目所属ft
       l.all_losing_hours_minutes,
       l.loss_ategory,
       l.accountability_unit,
       l.losing_desc,
       l.losing_hours,
       ROW_NUMBER()OVER(PARTITION BY l.process_instance_id,l.business_id,l.production_date,l.work_order_number,l.product_part_number ORDER BY l.losing_desc DESC) work_order_sort,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time   
FROM ${dwd_dbname}.dwd_dtk_production_losing_info_df l
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b 
ON b.d = '${pre1_date}' AND (l.project_code = b.project_code OR l.project_code = b.project_sale_code)
WHERE l.d = '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      