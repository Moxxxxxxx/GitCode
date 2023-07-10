#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-03-07 创建
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
--项目车架生产材料费用明细表 ads_project_frame_production_material_cost_detail

INSERT overwrite table ${ads_dbname}.ads_project_frame_production_material_cost_detail
SELECT '' AS id, -- 主键
       t1.order_bill_no, -- 生产订单编号
       t1.frame_no, -- 车架号
       t1.start_date, -- 开始日期
       t1.end_date, -- 结束日期
       t1.duration_days, -- 天数
       t2.pick_bill_no, -- 领料单据编码
       t2.pick_bill_date, -- 领料日期
       t2.material_id, -- 物料id
       t2.material_number, -- 物料编码
       t2.material_name, -- 物料名称
       t2.pick_quantity, -- 领料数量
       t2.consume_quantity, -- 用料数量
       t2.pick_price, -- 单价
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM
(
  SELECT r2.order_bill_no,
         r2.order_entry_id,
         r2.frame_no,
         MIN(r3.pick_bill_date) AS start_date,
         nvl(r2.order_instock_date,'${pre1_date}') AS end_date,
         DATEDIFF(nvl(r2.order_instock_date,'${pre1_date}'),MIN(r3.pick_bill_date)) AS duration_days
  FROM
  -- 车架号关联生产入库表 获取订单号
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY l.frame_no ORDER BY l.order_instock_date DESC) AS rn
    FROM ${dwd_dbname}.dwd_kde_agv_production_link_info_df l
    WHERE l.d = '${pre1_date}' AND l.is_agv_material = 1 AND l.frame_no IS NOT NULL
  )r2
  -- 订单号关联领料用料表 获取用料成本
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_kde_agv_production_material_info_df l
    WHERE l.d = '${pre1_date}'
  )r3
  ON r2.order_entry_id = r3.order_entry_id
  WHERE r2.rn = 1
  GROUP BY r2.order_bill_no,r2.order_entry_id,r2.frame_no,nvl(r2.order_instock_date,'${pre1_date}')
)t1
LEFT JOIN 
(
  SELECT *
  FROM ${dwd_dbname}.dwd_kde_agv_production_material_info_df l
  WHERE l.d = '${pre1_date}'
)t2
ON t1.order_entry_id = t2.order_entry_id;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      