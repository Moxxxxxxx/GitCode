#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
project_code=A51118


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=spark;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- 单项目abc动销统计 ads_single_project_abc_count_info 

-- 动销数据
with t1 as 
(
  SELECT p.cur_date,
         p.project_code,
         p.sku_id,
         p.sku_picking_orderline,
         p.sku_picking_quantity
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN 
  (
    SELECT TO_DATE(p.order_updated_time) as cur_date,
           p.project_code,
           p.sku_id,
           COUNT(DISTINCT p.id) as sku_picking_orderline,
           SUM(nvl(p.fulfill_quantity,0)) as sku_picking_quantity
    FROM ${dwd_dbname}.dwd_picking_order_detail_info p
    WHERE TO_DATE(p.order_updated_time) = '${pre1_date}' AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.quantity = p.fulfill_quantity -- 订单已完成
    GROUP BY TO_DATE(p.order_updated_time),p.project_code,p.sku_id
  )p
  ON c.project_code = p.project_code
  WHERE c.project_product_type_code IN (1,2) -- 到人
  ORDER BY sku_picking_orderline desc
),
t2 as
(
  SELECT p.cur_date,
         p.project_code,
         p.total_picking_orderline,
         p.total_picking_quantity
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN
  (
    SELECT TO_DATE(p.order_updated_time) as cur_date,
           p.project_code,
           COUNT(DISTINCT p.id) as total_picking_orderline,
           SUM(nvl(p.fulfill_quantity,0)) as total_picking_quantity
    FROM ${dwd_dbname}.dwd_picking_order_detail_info p
    WHERE TO_DATE(p.order_updated_time) = '${pre1_date}' AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.quantity = p.fulfill_quantity -- 订单已完成
    GROUP BY TO_DATE(p.order_updated_time),p.project_code
  )p
  ON c.project_code = p.project_code
  WHERE c.project_product_type_code IN (1,2) -- 到人
),
t3 as 
(
  SELECT bb.cur_date, -- 统计日期
         bb.project_code, -- 项目编码
         li.sku_id, -- sku
         SUM(nvl(li.quantity,0)) as sku_inventory_quantity
  FROM ${dim_dbname}.dim_collection_project_record_ful c  
  LEFT JOIN 
  (
    SELECT TO_DATE(bb.d) as cur_date,
           bb.project_code,
           bb.id,
           bb.bucket_code
    FROM ${dwd_dbname}.dwd_basic_bucket_base_info_df bb
    WHERE bb.d = '${pre1_date}' AND bb.bucket_state = 'effective' AND ((bb.pt = 'A51118' AND bb.bucket_type_id = 57) OR (bb.pt = 'A51149' AND bb.bucket_type_id IN (33,36,37)) OR (bb.pt = 'A51264' AND bb.bucket_type_id = 1) OR (bb.pt = 'A51203' AND bb.bucket_type_id IN (124,119)) OR (bb.pt = 'C35052' AND bb.bucket_type_id IN (122,128))) -- 正常货架
  )bb
  ON c.project_code = bb.project_code 
  LEFT JOIN 
  (
    SELECT TO_DATE(sb.d) as cur_date,
           sb.project_code,
           sb.slot_code,
           sb.bucket_id
    FROM ${dwd_dbname}.dwd_basic_slot_base_info_df sb
    WHERE sb.d = '${pre1_date}' AND sb.slot_state = 'effective'
  )sb
  ON c.project_code = sb.project_code AND bb.id = sb.bucket_id
  INNER JOIN  
  (
    SELECT TO_DATE(li.d) as cur_date,
           li.project_code,
           li.bucket_code,
           li.bucket_slot_code,
           li.sku_id,
           li.quantity
    FROM ${dwd_dbname}.dwd_inventory_level3_inventory_info_df li
    WHERE li.d = '${pre1_date}' AND li.sku_id is not null AND li.quantity != 0 -- 剔除零库存
  )li
  ON c.project_code =  li.project_code AND bb.bucket_code = li.bucket_code AND sb.slot_code = li.bucket_slot_code 
  WHERE c.project_product_type_code IN (1,2) -- 到人
  GROUP BY bb.cur_date,bb.project_code,li.sku_id
),
t4 as 
(
  SELECT bb.cur_date, -- 统计日期
         bb.project_code, -- 项目编码
         COUNT(DISTINCT li.sku_id) as total_inventory_num, -- sku库存个数
         SUM(nvl(li.quantity,0)) as total_inventory_quantity -- 库存总数
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN 
  (
    SELECT TO_DATE(bb.d) as cur_date,
           bb.project_code,
           bb.id,
           bb.bucket_code
    FROM ${dwd_dbname}.dwd_basic_bucket_base_info_df bb
    WHERE bb.d = '${pre1_date}' AND bb.bucket_state = 'effective' AND ((bb.pt = 'A51118' AND bb.bucket_type_id = 57) OR (bb.pt = 'A51149' AND bb.bucket_type_id IN (33,36,37)) OR (bb.pt = 'A51264' AND bb.bucket_type_id = 1) OR (bb.pt = 'A51203' AND bb.bucket_type_id IN (124,119)) OR (bb.pt = 'C35052' AND bb.bucket_type_id IN (122,128))) -- 正常货架
  )bb
  ON c.project_code = bb.project_code
  LEFT JOIN 
  (
    SELECT TO_DATE(sb.d) as cur_date,
           sb.project_code,
           sb.slot_code,
           sb.bucket_id
    FROM ${dwd_dbname}.dwd_basic_slot_base_info_df sb
    WHERE sb.d = '${pre1_date}' AND sb.slot_state = 'effective'
  )sb
  ON c.project_code = sb.project_code AND bb.id = sb.bucket_id
  INNER JOIN  
  (
    SELECT TO_DATE(li.d) as cur_date,
           li.project_code,
           li.bucket_code,
           li.bucket_slot_code,
           li.sku_id,
           li.quantity
    FROM ${dwd_dbname}.dwd_inventory_level3_inventory_info_df li
    WHERE li.d = '${pre1_date}' AND li.sku_id is not null AND li.quantity != 0 -- 剔除零库存
  )li
  ON c.project_code =  li.project_code AND bb.bucket_code = li.bucket_code AND sb.slot_code = li.bucket_slot_code 
  WHERE c.project_product_type_code IN (1,2) -- 到人
  GROUP BY bb.cur_date,bb.project_code
)

INSERT overwrite table ${ads_dbname}.ads_single_project_abc_count_info
SELECT '' as id, -- 主键
       abc.cur_date, -- 统计日期
       abc.project_code, -- 项目编码
       abc.class_type, -- 动销分类
       COUNT(DISTINCT abc.sku_id) as sku_num, -- abc类sku个数
       CAST(COUNT(DISTINCT abc.sku_id) / abc.total_inventory_num as decimal(10,3)) as sku_rate, -- abc类sku个数占比
       SUM(abc.sku_picking_quantity) as sku_picking_quantity, -- abc类sku出库件数
       CAST(SUM(abc.sku_picking_quantity) / abc.total_picking_quantity as decimal(10,3)) as picking_quantity_rate, -- abc类sku出库件数占比
       SUM(abc.sku_inventory_quantity) as sku_inventory_quantity, -- abc类sku库存件数
       CAST(SUM(abc.sku_inventory_quantity) / abc.total_inventory_quantity as decimal(10,3)) as inventory_quantity_rate, -- abc类sku库存件数占比
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT t1.cur_date,
         t1.project_code,
         t1.sku_id,
         t1.sku_picking_quantity,
         t2.total_picking_quantity,
         t3.sku_inventory_quantity,
         t4.total_inventory_num,
         t4.total_inventory_quantity,
         t1.sku_picking_orderline,
         t2.total_picking_orderline,
         IF(sum(t1.sku_picking_orderline) over(partition by t1.cur_date,t1.project_code order by t1.sku_picking_orderline desc) / t2.total_picking_orderline <= 0.8,'A类',IF(sum(t1.sku_picking_orderline) over(partition by t1.cur_date,t1.project_code order by t1.sku_picking_orderline desc) / t2.total_picking_orderline <= 0.95,'B类','C类')) as class_type
  FROM t1
  INNER JOIN t2
  ON t1.cur_date = t2.cur_date AND t1.project_code = t2.project_code
  LEFT JOIN t3
  ON t1.cur_date = t3.cur_date AND t1.project_code = t3.project_code AND t1.sku_id = t3.sku_id
  INNER JOIN t4
  ON t1.cur_date = t4.cur_date AND t1.project_code = t4.project_code
)abc
GROUP BY abc.cur_date,abc.project_code,abc.class_type,abc.total_inventory_num,abc.total_picking_quantity,abc.total_inventory_quantity

UNION ALL 

SELECT '' as id, -- 主键
       tt1.cur_date, -- 统计日期
       tt1.project_code, -- 项目编码
       '未动销' as class_type, -- 动销分类
       tt1.total_inventory_num - tt2.abc_sku_num as sku_num, -- 未动销sku个数
       CAST((tt1.total_inventory_num - tt2.abc_sku_num) / tt1.total_inventory_num as decimal(10,3)) as sku_rate, -- 未动销sku个数占比
       0 as sku_picking_quantity, -- 未动销sku出库件数
       0 as picking_quantity_rate, -- 未动销sku出库件数占比
       tt1.total_inventory_quantity - tt2.sku_inventory_quantity as sku_inventory_quantity, -- 未动销sku库存件数
       CAST((tt1.total_inventory_quantity - tt2.sku_inventory_quantity) / tt1.total_inventory_quantity as decimal(10,3)) as inventory_quantity_rate, -- 未动销sku库存件数占比
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM
(
  SELECT cur_date,
         project_code,
         total_inventory_num,
         total_inventory_quantity
  FROM t4
)tt1
LEFT JOIN 
(
  SELECT abc.cur_date,
         abc.project_code,
         COUNT(DISTINCT abc.sku_id) as abc_sku_num,
         SUM(abc.sku_inventory_quantity) as sku_inventory_quantity 
  FROM 
  (
    SELECT t1.cur_date,
           t1.project_code,
           t1.sku_id,
           t3.sku_inventory_quantity,
           IF(sum(t1.sku_picking_orderline) over(partition by t1.cur_date,t1.project_code order by t1.sku_picking_orderline desc) / t2.total_picking_orderline <= 0.8,'A类',IF(sum(t1.sku_picking_orderline) over(partition by t1.cur_date,t1.project_code order by t1.sku_picking_orderline desc) / t2.total_picking_orderline <= 0.95,'B类','C类')) as class_type
    FROM t1
    INNER JOIN t2
    ON t1.cur_date = t2.cur_date AND t1.project_code = t2.project_code
    LEFT JOIN t3
    ON t1.cur_date = t3.cur_date AND t1.project_code = t3.project_code AND t1.sku_id = t3.sku_id
    INNER JOIN t4
    ON t1.cur_date = t4.cur_date AND t1.project_code = t4.project_code
  )abc
  GROUP BY abc.cur_date,abc.project_code
)tt2
ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash



#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_project_view_stock_count_detail.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo

