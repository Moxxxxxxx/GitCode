-- 库存统计
-- 货架数、货位数
SELECT t1.days as cur_date, -- 统计日期
       t2.project_code, -- 项目编码
       nvl(t3.bucket_num_total,0) as bucket_num_total, -- 货架总数
       nvl(t3.slot_num_total,0) as slot_num_total, -- 货位总数
       nvl(t3.slot_num_actual,0) as slot_num_actual, -- 货位占用量（剔除零库存）
       nvl(t3.slot_using_rate,0) as slot_using_rate, -- 货位占用率（剔除零库存）
       nvl(t4.sku_num_total,0) as sku_num_total, -- sku总数（个）
       nvl(t3.sku_num_actual,0) as sku_num_actual, -- sku在库数量（个）
       nvl(t3.quantity_total,0) as quantity_total, -- 库存总数（件）
       nvl(t3.inventory_depth,0) as inventory_depth -- 平均库存深度（件）
FROM ${dim_dbname}.dim_day_date t1
LEFT JOIN ${dim_dbname}.dim_project_dict t2
LEFT JOIN 
(
  SELECT bb.d as cur_date, -- 统计日期
         bb.pt as project_code, -- 项目编码
         COUNT(DISTINCT bb.bucket_code) as bucket_num_total, -- 货架总数
         COUNT(DISTINCT sb.slot_code) as slot_num_total, -- 货位总数
         COUNT(DISTINCT li.bucket_slot_code) as slot_num_actual, -- 货位占用量
         CAST(COUNT(DISTINCT li.bucket_slot_code) / COUNT(DISTINCT sb.slot_code) as decimal(10,2)) as slot_using_rate, -- 货位占用率
         SUM(li.quantity) as quantity_total, -- 库存总量
         CAST(SUM(li.quantity) / COUNT(DISTINCT li.bucket_slot_code) as decimal(10,2)) as inventory_depth, -- 库存深度
         COUNT(DISTINCT li.sku_id) as sku_num_actual -- sku在库数量
  FROM ${dwd_dbname}.dwd_basic_bucket_base_info_df bb
  LEFT JOIN ${dwd_dbname}.dwd_basic_slot_base_info_df sb
  ON bb.d = sb.d AND bb.pt = sb.pt AND bb.id = sb.bucket_id AND sb.slot_state = 'effective'
  LEFT JOIN ${dwd_dbname}.dwd_inventory_level3_inventory_info_df li
  ON bb.d = li.d AND bb.pt = li.pt AND bb.bucket_code = li.bucket_code AND sb.slot_code = li.bucket_slot_code AND li.quantity != 0 -- 剔除零库存
  WHERE bb.d = DATE_ADD(CURRENT_DATE(), -1) AND bb.pt = '${project_code}' AND bb.bucket_state = 'effective' AND bb.bucket_type_id = '57' -- 正常货架
  GROUP BY bb.d,bb.pt
)t3
ON t1.days = t3.cur_date AND t2.project_code = t3.project_code
LEFT JOIN 
(
  SELECT b.d as cur_date, -- 统计日期
         b.pt as project_code, -- 项目编码
         COUNT(DISTINCT b.id) as sku_num_total -- sku总数（个）
  FROM ${dwd_dbname}.dwd_wes_basic_sku_info_df b
  WHERE b.d = DATE_ADD(CURRENT_DATE(), -1) AND b.pt = '${project_code}' AND b.sku_state = 'effective' -- 状态有效
  GROUP BY b.d,b.pt
)t4
ON t1.days = t4.cur_date AND t2.project_code = t4.project_code
WHERE t1.days = DATE_ADD(CURRENT_DATE(), -1) AND t2.project_code = '${project_code}';

-- 动销数据
with t1 as (
  SELECT TO_DATE(p.order_updated_time) as cur_date,
         p.pt as project_code,
         p.sku_id,
         COUNT(DISTINCT p.id) as sku_picking_orderline,
         SUM(p.fulfill_quantity) as sku_picking_quantity
  FROM ${dwd_dbname}.dwd_picking_order_detail_info p
  WHERE TO_DATE(p.order_updated_time) = DATE_ADD(CURRENT_DATE(), -1) AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.pt = '${project_code}' AND p.quantity = p.fulfill_quantity -- 订单已完成
  GROUP BY TO_DATE(p.order_updated_time),p.pt,p.sku_id
  ORDER BY sku_picking_orderline desc
),
t2 as
(
  SELECT TO_DATE(p.order_updated_time) as cur_date,
         p.pt as project_code,
         COUNT(DISTINCT p.id) as total_picking_orderline,
         SUM(p.fulfill_quantity) as total_picking_quantity
  FROM ${dwd_dbname}.dwd_picking_order_detail_info p
  WHERE TO_DATE(p.order_updated_time) = DATE_ADD(CURRENT_DATE(), -1) AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.pt = '${project_code}' AND p.quantity = p.fulfill_quantity -- 订单已完成
  GROUP BY TO_DATE(p.order_updated_time),p.pt
),
t3 as 
(
  SELECT TO_DATE(bb.d) as cur_date, -- 统计日期
         bb.pt as project_code, -- 项目编码
         li.sku_id, -- sku
         SUM(li.quantity) as sku_inventory_quantity
  FROM ${dwd_dbname}.dwd_basic_bucket_base_info_df bb
  LEFT JOIN ${dwd_dbname}.dwd_basic_slot_base_info_df sb
  ON bb.d = sb.d AND bb.pt = sb.pt AND bb.id = sb.bucket_id AND sb.slot_state = 'effective'
  LEFT JOIN ${dwd_dbname}.dwd_inventory_level3_inventory_info_df li
  ON bb.d = li.d AND bb.pt = li.pt AND bb.bucket_code = li.bucket_code AND sb.slot_code = li.bucket_slot_code AND li.quantity != 0 -- 剔除零库存
  WHERE bb.d = DATE_ADD(CURRENT_DATE(), -1) AND bb.pt = '${project_code}' AND bb.bucket_state = 'effective' AND bb.bucket_type_id = '57' -- 正常货架
    AND li.sku_id is not null
  GROUP BY bb.d,bb.pt,li.sku_id
),