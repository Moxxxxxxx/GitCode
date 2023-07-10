-- 根据出库数量区分ABC分类：货品数量及货架分布 ads_sku_ABC_checkout_distribution 

INSERT overwrite table ${ads_dbname}.ads_sku_ABC_checkout_distribution
SELECT ''                                                      as id,
       d.d                                                     as cur_date, -- 统计日期
       d.pt                                                    as project_code, -- 项目编码
       d.sku_id                                                as sku_id, -- sku编码
       if(e.bucket_num is null,0,e.bucket_num)                 as bucket_num, -- 货架分布数量
       d.\`sku出库数量\`                                            as sku_num, -- sku出库数量
       if(d.\`比例\` <= 0.8, 'A', if(d.\`比例\` <= 0.95, 'B', 'C'))   as class_level, -- 货品分类等级
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM(
    SELECT a.sku_id,a.\`sku出库数量\`,(SELECT sum(b.\`出库总数\`) FROM (SELECT sum(quantity) as \`出库总数\` FROM ${dwd_dbname}.dwd_picking_order_detail_info WHERE d = DATE_ADD(CURRENT_DATE(), -1)  GROUP BY sku_id,d,pt) b where a.\`sku出库数量\` <= b.\`出库总数\`) / c.\`出库总数\` as \`比例\`,a.d,a.pt
    FROM (
         SELECT sku_id,sum(quantity) as \`sku出库数量\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info
         WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
         GROUP BY sku_id,d,pt
         ) a
    INNER JOIN (
         SELECT sum(quantity) as \`出库总数\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info
         WHERE d = DATE_ADD(CURRENT_DATE(), -1)
         GROUP BY d,pt
         ) c 
    ON 1=1 and a.pt = c.pt
    ORDER BY a.\`sku出库数量\`
    ) d
LEFT JOIN (
    SELECT a.sku_id,count(distinct a.bucket_code) as bucket_num,a.pt
    FROM ${dwd_dbname}.dwd_inventory_transaction_info a
    INNER JOIN (
                SELECT b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,max(b.transaction_created_time) as cd,b.pt
                FROM ${dwd_dbname}.dwd_inventory_transaction_info b
                INNER JOIN (
                           SELECT bucket_code,pt
                           FROM ${dwd_dbname}.dwd_basic_bucket_info
                           WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
                           ) c
                 ON b.bucket_code = c.bucket_code and b.pt = c.pt
                 WHERE b.inventory_level='LEVEL_THREE' and b.d = DATE_ADD(CURRENT_DATE(), -1) 
                 GROUP BY b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,b.pt
                 ) b
     ON a.bucket_slot_code = b.bucket_slot_code and a.sku_id = b.sku_id and a.lot_id = b.lot_id and a.pack_id = b.pack_id and a.transaction_created_time = b.cd and a.pt = b.pt
     WHERE a.inventory_level='LEVEL_THREE' and a.post_quantity > 0 and a.d = DATE_ADD(CURRENT_DATE(), -1) 
     GROUP BY a.sku_id,a.pt
    )e
ON d.sku_id = e.sku_id and d.pt = e.pt
ORDER BY d.pt,d.\`比例\`;