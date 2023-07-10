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
-- 动销SKU分布货架在货架总量占比 ads_sku_abc_bucket_rate 

INSERT overwrite table ${ads_dbname}.ads_sku_abc_bucket_rate
-- ads.ads_sku_abc_bucket_rate
-- 根据出库数量区分ABC分类:动销SKU分布货架在货架总量占比
SELECT ''                                                       as id,
       f1.d                                                     as cur_date, -- 统计日期
       f1.pt                                                    as project_code, -- 项目编码
       '出库数量'                                                  as abc_type,
       f1.abc_bucket_num,
       f2.total_bucket_num,
       f1.abc_bucket_num / f2.total_bucket_num as abc_bucket_rate,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM (
    SELECT d.d,d.pt,COUNT(DISTINCT e.bucket_code) as abc_bucket_num
    FROM
    (
    SELECT a.sku_id,a.\`sku出库数量\`,(SELECT sum(b.\`出库总数\`) FROM (SELECT sum(quantity) as \`出库总数\` FROM ${dwd_dbname}.dwd_picking_order_detail_info_di WHERE d = DATE_ADD(CURRENT_DATE(), -1)  GROUP BY sku_id,d,pt) b where a.\`sku出库数量\` <= b.\`出库总数\`) / c.\`出库总数\` as \`比例\`,a.d,a.pt
    FROM (
         SELECT sku_id,sum(quantity) as \`sku出库数量\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info_di
         WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
         GROUP BY sku_id,d,pt
         ) a
    INNER JOIN (
         SELECT sum(quantity) as \`出库总数\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info_di
         WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
         GROUP BY d,pt
         ) c 
    ON 1=1 and a.pt = c.pt
    ORDER BY a.\`sku出库数量\`
    ) d
    LEFT JOIN (
    SELECT a.sku_id,a.bucket_code,a.pt
    FROM ${dwd_dbname}.dwd_inventory_transaction_info_di a
    INNER JOIN (
                SELECT b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,max(b.transaction_created_time) as cd,b.pt
                FROM ${dwd_dbname}.dwd_inventory_transaction_info_di b
                INNER JOIN (
                           SELECT bucket_code,pt
                           FROM ${dwd_dbname}.dwd_basic_bucket_info_df
                           WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
                           ) c
                 ON b.bucket_code = c.bucket_code and b.pt = c.pt
                 WHERE b.inventory_level='LEVEL_THREE' and b.d = DATE_ADD(CURRENT_DATE(), -1)
                 GROUP BY b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,b.pt
                 ) b
     ON a.bucket_slot_code = b.bucket_slot_code and a.sku_id = b.sku_id and a.lot_id = b.lot_id and a.pack_id = b.pack_id and a.transaction_created_time = b.cd and a.pt = b.pt
     WHERE a.inventory_level='LEVEL_THREE' and a.post_quantity > 0 and a.d = DATE_ADD(CURRENT_DATE(), -1)
    )e
    ON d.sku_id = e.sku_id and d.pt = e.pt
    GROUP BY d.d,d.pt
)f1
LEFT JOIN (
    SELECT COUNT(DISTINCT bucket_code) as total_bucket_num,d,pt
    FROM ${dwd_dbname}.dwd_basic_bucket_info_df 
    WHERE d = DATE_ADD(CURRENT_DATE(), -1)
    GROUP BY d,pt
)f2
ON f1.d = f2.d and f1.pt = f2.pt

UNION ALL 

-- ads.ads_sku_abc_bucket_rate
-- 根据订单行数区分ABC分类：动销SKU分布货架在货架总量占比
SELECT ''                                                       as id,
       f1.d                                                     as cur_date, -- 统计日期
       f1.pt                                                    as project_code, -- 项目编码
       '订单行数'                                                  as abc_type,
       f1.abc_bucket_num,
       f2.total_bucket_num,
       f1.abc_bucket_num / f2.total_bucket_num as abc_bucket_rate,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM (
    SELECT d.d,d.pt,COUNT(DISTINCT e.bucket_code) as abc_bucket_num
    FROM 
    (
    SELECT a.sku_id,a.\`sku订单行数\`,(SELECT sum(b.\`订单行数\`) FROM (SELECT count(1) as \`订单行数\` FROM ${dwd_dbname}.dwd_picking_order_detail_info_di WHERE d = DATE_ADD(CURRENT_DATE(), -1)  GROUP BY sku_id,d,pt) b WHERE a.\`sku订单行数\` <= b.\`订单行数\`)/c.\`订单行数\` as \`比例\`,a.d,a.pt
    FROM (
         SELECT sku_id,count(1) as \`sku订单行数\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info_di
         WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
         GROUP BY sku_id,d,pt
         ) a
    INNER JOIN (
         SELECT count(1) as \`订单行数\`,d,pt
         FROM ${dwd_dbname}.dwd_picking_order_detail_info_di
         WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
         GROUP BY d,pt
         ) c 
    ON 1=1 and a.pt = c.pt
    ORDER BY a.\`sku订单行数\`
    ) d
    LEFT JOIN (
    SELECT a.sku_id,a.bucket_code,a.pt
    FROM ${dwd_dbname}.dwd_inventory_transaction_info_di a
    INNER JOIN (
                SELECT b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,max(b.transaction_created_time) as cd,b.pt
                FROM ${dwd_dbname}.dwd_inventory_transaction_info_di b
                INNER JOIN (
                           SELECT bucket_code,pt
                           FROM ${dwd_dbname}.dwd_basic_bucket_info_df
                           WHERE d = DATE_ADD(CURRENT_DATE(), -1) 
                           ) c
                 ON b.bucket_code = c.bucket_code and b.pt = c.pt
                 WHERE b.inventory_level='LEVEL_THREE' and b.d = DATE_ADD(CURRENT_DATE(), -1) 
                 GROUP BY b.bucket_slot_code,b.sku_id,b.lot_id,b.pack_id,b.pt
                 ) b
     ON a.bucket_slot_code = b.bucket_slot_code and a.sku_id = b.sku_id and a.lot_id = b.lot_id and a.pack_id = b.pack_id and a.transaction_created_time = b.cd and a.pt = b.pt
     WHERE a.inventory_level='LEVEL_THREE' and a.post_quantity > 0 and a.d = DATE_ADD(CURRENT_DATE(), -1) 
    )e
    ON d.sku_id = e.sku_id and d.pt = e.pt
    GROUP BY d.d,d.pt
)f1
LEFT JOIN (
    SELECT COUNT(DISTINCT bucket_code) as total_bucket_num,d,pt
    FROM ${dwd_dbname}.dwd_basic_bucket_info_df 
    WHERE d = DATE_ADD(CURRENT_DATE(), -1)
    GROUP BY d,pt
)f2
ON f1.d = f2.d and f1.pt = f2.pt;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"