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
-- 根据订单行数区分ABC分类：货品数量及货架分布 ads_sku_ABC_linenum_distribution 

INSERT overwrite table ${ads_dbname}.ads_sku_ABC_linenum_distribution
SELECT ''                                                      as id,
       d.d                                                     as cur_date, -- 统计日期
       d.pt                                                    as project_code, -- 项目编码
       d.sku_id                                                as sku_id, -- sku编码
       if(e.bucket_num is null,0,e.bucket_num)                 as bucket_num, -- 货架分布数量
       d.\`sku订单行数\`                                            as sku_orderline_num, -- sku订单行数
       if(d.\`比例\` <= 0.8,'A', if(d.\`比例\` <= 0.95, 'B', 'C'))    as class_level, -- 货品分类等级
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM(
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
    SELECT a.sku_id,count(distinct a.bucket_code) as bucket_num,a.pt
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
     GROUP BY a.sku_id,a.pt
    )e
ON d.sku_id = e.sku_id and d.pt = e.pt
ORDER BY d.pt,d.\`比例\`;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"