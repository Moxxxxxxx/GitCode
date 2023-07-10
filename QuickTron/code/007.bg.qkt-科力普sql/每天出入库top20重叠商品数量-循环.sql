SELECT a.order_date as '日期', count(1) as '重叠数量'
FROM 
  (SELECT t.order_date, t.sku_id, t.sku_code, t.sku_name 
   FROM 
      -- 每天出库TOP N商品
     (SELECT c.rownum, 'picking' as order_type, c.order_date, c.sku_id, s.sku_code, s.sku_name, c.qty 
      FROM 
        (SELECT if(@date = a.order_date, (@rownum := @rownum + 1), (@rownum := 1)) as rownum, a.*, (@date := a.order_date) as date2 
         FROM 
           (SELECT DATE_FORMAT(t.created_date, '%Y-%m-%d') as order_date, t.sku_id, sum(t.quantity) as qty 
            FROM evo_wes_picking.picking_order_fulfill_detail t
            WHERE 1=1
              AND t.created_date BETWEEN '2021-06-01' AND '2021-07-01'
              AND t.project_code = 'A51118'
            GROUP BY DATE_FORMAT(t.created_date, '%Y-%m-%d'), t.sku_id
            ORDER BY order_date desc, qty desc
            )a, 
            (SELECT @rownum := 0, @date := ' ')b
        ) c 
      LEFT JOIN evo_wes_basic.basic_sku s 
      ON c.sku_id = s.id
      WHERE c.rownum <= 20

     UNION ALL
     -- 每天入库TOP N商品
     SELECT c.rownum, 'replenish' as order_type, c.order_date, c.sku_id, s.sku_code, s.sku_name, c.qty 
     FROM 
       (SELECT if(@date = a.order_date, (@rownum := @rownum + 1), (@rownum := 1)) as rownum, a.*, (@date := a.order_date) as date2 
        FROM 
          (SELECT DATE_FORMAT(t.created_date, '%Y-%m-%d') as order_date, t.sku_id, sum(t.fulfill_quantity) as qty 
           FROM evo_wes_replenish.replenish_order_fulfill_detail t
           WHERE 1=1
             AND t.created_date BETWEEN '2021-06-01' AND '2021-07-01'
             AND t.project_code = 'A51118'
           GROUP BY DATE_FORMAT(t.created_date, '%Y-%m-%d'), t.sku_id
           ORDER BY order_date desc, qty desc
          )a, 
          (select @rownum := 0, @date := ' ')b
      )c 
      LEFT JOIN evo_wes_basic.basic_sku s 
      ON c.sku_id = s.id
      WHERE c.rownum <= 20
  )t
GROUP BY t.order_date, t.sku_code 
HAVING count(1) > 1
)a 
GROUP BY a.order_date;