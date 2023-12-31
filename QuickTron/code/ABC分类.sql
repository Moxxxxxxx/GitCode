
    
# ABC分类--订单行
worksheet = workbook.add_worksheet('ABC分类_订单行')
print('ABC分类_订单行')
sql = '''
select
SKUSS sku数_种,CONCAT(ROUND(SKUSS*100/s,2)) SKU占比_百分比, QTYSS 订单行数_行,pp as `行数占比_百分数`

from
(
select * from
(SELECT * from (
select  count(sku_id) skuss, sum(sq) qtyss,pp  from (
select *,if(p<=0.8, 80, if(p<=0.95, 15, 5)) pp from(
select sku_id, sq,
(select sum(b.sq) from (
select count(1) sq
from evo_wes_picking.picking_order_detail
group by sku_id) b where a.sq<=b.sq)/ssq p
       from (
select sku_id,count(1) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
inner join (select count(1) ssq
from evo_wes_picking.picking_order_detail
    ) c on 1=1
order by a.sq) d) e
group by pp
)AA

) t1
inner join
(select SUM(SKUSS) S from 
(select  count(sku_id) skuss, sum(sq) qtyss,pp  from (
select *,if(p<=0.8, 80, if(p<=0.95, 15, 5)) pp from(
select sku_id, sq,
(select sum(b.sq) from (
select count(1) sq
from evo_wes_picking.picking_order_detail
group by sku_id) b where a.sq<=b.sq)/ssq p
       from (
select sku_id,count(1) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
inner join (select count(1) ssq
from evo_wes_picking.picking_order_detail
    ) c on 1=1
order by a.sq) d) e
group by pp
)AB

) t2 
on 1=1
)t
ORDER BY pp desc;
'''

ABC_order_QTYSS_count = pd.read_sql(sql, engine)
worksheet.write_row(0, 0, ABC_order_QTYSS_count.columns)
start_row = 0
for i, column in enumerate(ABC_order_QTYSS_count.columns, start=0):
    worksheet.write_column(start_row + 1, i, ABC_order_QTYSS_count[column])






# ！ABC分类--出库件数
worksheet = workbook.add_worksheet('ABC分类_出库件数')
print('ABC分类_出库件数')
sql = '''
select
SKUSS sku数,CONCAT(ROUND(SKUSS*100/s,2)) SKU占比, QTYSS 出库件数,pp 件数占比

from
(
select * from
(SELECT * from (
select  count(sku_id) skuss, sum(sq) qtyss,pp  from (
select *,if(p<=0.8, '80', if(p<=0.95, '95', '100')) pp from(
select sku_id, sq,
(select sum(b.sq) from (
select sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) b where a.sq<=b.sq)/ssq p
       from (
select sku_id,sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
inner join (select sum(quantity) ssq
from evo_wes_picking.picking_order_detail
    ) c on 1=1
order by a.sq) d) e
group by pp
)AA

) t1
inner join
(select SUM(SKUSS) S from 
(select  count(sku_id) skuss, sum(sq) qtyss,pp  from (
select *,if(p<=0.8, '80', if(p<=0.95, '15', '5')) pp from(
select sku_id, sq,
(select sum(b.sq) from (
select sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) b where a.sq<=b.sq)/ssq p
       from (
select sku_id,sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
inner join (select sum(quantity) ssq
from evo_wes_picking.picking_order_detail
    ) c on 1=1
order by a.sq) d) e
group by pp
)AB

) t2 
on 1=1
)t
;'''    
a1 = pd.read_sql(sql, engine)
worksheet.write_row(0, 0, a1.columns)
start_row = 0
for i, column in enumerate(a1.columns, start=0):
    worksheet.write_column(start_row + 1, i, a1[column])
    

# ABC分类--出货件数明细
worksheet = workbook.add_worksheet('ABC分类_出货件数明细')
print('ABC分类_出货件数明细')
sql = '''
select sku_id,sq sku对应的出货件数 ,lj 件数上下累加,  S 件数总值,CONCAT(ROUND(lj*100/s,2)) sku件数占比
from
(SELECT
* from
(select * 
from 
(select sku_id, sq,
(select sum(b.sq) 
from (
select sum(quantity) sq
from 
evo_wes_picking.picking_order_detail
group by sku_id) b 
where a.sq<=b.sq) lj
       from (
select sku_id,sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
order by a.sq desc
)  d )t1
INNER JOIN
(select sum(sq) s from (select sku_id, sq,
(select sum(b.sq) 
from (
select sum(quantity) sq
from 
evo_wes_picking.picking_order_detail
group by sku_id) b 
where a.sq<=b.sq) lj
       from (
select sku_id,sum(quantity) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
order by a.sq desc) c) t2 on 1=1
)t;
'''
b1 = pd.read_sql(sql, engine)
worksheet.write_row(0, 0, b1.columns)
start_row = 0
for i, column in enumerate(b1.columns, start=0):
    worksheet.write_column(start_row + 1, i, b1[column])


# ABC分类--出货行数明细
worksheet = workbook.add_worksheet('ABC分类_出货行数明细')
print('ABC分类_出货行数明细')
sql = '''
select sku_id,sq sku对应的出货行数 ,lj 行数上下累加,  S 行数总值,CONCAT(ROUND(lj*100/s,2)) sku行数占比
from
(SELECT
* from
(select * 
from 
(select sku_id, sq,
(select sum(b.sq) 
from (
select count(sku_id)  sq
from 
evo_wes_picking.picking_order_detail
group by sku_id) b 
where a.sq<=b.sq) lj
       from (
select sku_id, count(sku_id) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
order by a.sq desc
)  d )t1
INNER JOIN
(select sum(sq) s from (select sku_id, sq,
(select sum(b.sq) 
from (
select count(sku_id) sq
from 
evo_wes_picking.picking_order_detail
group by sku_id) b 
where a.sq<=b.sq) lj
       from (
select sku_id, count(sku_id) sq
from evo_wes_picking.picking_order_detail
group by sku_id) a
order by a.sq desc) c) t2 on 1=1
)t
;'''
c1 = pd.read_sql(sql, engine)
worksheet.write_row(0, 0, c1.columns)
start_row = 0
for i, column in enumerate(c1.columns, start=0):
    worksheet.write_column(start_row + 1, i, c1[column])
