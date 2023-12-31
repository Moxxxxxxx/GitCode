###############################sheet20 库存信息
-- 存储层数

select substr(a.last_updated_date,1,10) as date,
sum(a.post_quantity) as `当日库存件数`,
count(distinct a.bucket_slot_code) as `当日库存货位`,
count(distinct a.bucket_code) as `当日库存货架`,
count(distinct a.sku_id) as `当日库存SKU数`
from 
(select bucket_slot_code,bucket_code ,last_updated_date,sku_id,post_quantity
from  evo_wes_inventory.inventory_transaction a
where warehouse_id='1' 
and inventory_level='LEVEL_THREE' 
and substr(last_updated_date,1,10)>='2021-11-01'
and substr(last_updated_date,1,10)<='2022-01-13'
and sku_id is not null
group by  bucket_slot_code,sku_id
order by last_updated_date desc
)a
inner join 
(
select bucket_code,bucket_type_id
from evo_basic.basic_bucket
where bucket_type_id in (117,118) and zone_id=8
)b 
on (a.bucket_code=b.bucket_code)
group by substr(a.last_updated_date,1,10);
--  存储层数

select 
bucket_type_id, 
level_1,
count(distinct bucket_slot_code)as slot_cnt, 
count(distinct bucket_code) as bucket_cnt

from (
select a.sku_id,a.bucket_slot_code,a.bucket_code,
substring_index(substring_index(a.bucket_slot_code,'-',-2),'-',1) as level_1,
bucket_type_id
from evo_wes_inventory.inventory_transaction a
inner join (
select bucket_slot_code,sku_id,lot_id,pack_id,max(created_date) cd,bucket_type_id
from evo_wes_inventory.inventory_transaction b
inner join 
(
select bucket_code,bucket_type_id
from evo_basic.basic_bucket
where bucket_type_id in (117,118) 
and zone_id=8
) c 
on b.bucket_code=c.bucket_code
where inventory_level='LEVEL_THREE'
and substr(created_date,1,10)<='2022-01-13'
and substr(created_date,1,10)>='2021-11-01'
group by bucket_slot_code,sku_id,lot_id,pack_id
) b
on a.bucket_slot_code=b.bucket_slot_code and a.sku_id=b.sku_id
and a.lot_id=b.lot_id and a.pack_id=b.pack_id and a.created_date=b.cd
where a.inventory_level='LEVEL_THREE'
and a.post_quantity>0


) aa
group by bucket_type_id, 
level_1;

--  命中层数
select
bucket_type_id,
date_format(last_updated_date, '%Y-%m') as month,
substring_index(substring_index(bucket_slot_code, '-', -2), '-', 1) '层数',
count(distinct job_id) as '命中任务数'
from evo_wes_picking.picking_order_fulfill_detail a
inner join (select * from evo_basic.basic_bucket
where bucket_type_id in (117,118)) b on a.bucket_code=b.bucket_code
where bucket_slot_code is not null
and substr(last_updated_date,1,10)>='2021-11-01'
and substr(last_updated_date,1,10)<='2022-01-13'
group by date_format(last_updated_date, '%Y-%m'),
substring_index(substring_index(bucket_slot_code, '-', -2), '-', 1),
bucket_type_id;
-- SKU存储分布

select bucket_type_id, bucket_count, count(sku_id) 'sku_count'
from (
select a.sku_id,count(distinct a.bucket_code) 'bucket_count',bucket_type_id
from evo_wes_inventory.inventory_transaction a
inner join (
select bucket_slot_code,sku_id,lot_id,pack_id,max(created_date) cd,bucket_type_id
from evo_wes_inventory.inventory_transaction b
inner join (select bucket_code,bucket_type_id
from evo_basic.basic_bucket
where bucket_type_id in (117,118) and zone_id=8) c
 on b.bucket_code=c.bucket_code
where inventory_level='LEVEL_THREE'
and substr(created_date, 1,10)<='2022-01-13'
and substr(created_date,1,10)>='2021-11-01'
group by bucket_slot_code,sku_id,lot_id,pack_id
) b
on a.bucket_slot_code=b.bucket_slot_code and a.sku_id=b.sku_id
and a.lot_id=b.lot_id and a.pack_id=b.pack_id and a.created_date=b.cd
where a.inventory_level='LEVEL_THREE'
and a.post_quantity>0
group by a.sku_id, bucket_type_id) c
group by bucket_count,bucket_type_id;--  每个货架上货位的占用率
select 
aa.bucket_code as `货架`,
aa.bucket_type_id as `货架类型编号`,
sum(aa.hw_fulfil_cnt) as `货位数`,
round(sum(aa.hw_fulfil_cnt)/hw_cnt*100,2) as `货位占用率百分比`

from 
(
select
a.bucket_code,
bucket_type_id,
bucket_slot_code,
count(distinct a.bucket_slot_code) as hw_fulfil_cnt,
case when bucket_type_id='117' then 20 else 50 end as hw_cnt 
from
evo_wes_inventory.level3_inventory a
INNER join
evo_basic.basic_bucket b
on a.bucket_code=b.bucket_code
where b.bucket_type_id in (117,118)
GROUP BY a.bucket_slot_code,
bucket_type_id,
bucket_slot_code,
case when bucket_type_id='117' then 20 else 50 end
)aa
group by aa.bucket_code ,
aa.bucket_type_id;