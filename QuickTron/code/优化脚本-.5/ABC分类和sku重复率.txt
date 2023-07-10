#sku重复率
select
a.so_date 日期, a.hours 小时, 
count(DISTINCT a.sku) 总sku数,
count(DISTINCT b.sku) 与上个小时比sku重复数,
count(DISTINCT a.sku)-count(DISTINCT b.sku) 新增sku数,
CONCAT(ROUND(count(DISTINCT b.sku)*100/count(DISTINCT a.sku),2),'%') 不变sku重复率,
CONCAT(ROUND((count(DISTINCT a.sku)-count(DISTINCT b.sku))*100/count(DISTINCT a.sku),2),'%') 新增sku重复率
FROM so a 
left join so b on a.hours=b.hours + 1 and a.so_date=b.so_date and a.sku=b.sku
GROUP BY a.so_date, a.hours;


# ABC分类--订单行
select SKUSS sku数_种,CONCAT(ROUND(SKUSS*100/s,2)) SKU占比_百分比, QTYSS 订单行数_行,pp 行数占比_百分数
from
(
SELECT *
from 
(
  SELECT count(e.sku) skuss, sum(e.sq) qtyss,e.pp  
  from 
  (
    select d.*,if(d.p<=0.8, 80, if(d.p<=0.95, 15, 5)) pp 
    from
    (
	    select a.sku,a.sq,(select sum(b.sq) from (select count(1) sq from so group by sku) b where a.sq<=b.sq)/c.ssq p 
	    from 
		  (
		    select sku,count(1) sq 
			  from so 
				-- where so_date = '2021-04-30'
			  group by sku
	  	) a 
	  	inner join 
	  	(
	  	  select count(1) ssq 
	  		from so
				-- where so_date = '2021-04-30'
	  	) c 
		  on 1=1 
	  	order by a.sq
	  ) d
  )e
  group by pp
)AA
inner join
(
  select SUM(SKUSS) S 
  from 
  (
	  select  count(sku) skuss, sum(sq) qtyss,pp  
		from 
		(
      select *,if(p<=0.8, 80, if(p<=0.95, 15, 5)) pp 
			from
			(
        select sku, sq,(select sum(b.sq) from (select count(1) sq from so group by sku) b where a.sq<=b.sq)/ssq p
        from 
				(
				  select sku,count(1) sq
          from so
					where so_date = '2021-04-30'
          group by sku
				) a
        inner join 
				( 
				  select count(1) ssq
          from so
					where so_date = '2021-04-30'
         ) c
				 on 1=1
         order by a.sq
			) d
	  ) e
  group by pp
  )AB
)t2 
on 1=1
)t
ORDER BY pp desc


# ABC分类--出货件数明细
select sku,sq sku对应的出货件数 ,lj 件数上下累加,  S 件数总值,CONCAT(ROUND(lj*100/s,2),'%') sku件数占比
from
(
  SELECT * 
	from
  (
	  select sku, sq,(select sum(b.sq) from (select sum(qty) sq from so group by sku) b where a.sq<=b.sq) lj
    from
	  (
      select sku,sum(qty) sq
      from so
			where so_date = '2021-04-30'
      group by sku
		) a
    order by a.sq desc
  )t1
INNER JOIN
(
select sum(sq) s
from 
  (
	select sku, sq,(select sum(b.sq) from (select sum(qty) sq from so group by sku) b where a.sq<=b.sq) lj
  from 
	  (
		select sku,sum(qty) sq 
		from so 
		where so_date = '2021-04-30'
		group by sku
		) a
    order by a.sq desc
  ) c
) t2 
on 1=1
)t

# ABC分类--出货行数明细
select sku,sq sku对应的出货行数 ,lj 行数上下累加,  S 行数总值,CONCAT(ROUND(lj*100/s,2),'%') sku行数占比
from
(
  select * 
  from 
  (
	  select sku, sq,(select sum(b.sq) from (select count(sku) sq from so group by sku) b where a.sq<=b.sq) lj
    from 
		(
      select sku, count(sku) sq
      from so
			where so_date = '2021-04-30'
      group by sku
		) a
    order by a.sq desc
  )t1
INNER JOIN
(
  select sum(sq) s 
	from 
	(
	  select sku, sq,(select sum(b.sq) from (select count(sku) sq from so group by sku) b where a.sq<=b.sq) lj
    from 
		(
      select sku, count(sku) sq
      from so
			where so_date = '2021-04-30'
      group by sku
		) a
    order by a.sq desc
	) c
) t2
on 1=1
)t