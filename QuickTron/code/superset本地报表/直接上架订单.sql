SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.nodone_order_linenum) as '待完成订单行',
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数' -- 单次进站完成货品件数
FROM (
SELECT DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT ro.id)order_num,-- replenish_order行数
     0 'order_linenum',
     0 'sku_num', -- putaway_order实捡数量
     0 'into_station_times',
     0 'nodone_order_linenum'
FROM evo_wes_replenish.replenish_order ro
LEFT JOIN evo_wcs_g2p.putaway_work w
ON ro.id = w.order_id 
WHERE ro.state = 'DONE' AND ro.last_updated_date >= @begin_time and ro.last_updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num  MINUTE) -- AND ro.project_code = 'A51118'
group BY DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   0 'order_num',-- picking_order行数
     0 'order_linenum',
     sum(rod.fulfill_quantity) sku_num, -- replenish_order_detail实捡数量
     0 'into_station_times',
     0 'nodone_order_linenum'
FROM evo_wes_replenish.replenish_order_detail rod
LEFT JOIN evo_wcs_g2p.putaway_work w
ON rod.replenish_order_id = w.order_id 
WHERE rod.quantity = rod.fulfill_quantity AND rod.last_updated_date >= @begin_time and rod.last_updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num  MINUTE) -- AND rod.project_code = 'A51118' 
group BY DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct pwd.id) order_linenum,-- putaway_work_detail行数
     0 'sku_num',
     0 'into_station_times',
     0 'nodone_order_linenum'
   FROM evo_wcs_g2p.putaway_work_detail pwd
	WHERE pwd.state= 'DONE'  AND pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND pwd.project_code = 'A51118'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
       count(se.id) into_station_times,
     0 'nodone_order_linenum'
    FROM evo_station.station_entry se
	WHERE se.biz_type LIKE '%DIRECT%' and se.entry_time >= @begin_time AND se.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND se.project_code = 'A51118' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')
	  
UNION ALL

SELECT 
     tt.theDayStartofhour times,
     0 'order_num',
     0 'order_linenum',
	   0 'sku_num',
     0 'into_station_times',
     count(distinct case when pwd.created_date <= theDayEndofhour then pwd.id end) - count(distinct case when pwd.updated_date <= theDayEndofhour then pwd.id end) as nodone_order_linenum -- T-1所处时间段累计未完成的订单行
FROM (
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tt
join evo_wcs_g2p.putaway_work_detail pwd
left join
(
 SELECT *
 FROM 
 (
  SELECT pwd.project_code,pwd.id,pwd.updated_date
  FROM evo_wcs_g2p.putaway_work pw 
  left join evo_wcs_g2p.putaway_work_detail pwd 
  on pwd.work_id = pw.work_id and pw.project_code = pwd.project_code
  where 1 = 1  -- AND ro.project_code = 'C35052' 
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pw.state in ('CANCEL_DONE', 'DONE') -- 完成或取消的
  UNION ALL    
  SELECT pwd.project_code,pwd.id,pwd.updated_date
  FROM evo_wcs_g2p.putaway_work_detail pwd 
  left join evo_wcs_g2p.putaway_job pj
  on pj.detail_id = pwd.id and pj.project_code = pwd.project_code
  where 1 = 1  -- AND ro.project_code = 'C35052' 
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pj.state = 'DONE'
 )t
 group by t.project_code,t.id
)t1
on t1.project_code = pwd.project_code and t1.id = pwd.id
where 1 = 1  -- AND ro.project_code = 'C35052' 
and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY)
group by pwd.project_code,TIMESTAMPDIFF(HOUR,@begin_time,tt.theDayStartofhour)

UNION ALL

SELECT 
     tmp1.ids times,
     0 'order_num',
     0 'order_linenum',
	   0 'sku_num',
     0 'into_station_times',
     0 'nodone_order_linenum'
	FROM (
       SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) ids
       FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
       WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR) 
    ) tmp1
    GROUP BY tmp1.ids
) tmp2 
GROUP BY tmp2.times