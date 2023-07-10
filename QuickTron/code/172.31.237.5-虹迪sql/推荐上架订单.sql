SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数' -- 单次进站完成货品件数
FROM (
SELECT  DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT ro.replenish_order_number)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wes_replenish.replenish_order ro
    JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
      ON ro.id = wpj.order_id
	WHERE ro.state = 'DONE' AND ro.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and ro.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')AND ro.project_code = 'C35052' AND wpj.project_code = 'C35052'
group BY DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct cwd.id) order_linenum, -- picking_work_detail行数
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wcs_g2p.w2p_guided_putaway_work_detail cwd
	JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
	  ON cwd.detail_id = wpj.detail_id
	WHERE cwd.quantity = cwd.fulfill_quantity AND wpj.state= 'DONE' AND cwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and cwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cwd.project_code = 'C35052' AND wpj.project_code = 'C35052'
	GROUP BY DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
       sum(j.fullfill_quantity) sku_num, 
	   0 'into_station_times'
	FROM evo_wcs_g2p.w2p_guided_put_away_job j
	WHERE j.state='DONE' AND j.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and j.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND j.project_code = 'C35052' 
	group BY DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
       count(se.id) into_station_times
    FROM evo_station.station_entry se
	WHERE se.biz_type LIKE '%GUIDED%' and se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'C35052' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT 
     tmp_line.ida times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
	FROM (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    GROUP BY tmp_line.ida
) tmp2 
GROUP BY tmp2.times