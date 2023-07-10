SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

SELECT tmp2.times as 'time', -- 时间段
     SUM(tmp2.order_num) as 'order_num', -- 订单完成数
     SUM(tmp2.order_linenum) as 'order_linenum', -- 完成订单行数
     SUM(tmp2.nodone_order_linenum) as 'nodone_order_linenum',
     SUM(tmp2.sku_num) as 'sku_num', -- 完成货品件数
  	 SUM(tmp2.station_slot_times) as 'station_slot_times', -- 命中槽位次数
     SUM(tmp2.into_station_times) as 'into_station_times', -- 进站次数
     SUM(tmp2.win_open_times) as 'win_open_times', -- 弹窗次数
     cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0),0)) as decimal(10,2)) as 'once_win_open_times', -- 单次进站弹窗次数
	 cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_picking_quantity', -- 单次进站完成货品件数
     cast((if(SUM(tmp2.win_open_times)!=0,ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0),0))as decimal(10,2)) as 'once_station_slot_times', -- 单次弹窗命中槽位次数
	 cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_order_linenum', -- 单次进站完成订单行数
	 'A51118' as project_code
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
     0 order_num,
     0 order_linenum,
      sum(pj.actual_quantity) sku_num, -- picking_job实捡数量
      count(pj.station_slot_code) station_slot_times, -- 工作站槽位code
	 0 into_station_times, -- 进出站时间
	 count(distinct stg.group_job_id) win_open_times, -- 任务组任务id
     0 'nodone_order_linenum'
	FROM evo_wcs_g2p.station_task_group stg 
	JOIN evo_wcs_g2p.picking_job pj 
      ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date between @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND pj.project_code = 'A51118' AND stg.project_code = 'A51118'
	group BY DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(po.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT po.id)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num', 
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times',
     0 'nodone_order_linenum'
    FROM evo_wes_picking.picking_order po
	WHERE po.state = 'DONE' AND po.last_updated_date between @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num  MINUTE) -- AND po.project_code = 'A51118'
group BY DATE_FORMAT(po.last_updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num',
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times',
     0 'nodone_order_linenum'
    FROM evo_wcs_g2p.picking_work_detail pwd
	JOIN evo_wcs_g2p.picking_job pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE'  AND pwd.updated_date BETWEEN @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND pwd.project_code = 'A51118' AND pj.project_code = 'A51118'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') 
	  
UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'station_slot_times',
       count(se.id) into_station_times,
     0 'win_open_times',
     0 'nodone_order_linenum'
    FROM evo_station.station_entry se
	WHERE idempotent_id LIKE '%G2PPicking%' and entry_time >= @begin_time AND entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND se.project_code = 'A51118'
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT 
     tt.theDayStartofhour times,
     0 'order_num',
     0 'order_linenum',
	 0 'sku_num',
	 0 'station_slot_times',
     0 'into_station_times', 
     0 'win_open_times',
     count(distinct case when pwd.created_date <= theDayEndofhour then pwd.picking_work_detail_id end) - count(distinct case when pwd.updated_date <= theDayEndofhour then pwd.picking_work_detail_id end) as nodone_order_linenum -- T-1所处时间段累计未完成的订单行
FROM (
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tt
join evo_wcs_g2p.picking_work_detail pwd
left join
(
 SELECT *
 FROM 
 (
  SELECT pwd.project_code,pwd.picking_work_detail_id,pwd.updated_date
  FROM evo_wcs_g2p.picking_work pw 
  left join evo_wcs_g2p.picking_work_detail pwd 
  on pwd.picking_work_id = pw.picking_work_id and pw.project_code = pwd.project_code
  where 1 = 1  -- AND ro.project_code = 'C35052' 
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pw.state in ('CANCEL_DONE', 'DONE') -- 完成或取消的
  UNION ALL    
  SELECT pwd.project_code,pwd.picking_work_detail_id,pwd.updated_date
  FROM evo_wcs_g2p.picking_work_detail pwd 
  left join evo_wcs_g2p.picking_job pj
  on pj.picking_work_detail_id = pwd.picking_work_detail_id and pj.project_code = pwd.project_code
  where 1 = 1  -- AND ro.project_code = 'C35052' 
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pwd.quantity = pwd.fulfill_quantity and pj.state = 'DONE'
 )t
 group by t.project_code,t.picking_work_detail_id
)t1
on t1.project_code = pwd.project_code and t1.picking_work_detail_id = pwd.picking_work_detail_id
where 1 = 1  -- AND ro.project_code = 'C35052' 
and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY)
group by pwd.project_code,TIMESTAMPDIFF(HOUR,@begin_time,tt.theDayStartofhour)

UNION ALL

SELECT 
     tmp1.ids times,
     0 'order_num',
     0 'order_linenum',
	 0 'sku_num',
	 0 'station_slot_times',
     0 'into_station_times', 
     0 'win_open_times',
     0 'nodone_order_linenum'
	FROM (
       SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
       WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
    ) tmp1
    GROUP BY tmp1.ids
) tmp2 
GROUP BY tmp2.times