-- 效率指标概览
SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = '2021-07-21 07:00:00';
SELECT 
     tmp2.times as 'time', -- 时间段
     SUM(tmp2.order_num) as 'order_num', -- 订单完成数
     SUM(tmp2.order_linenum) as 'order_linenum', -- 完成订单行数
     SUM(tmp2.sku_num) as 'sku_num', -- 完成货品件数
	 SUM(tmp2.station_slot_times) as 'station_slot_times', -- 命中槽位次数
     SUM(tmp2.into_station_times) as 'into_station_times', -- 进站次数
     SUM(tmp2.win_open_times) as 'win_open_times', -- 弹窗次数
     cast(ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as 'once_win_open_times', -- 单次进站弹窗次数
	 cast(ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as 'once_picking_quantity', -- 单次进站完成货品件数
     cast(ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0)as decimal(10,2)) as 'once_station_slot_times', -- 单次弹窗命中槽位次数
	 cast(ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as 'once_order_linenum' -- 单次进站完成订单行数
FROM (
SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60) times,
     0 order_num,
     0 order_linenum,
      sum(pj.actual_quantity) sku_num, -- picking_job实捡数量
      count(pj.station_slot_code) station_slot_times, -- 工作站槽位code
	 0 into_station_times, -- 进出站时间
	 count(distinct stg.group_job_id) win_open_times -- 任务组任务id
	FROM evo_wcs_g2p.wcs_station_task_group stg 
	JOIN evo_wcs_g2p.wcs_picking_job pj 
      ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date between @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)
	group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60)

UNION ALL

SELECT  ceil(TIMESTAMPDIFF(SECOND, @begin_time, po.last_updated_date)/@interval_time/60) times,
	   COUNT(DISTINCT po.id)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num', 
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wes_picking.picking_order po
	WHERE po.state = 'DONE' AND po.last_updated_date between @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num  MINUTE) AND po.project_code = 'A51118'
group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time, po.last_updated_date)/@interval_time/60)

UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60) times,
     0 'order_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num',
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.wcs_picking_work_detail pwd
	JOIN evo_wcs_g2p.wcs_picking_job pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE'  AND pwd.updated_date BETWEEN @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60)

UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.last_updated_date)/@interval_time/60) times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'station_slot_times',
       count(se.id) into_station_times,
     0 'win_open_times'
    FROM evo_station.evo_station_entry se
	WHERE idempotent_id LIKE '%G2PPicking%' and entry_time >= @begin_time AND entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.last_updated_date)/@interval_time/60)
	  
UNION ALL

SELECT 
     tmp1.ids times,
     0 'order_num',
     0 'order_linenum',
	 0 'sku_num',
	 0 'station_slot_times',
     0 'into_station_times', 
     0 'win_open_times'
	FROM (
       SELECT @ids:=@ids+1 ids FROM information_schema.COLUMNS,(select @ids:=0) tmp WHERE  @ids <@line_num
    ) tmp1
    GROUP BY tmp1.ids
) tmp2 
GROUP BY tmp2.times