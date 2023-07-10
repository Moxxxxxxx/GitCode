-- 效率指标概览
SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

-- INSERT INTO smart_reporter.eff_index(time,order_num,order_linenum,sku_num,station_slot_times,into_station_times,win_open_times,once_win_open_times,once_picking_quantity,once_station_slot_times,once_order_linenum,project_code)
SELECT  CASE WHEN tmp2.times=1 THEN '00:00'
             WHEN tmp2.times=2 THEN '01:00'
             WHEN tmp2.times=3 THEN '02:00'
             WHEN tmp2.times=4 THEN '03:00'
             WHEN tmp2.times=5 THEN '04:00'
             WHEN tmp2.times=6 THEN '05:00'
             WHEN tmp2.times=7 THEN '06:00'
             WHEN tmp2.times=8 THEN '07:00'
             WHEN tmp2.times=9 THEN '08:00'
             WHEN tmp2.times=10 THEN '09:00'
             WHEN tmp2.times=11 THEN '10:00'
             WHEN tmp2.times=12 THEN '11:00'
             WHEN tmp2.times=13 THEN '12:00'
             WHEN tmp2.times=14 THEN '13:00'
             WHEN tmp2.times=15 THEN '14:00'
             WHEN tmp2.times=16 THEN '15:00'
             WHEN tmp2.times=17 THEN '16:00'
             WHEN tmp2.times=18 THEN '17:00'
             WHEN tmp2.times=19 THEN '18:00'
             WHEN tmp2.times=20 THEN '19:00'
             WHEN tmp2.times=21 THEN '20:00'
             WHEN tmp2.times=22 THEN '21:00'
             WHEN tmp2.times=23 THEN '22:00'
             WHEN tmp2.times=24 THEN '23:00'
             END as 'time', -- 时间段
     SUM(tmp2.order_num) as 'order_num', -- 订单完成数
     SUM(tmp2.order_linenum) as 'order_linenum', -- 完成订单行数
     SUM(tmp2.sku_num) as 'sku_num', -- 完成货品件数
  	 SUM(tmp2.station_slot_times) as 'station_slot_times', -- 命中槽位次数
     SUM(tmp2.into_station_times) as 'into_station_times', -- 进站次数
     SUM(tmp2.win_open_times) as 'win_open_times', -- 弹窗次数
     cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0),0)) as decimal(10,2)) as 'once_win_open_times', -- 单次进站弹窗次数
	 cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_picking_quantity', -- 单次进站完成货品件数
     cast((if(SUM(tmp2.win_open_times)!=0,ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0),0))as decimal(10,2)) as 'once_station_slot_times', -- 单次弹窗命中槽位次数
	 cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_order_linenum', -- 单次进站完成订单行数
     'A51264' as project_code
FROM (
SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60) times,
     0 order_num,
     0 order_linenum,
      sum(pj.actual_quantity) sku_num, -- picking_job实捡数量
      count(pj.station_slot_code) station_slot_times, -- 工作站槽位code
	 0 into_station_times, -- 进出站时间
	 count(distinct stg.group_job_id) win_open_times -- 任务组任务id
	FROM evo_wcs_g2p.station_task_group stg 
	JOIN evo_wcs_g2p.picking_job pj 
      ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= @begin_time and stg.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND pj.project_code = 'A51264' AND stg.project_code = 'A51264'
	group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60)

UNION ALL

SELECT  ceil(TIMESTAMPDIFF(SECOND, @begin_time, pw.updated_date)/@interval_time/60) times,
	   COUNT(DISTINCT pw.order_id)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num', 
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.picking_work pw
	WHERE pw.state = 'DONE' AND pw.updated_date >= @begin_time and pw.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num  MINUTE) AND pw.project_code = 'A51264'
    group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time, pw.updated_date)/@interval_time/60)

UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60) times,
     0 'order_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num',
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.picking_work_detail pwd
	JOIN evo_wcs_g2p.picking_job pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE'  AND pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND pwd.project_code = 'A51264' AND pj.project_code = 'A51264'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60)
	  
UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.entry_time)/@interval_time/60) times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'station_slot_times',
       count(se.id) into_station_times,
     0 'win_open_times'
    FROM evo_station.station_entry se
	WHERE idempotent_id LIKE '%G2PPicking%' and se.entry_time >= @begin_time AND se.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND se.project_code = 'A51264'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.entry_time)/@interval_time/60)

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