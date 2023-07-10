#!/bin/bash

#############################################################
# 效率指标概览
eff_index_sql="
-- 效率指标概览
SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

INSERT INTO smart_report.eff_index(time,order_num,order_linenum,sku_num,station_slot_times,into_station_times,win_open_times,once_win_open_times,once_picking_quantity,once_station_slot_times,once_order_linenum,project_code)
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
"

###############################################################
# 效率指标详表
eff_index_time_sql="
-- 效率指标详表
SET @interval_time =60;
SET @line_num =24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

INSERT INTO smart_report.eff_index_time(time,station_code,into_station_times,order_linenum,sku_num,station_slot_times,win_open_times,once_win_open_times,once_picking_quantity,once_station_slot_times,once_order_linenum,station_used_rate,station_busy_rate,station_online_rate,picking_time,project_code)
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
     tmp2.station_code as 'station_code', -- 工作站
     SUM(tmp2.into_station_times) as 'into_station_times', -- 进站次数
     SUM(tmp2.order_linenum) as 'order_linenum', -- 完成订单行数
     SUM(tmp2.sku_num) as 'sku_num', -- 完成货品件数
	   SUM(tmp2.station_slot_times) as 'station_slot_times', -- 命中槽位次数
     SUM(tmp2.win_open_times) as 'win_open_times', -- 弹窗次数
     cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0),0)) as decimal(10,2)) as 'once_win_open_times', -- 单次进站弹窗次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_picking_quantity', -- 单次进站完成货品件数
     cast((if(SUM(tmp2.win_open_times)!=0,ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0),0))as decimal(10,2)) as 'once_station_slot_times', -- 单次弹窗命中槽位次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_order_linenum', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as 'station_used_rate', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as 'station_busy_rate', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as 'station_online_rate', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as 'picking_time', -- '平均人工拣货耗时/秒'
     'A51264' AS project_code
FROM (
SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60) times,
     0 'order_linenum',
     pj.station_code, 
   sum(pj.actual_quantity) sku_num,
   count(pj.station_slot_code) station_slot_times,
	   0 'into_station_times', 
	 count(distinct stg.group_job_id) win_open_times,
     0 'station_used',
     0 'station_busy',
     0 'time'
	FROM evo_wcs_g2p.station_task_group stg 
	 JOIN evo_wcs_g2p.picking_job pj 
	 ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= @begin_time and stg.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND pj.project_code = 'A51264' AND stg.project_code = 'A51264'
	group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60),pj.station_code

UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60) times,
       count(distinct pwd.id) order_linenum,
       pj.station_code,
       0 'sku_num', 
       0 'station_slot_times',
       0 'into_station_times',
       0 'win_open_times',
       0 'station_used',
       0 'station_busy',
     0 'time'
    FROM evo_wcs_g2p.picking_work_detail pwd
	 JOIN evo_wcs_g2p.picking_job pj
	 ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE' and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND pwd.project_code = 'A51264' AND pj.project_code = 'A51264'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60),pj.station_code

UNION ALL

SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.entry_time)/@interval_time/60) times,
     0 'order_linenum',
     se.station_code,
     0 'sku_num',
     0 'station_slot_times',
     count(se.id) into_station_times,
     0 'win_open_times',
       0 'station_used',
       0 'station_busy',
     0 'time'
    FROM evo_station.station_entry se
	WHERE idempotent_id LIKE '%G2PPicking%' and se.entry_time >= @begin_time AND se.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND se.project_code = 'A51264'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.entry_time)/@interval_time/60),se.station_code

UNION ALL

SELECT 
   tmp1.ids times,
   0 'order_linenum',
	 tmp1.station_code,
   0 'sku_num',
	 0 'station_slot_times',
	 0 'into_station_times', 
   0 'win_open_times',
SUM(		
			CASE WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineBegin_time
				  ELSE 0 END
			) station_used,
    0 'station_busy',
     0 'time'
	FROM (
	  SELECT 
		tmp_line.ids,
		seq.station_code,
    TIMESTAMPDIFF(SECOND, @begin_time,if(seq.entry_time <= @begin_time,@begin_time,seq.entry_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, @begin_time,if(seq.exit_time is null,DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE),if(seq.exit_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE),seq.exit_time,DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time*(tmp_line.ids-1) MINUTE)) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time*tmp_line.ids MINUTE)) 'begin_to_lineEnd_time'
    FROM evo_station.station_entry seq,
	    (SELECT @ids:=@ids+1 ids FROM information_schema.COLUMNS,(select @ids:=0) tmp WHERE  @ids <@line_num) tmp_line
    WHERE 
       ((seq.entry_time >= @begin_time and seq.entry_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
       OR (seq.exit_time >= @begin_time and seq.exit_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
		   OR (seq.entry_time <= @begin_time and seq.exit_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
       AND seq.idempotent_id LIKE '%G2PPicking%' AND seq.project_code = 'A51264'
    ) tmp1
    GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times1,
   0 'order_linenum',
	 tt1.station_code station_code1,
   0 'sku_num',
	 0 'station_slot_times', 
	 0 'into_station_times',
	 0 'win_open_times',
   0 'station_used',
     SUM(		
			CASE WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time then tt1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time then tt1.begin_to_lineBegin_time
				  ELSE 0 END
			) station_busy,
     0 'time'
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, @begin_time,if(sl.login_time <= @begin_time,@begin_time,sl.login_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, @begin_time,if(sl.logout_time is null,DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE),if(sl.logout_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE),sl.logout_time,DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time*(tmp_line.ida-1) MINUTE)) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time*tmp_line.ida MINUTE)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	    (SELECT @ida:=@ida+1 ida FROM information_schema.COLUMNS,(select @ida:=0) tmp WHERE  @ida <@line_num) tmp_line
    WHERE 
       ((sl.login_time >= @begin_time and sl.login_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
       OR (sl.logout_time >= @begin_time and sl.logout_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
		   OR (sl.login_time <= @begin_time and sl.logout_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
       AND sl.biz_type = 'PICKING_ONLINE_G2P_B2P' AND sl.project_code = 'A51264' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   ceil(TIMESTAMPDIFF(SECOND, @begin_time,jsc.updated_date)/@interval_time/60) times,
   0 'order_linenum',
	 pj.station_code,
   0 'sku_num',
	 0 'station_slot_times', 
	 0 'into_station_times',
	 0 'win_open_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(pj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.picking_job pj
ON jsc.job_id = pj.job_id
JOIN
(
SELECT pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.station_task_group g
ON t.task_no = g.group_job_id
JOIN evo_wcs_g2p.picking_job pj
ON g.job_id = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN @begin_time AND DATE_ADD(@begin_time,INTERVAL 1 DAY) AND sc.project_code = 'A51264' AND t.project_code = 'A51264' AND g.project_code = 'A51264' AND pj.project_code = 'A51264'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= @begin_time AND jsc.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND jsc.state = 'DONE' AND jsc.project_code = 'A51264'  AND pj.project_code = 'A51264'
GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,jsc.updated_date)/@interval_time/60),pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code
"

###############################################################
# 效率指标概览
station_free_sql="
-- 工作台空闲率
SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

INSERT INTO smart_report.station_free(station_code,\`00:00\`,\`01:00\`,\`02:00\`,\`03:00\`,\`04:00\`,\`05:00\`,\`06:00\`,\`07:00\`,\`08:00\`,\`09:00\`,\`10:00\`,\`11:00\`,\`12:00\`,\`13:00\`,\`14:00\`,\`15:00\`,\`16:00\`,\`17:00\`,\`18:00\`,\`19:00\`,\`20:00\`,\`21:00\`,\`22:00\`,\`23:00\`,project_code)

SELECT 
    seq.station_code AS station_code,
    cast((@interval_time * 60 - 
	    sum(CASE WHEN seq.entry_time >= @begin_time  
    	AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE)
	    AND seq.exit_time < DATE_ADD(@begin_time , INTERVAL @interval_time MINUTE) 
    	THEN timestampdiff(SECOND,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= @begin_time  AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE))
		ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '00:00',
		
    cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
    	AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE)
      	AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE)
    	THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '01:00',
		
    cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
	    AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
     	AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
    	THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE)
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE)
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '02:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
	    AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '03:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE)
	    AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE)
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '04:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
	    AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '05:00',
		
    cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
	    AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*6 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '06:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
     	AND seq.entry_time < DATE_ADD(@begin_time , INTERVAL @interval_time*8 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*7 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '07:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
     	AND seq.entry_time < DATE_ADD(@begin_time , INTERVAL @interval_time*9 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*8 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '08:00',
		
    cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
     	AND seq.entry_time < DATE_ADD(@begin_time , INTERVAL @interval_time*10 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*9 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '09:00',
		
	cast((@interval_time*60-
	    sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
     	AND seq.entry_time < DATE_ADD(@begin_time , INTERVAL @interval_time*11 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE)
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE)
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*10 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '10:00',
		
	cast((@interval_time*60-
		sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE)
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*11 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '11:00',
		
	cast((@interval_time*60-
		sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*12 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '12:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*13 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '13:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*14 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '14:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*15 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '15:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time* 16 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*16 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '16:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*17 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '17:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*18 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '18:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*19 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '19:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*20 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '20:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*21 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '21:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*22 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '22:00',
		
	cast((@interval_time*60
		-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE)
		THEN timestampdiff(second,seq.entry_time,seq.exit_time)
        WHEN seq.entry_time >= DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE))
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		AND seq.exit_time>= DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE)
		AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE),seq.exit_time)
        WHEN seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE) 
		AND seq.entry_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE)
		AND seq.exit_time > DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE) 
		THEN timestampdiff(second,DATE_ADD(@begin_time ,INTERVAL @interval_time*23 MINUTE),DATE_ADD(@begin_time ,INTERVAL @interval_time*24 MINUTE))
        ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '23:00'
    ,'A51264' as project_code
		
FROM evo_station.station_entry seq

WHERE seq.entry_time >= @begin_time  
  AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE)
  AND seq.idempotent_id LIKE '%G2PPicking%' 
  AND seq.project_code = 'A51264'
  
GROUP BY seq.station_code
"

# MYSQL
HOSTNAME="192.168.1.70"                                         #数据库信息
PORT="3306"
USERNAME="smart_report"
PASSWORD="XAynvGMEwlg"
DBNAME="smart_report"                                           #数据库名称

# 删除重复数据
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "delete from smart_report.eff_index where date(created_date) = date(sysdate()) AND project_code = 'A51264';"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "delete from smart_report.eff_index_time where date(created_date) = date(sysdate()) AND project_code = 'A51264';"
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "delete from smart_report.station_free where date(created_date) = date(sysdate()) AND project_code = 'A51264';"
echo "delete ok"

#插入数据
# 效率指标概览
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${eff_index_sql}"
echo "eff_index_sql ok"

# 效率指标详表
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${eff_index_time_sql}"
echo "eff_index_time_sql ok"

# 效率指标概览
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${station_free_sql}"
echo "station_free_sql ok"

exit 0
