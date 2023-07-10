-- 效率指标详表
SET @interval_time =60;
SET @line_num =24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 07:00:00'),INTERVAL -1 DAY);

SELECT  
     tmp2.times as 'time', -- 时间段
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
	   'A51118' as project_code
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
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
	WHERE pj.state='DONE' AND stg.updated_date >= @begin_time and stg.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND pj.project_code = 'A51118' AND stg.project_code = 'A51118'
	group BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60),pj.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
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
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE' and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND pwd.project_code = 'A51118' AND pj.project_code = 'A51118'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,pwd.updated_date)/@interval_time/60),pj.station_code

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
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
	WHERE idempotent_id LIKE '%G2PPicking%' and entry_time >= @begin_time AND entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) -- AND se.project_code = 'A51118'
	GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,se.last_updated_date)/@interval_time/60),se.station_code

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
		TIMESTAMPDIFF(SECOND, @begin_time,tmp_line.ids) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(tmp_line.ids,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_entry seq,
	    (SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
       WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((seq.entry_time >= @begin_time and seq.entry_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
       OR (seq.exit_time >= @begin_time and seq.exit_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
		   OR (seq.entry_time <= @begin_time and seq.exit_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
       AND seq.idempotent_id LIKE '%G2PPicking%' -- AND seq.project_code = 'A51118' 
        ) tmp1
    GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times,
   0 'order_linenum',
	 tt1.station_code,
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
		TIMESTAMPDIFF(SECOND, @begin_time,tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, @begin_time,DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((sl.login_time >= @begin_time and sl.login_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
       OR (sl.logout_time >= @begin_time and sl.logout_time <= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
		   OR (sl.login_time <= @begin_time and sl.logout_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
       AND sl.biz_type = 'PICKING_ONLINE_G2P_B2P' -- AND sl.project_code = 'A51118' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
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
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN @begin_time AND DATE_ADD(@begin_time,INTERVAL 1 DAY) -- AND sc.project_code = 'A51118' AND t.project_code = 'A51118' AND g.project_code = 'A51118' AND pj.project_code = 'A51118' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= @begin_time AND jsc.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND jsc.state = 'DONE' -- AND jsc.project_code = 'A51118'  AND pj.project_code = 'A51118' 
GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,jsc.updated_date)/@interval_time/60),pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code