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
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_picking_quantity', -- 单次进站完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as 'once_order_linenum', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as 'station_used_rate', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as 'station_busy_rate', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as 'station_online_rate', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as 'cyclecount_time', -- '平均人工盘点耗时/秒'
	   'A51118' as project_code
FROM (
SELECT DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     w.station_code, 
     sum(rod.fulfill_quantity) sku_num,
	   0 'into_station_times', 
     0 'station_used',
     0 'station_busy',
     0 'time'
FROM evo_wes_replenish.replenish_order_detail rod
LEFT JOIN evo_wes_replenish.replenish_work w
ON rod.replenish_order_id = w.source_order_id 
WHERE rod.quantity = rod.fulfill_quantity AND rod.last_updated_date >= @begin_time and rod.last_updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND w.replenish_mode LIKE '%DIRECT%' AND rod.project_code = 'A51118' AND w.project_code = 'A51118' 
group BY DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00'),w.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       pwd.station_code,
       0 'sku_num', 
       0 'into_station_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.putaway_work_detail pwd
	WHERE pwd.state= 'DONE' AND pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND pwd.project_code = 'A51118'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),pwd.station_code

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     se.station_code,
     0 'sku_num',
     count(se.id) into_station_times,
     0 'station_used',
     0 'station_busy',
     0 'time'
    FROM evo_station.station_entry se
	WHERE se.biz_type LIKE '%DIRECT%' and se.entry_time >= @begin_time AND se.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND se.project_code = 'A51118'
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00'),se.station_code

UNION ALL

SELECT 
   tmp1.ids times,
   0 'order_linenum',
	 tmp1.station_code,
   0 'sku_num',
	 0 'into_station_times',
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
       AND seq.biz_type LIKE '%DIRECT%' AND seq.project_code = 'A51118' 
        ) tmp1
    GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times,
   0 'order_linenum',
	 tt1.station_code,
   0 'sku_num',
	 0 'into_station_times',
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
       AND sl.biz_type LIKE '%DIRECT%' AND sl.project_code = 'A51118' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
   0 'order_linenum',
	 pj.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(pj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
LEFT JOIN evo_wcs_g2p.putaway_job pj
ON jsc.job_id = pj.job_id
LEFT JOIN
(
SELECT pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
LEFT JOIN evo_station.station_task t
ON sc.station_task_id = t.id
LEFT JOIN evo_wcs_g2p.putaway_job pj
ON t.task_no = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN @begin_time AND DATE_ADD(@begin_time,INTERVAL 1 DAY) AND sc.project_code = 'A51118' AND t.project_code = 'A51118' AND pj.project_code = 'A51118' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= @begin_time AND jsc.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) AND jsc.state = 'DONE' AND jsc.project_code = 'A51118'  AND pj.project_code = 'A51118' 
GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,jsc.updated_date)/@interval_time/60),pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code