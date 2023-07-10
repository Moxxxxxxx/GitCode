SELECT  
     tmp2.times as '时间段', -- 时间段
     tmp2.station_code as '工作站', -- 工作站
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as '工作站利用率', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as '工作站繁忙率', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as '工作站在线率', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as '平均人工上架耗时/秒' -- '平均人工上架耗时/秒'
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
WHERE rod.quantity = rod.fulfill_quantity AND rod.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and rod.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND w.replenish_mode LIKE '%GUIDED%' AND rod.project_code = 'C35052' AND w.project_code = 'C35052' 
group BY DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00'),w.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       j.station_code,
       0 'sku_num', 
       0 'into_station_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.w2p_guided_putaway_work_detail pwd
    LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job  j
    ON pwd.detail_id = j.detail_id
	WHERE j.state= 'DONE' AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'C35052' AND j.project_code = 'C35052'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),j.station_code

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
	WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' and se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'C35052'
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
    TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),seq.entry_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.exit_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),seq.exit_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ids) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ids,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM (
SELECT a.station_code,
       IF(a.hjk = 1,a.entry_time1,MIN(a.entry_time2)) as entry_time,
       IF(a.hjk = 1,a.exit_time1,MAX(a.exit_time2)) as exit_time
       
FROM
(
SELECT tt1.station_code1 as station_code,tt1.rn1,tt1.entry_time1,tt1.exit_time1,tt1.rn2,tt1.entry_time2,tt1.exit_time2,tt1.cf1,tt2.cf2,
       case when tt1.rn1 = 1 AND tt1.cf1 = 1 then 1
            when tt1.cf1 <= tt2.cf2 AND tt1.cf1 = 0 then @i:=@i 
            when tt1.cf1 != tt2.cf2 then @i := @i + 1 + tt2.cf2 
            end as hjk
FROM
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t1:=@t1+1) as cf1
FROM
(
SELECT (@rn1:=@rn1+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn1:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'C35052' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn2:=@rn2+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn2:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'C35052' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t1:=0)tmp1
)tt1
JOIN
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t2:=@t2+1) as cf2
FROM
(
SELECT (@rn3:=@rn3+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn3:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'C35052' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn4:=@rn4+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn4:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'C35052' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t2:=0)tmp1
)tt2
ON tt1.station_code1 = tt2.station_code1 AND tt1.rn1 = tt2.rn1 - 1
JOIN
(SELECT @i:=0)tmp
)a
GROUP BY a.station_code,a.hjk
ORDER BY a.station_code,`entry_time`
)seq,
	    (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
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
    TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),sl.login_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.logout_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),sl.logout_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((sl.login_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.login_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
       OR (sl.logout_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
		   OR (sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
       AND sl.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND sl.project_code = 'C35052' 
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
LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job pj
ON jsc.job_id = pj.job_id
LEFT JOIN
(
SELECT pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
LEFT JOIN evo_station.station_task t
ON sc.station_task_id = t.id
LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job pj
ON t.task_no = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'C35052' AND t.project_code = 'C35052' AND pj.project_code = 'C35052' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'C35052'  AND pj.project_code = 'C35052' 
GROUP BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00'),pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code