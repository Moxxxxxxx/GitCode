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
     cast(SUM(tmp2.time) as decimal(10,2)) as '平均人工盘点耗时/秒' -- '平均人工盘点耗时/秒'
FROM (
SELECT DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     ccd.station_code, 
     IF(sum(ccd.actual_quantity) is not null,sum(ccd.actual_quantity),0) sku_num,
	   0 'into_station_times', 
     0 'station_used',
     0 'station_busy',
     0 'time'
FROM evo_wes_cyclecount.cycle_count_detail ccd
WHERE ccd.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and ccd.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')  AND ccd.station_code != ' ' AND ccd.station_code is not null  AND ccd.project_code = 'C35052' 
group BY DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00'),ccd.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       cj.station_code,
       0 'sku_num', 
       0 'into_station_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.w2p_countcheck_work_detail_v2 pwd
    LEFT JOIN evo_wcs_g2p.w2p_countcheck_job_v2 cj
    ON pwd.id = cj.detail_id
	WHERE pwd.state= 'DONE'  AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'C35052' AND cj.project_code = 'C35052'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),cj.station_code

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
	WHERE se.biz_type LIKE '%CYCLE%' and entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'C35052'
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
      SELECT DISTINCT tt.station_code,tt.entry_time,tt.exit_time
      FROM
      (
      SELECT 
      b.station_code,
      case when a.entry_time >= b.entry_time AND a.entry_time <= b.exit_time THEN b.entry_time 
          when a.entry_time > b.exit_time AND a.exit_time is not NULL THEN a.entry_time 
          when a.entry_time < b.entry_time THEN a.entry_time
          when a.entry_time > b.exit_time AND a.exit_time is null THEN b.entry_time
          when a.entry_time is null THEN b.entry_time
          when b.entry_time is null THEN a.entry_time END as entry_time,
      case when a.exit_time >= b.entry_time AND a.exit_time <= b.exit_time THEN b.exit_time
          when a.exit_time > b.exit_time THEN a.exit_time
          when a.exit_time < b.entry_time THEN a.exit_time 
          when a.exit_time is null THEN b.exit_time
          when a.exit_time is null THEN DATE_FORMAT(DATE_ADD(a.entry_time,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00.000')
          when b.exit_time is null THEN a.exit_time END as exit_time
      FROM
      (
SELECT
se.station_code,se.entry_time,se.exit_time
FROM 
(
SELECT
tmp.station_code,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) entry_time,tmp.exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
ORDER BY seq.station_code,seq.entry_time
)tmp
union all
SELECT
tmp.station_code,tmp.entry_time,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
AND DATE_FORMAT(seq.entry_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(seq.exit_time,'%Y-%m-%d %H:00:00')
ORDER BY seq.station_code,seq.entry_time
)tmp
)se
LEFT JOIN
(
SELECT
tmp.station_code,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) entry_time,tmp.exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
ORDER BY seq.station_code,seq.entry_time
)tmp
union all
SELECT
tmp.station_code,tmp.entry_time,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
AND DATE_FORMAT(seq.entry_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(seq.exit_time,'%Y-%m-%d %H:00:00')
ORDER BY seq.station_code,seq.entry_time
)tmp
)tt
ON se.station_code = tt.station_code AND ((se.entry_time >= tt.entry_time AND se.exit_time < tt.exit_time) OR (se.exit_time >= tt.entry_time AND se.exit_time <= tt.exit_time))
WHERE tt.entry_time is NULL
)a
RIGHT JOIN
(
SELECT
tmp.station_code,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),MIN(tmp.entry_time)) as entry_time,MAX(tmp.exit_time1) as exit_time
FROM 
(
SELECT
se.station_code,se.entry_time,se.exit_time,tt.entry_time as entry_time1,tt.exit_time as exit_time1
FROM
(
SELECT
tmp.station_code,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) entry_time,tmp.exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
ORDER BY seq.station_code,seq.entry_time
)tmp
union all
SELECT
tmp.station_code,tmp.entry_time,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
AND DATE_FORMAT(seq.entry_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(seq.exit_time,'%Y-%m-%d %H:00:00')
ORDER BY seq.station_code,seq.entry_time
)tmp
)
se
LEFT JOIN
(
SELECT
se.station_code,se.entry_time,se.exit_time
FROM 
(
SELECT
tmp.station_code,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) entry_time,tmp.exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
ORDER BY seq.station_code,seq.entry_time
)tmp
union all
SELECT
tmp.station_code,tmp.entry_time,IF(DATE_FORMAT(tmp.entry_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00.000'),tmp.entry_time) exit_time
FROM
(
SELECT 
seq.station_code,seq.entry_time,seq.exit_time
FROM evo_station.station_entry seq
WHERE 
((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
AND seq.biz_type LIKE '%CYCLE%' AND seq.project_code = 'C35052' 
AND DATE_FORMAT(seq.entry_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(seq.exit_time,'%Y-%m-%d %H:00:00')
ORDER BY seq.station_code,seq.entry_time
)tmp 
)se
)tt
ON se.station_code = tt.station_code AND ((se.entry_time >= tt.entry_time AND se.exit_time < tt.exit_time) OR (se.exit_time >= tt.entry_time AND se.exit_time <= tt.exit_time))
WHERE tt.entry_time is not NULL 
ORDER BY se.station_code,se.entry_time
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.exit_time,'%Y-%m-%d %H:00:00')
)b
ON a.station_code = b.station_code 
ORDER BY a.station_code,a.entry_time
)tt
)seq,
	    (SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @i:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @i < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
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
       AND sl.biz_type LIKE '%CYCLECOUNT%' AND sl.project_code = 'C35052' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
   0 'order_linenum',
	 cj.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(cj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON jsc.job_id = cj.job_id
LEFT JOIN
(
SELECT cj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
LEFT JOIN evo_station.station_task t
ON sc.station_task_id = t.id
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON t.task_no = cj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'C35052' AND t.project_code = 'C35052' AND cj.project_code = 'C35052' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'C35052'  AND cj.project_code = 'C35052' 
GROUP BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00'),cj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code