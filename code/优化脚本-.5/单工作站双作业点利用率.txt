SELECT a.station_code,a.*,if(COUNT(a.hjk) = 1,a.single_entry,MIN(a.repeat_entry)) as entry_time,if(COUNT(a.hjk) = 1,a.single_exit,MAX(a.repeat_exit)) as exit_time
FROM
(
SELECT tt1.station_code1 as station_code,tt1.rn1,tt1.entry_time1,tt1.rn2,tt1.exit_time1,tt1.entry_time2,tt1.exit_time2,tt1.cf,tt2.rn2,tt2.entry_time2,tt2.exit_time2,tt2.cf,case when tt1.cf = tt2.cf then @i:=@i when tt1.cf != tt2.cf then @i := @i + 1 + tt2.cf end as hjk
FROM
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t1:=@t1+1) as cf
FROM
(
SELECT (@rn1:=@rn1+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn1:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'C35052' AND se.entry_time >= '2021-12-22 00:00:00' AND se.entry_time <= '2021-12-23 00:00:00'
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn2:=@rn2+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn2:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'C35052' AND se.entry_time >= '2021-12-22 00:00:00' AND se.entry_time <= '2021-12-23 00:00:00' 
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t1:=0)tmp1
)tt1
JOIN
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t2:=@t2+1) as cf
FROM
(
SELECT (@rn3:=@rn3+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn3:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'C35052' AND se.entry_time >= '2021-12-22 00:00:00' AND se.entry_time <= '2021-12-23 00:00:00'
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn4:=@rn4+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn4:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'C35052' AND se.entry_time >= '2021-12-22 00:00:00' AND se.entry_time <= '2021-12-23 00:00:00'
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