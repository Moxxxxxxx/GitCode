SET @interval_time = 60;
SET @line_num = 24;
SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 07:00:00'),INTERVAL -1 DAY);

SELECT 
    seq.station_code AS station_code,
		tmp.theDayStartofhour,
    cast((@interval_time*60-
	  sum(CASE WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,seq.exit_time)
             WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,tmp.theDayEndofhour)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time >= tmp.theDayStartofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,seq.exit_time)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,tmp.theDayEndofhour)
             ELSE 0 END))/(@interval_time*60)as decimal(10,2)) as '空闲率'
FROM (
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tmp
join evo_station.station_entry seq
WHERE seq.entry_time >= @begin_time  AND seq.exit_time < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) AND idempotent_id LIKE '%G2PPicking%'  AND project_code = 'A51118'
GROUP BY seq.station_code,tmp.theDayStartofhour