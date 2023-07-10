SELECT 
  tmp2.times as '时间段',
  SUM(tmp2.done_jobtask_num) as '完成作业任务数',
  SUM(tmp2.nodone_jobtask_num) as '未完成作业任务数'
FROM
(
   SELECT DATE_FORMAT(jsc.updated_date, '%Y-%m-%d %H:00:00') times,
          COUNT(DISTINCT jsc.job_id) done_jobtask_num,
          0 'nodone_jobtask_num'
   FROM evo_wcs_g2p.job_state_change jsc
   WHERE jsc.project_code = 'A51274' AND jsc.state = 'DONE' AND jsc.job_type != 'SI_QUICK_PICK' AND jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')
   group BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')

   UNION ALL 

   SELECT tt.theDayStartofhour times,
          0 'done_jobtask_num',
          count(distinct case when jsc.created_date <= theDayEndofhour then jsc.job_id end) - count(distinct case when jsc.updated_date <= theDayEndofhour then jsc.job_id end) as 'nodone_jobtask_num'
   FROM (
        SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
        FROM information_schema.COLUMNS,(select @i:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
        WHERE @i < DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)  
        )tt
   JOIN evo_wcs_g2p.job_state_change jsc
   WHERE jsc.project_code = 'A51274' AND jsc.state = 'DONE' AND jsc.job_type != 'SI_QUICK_PICK' AND jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')
   group by TIMESTAMPDIFF(HOUR,DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tt.theDayStartofhour)   
       
   UNION ALL 

   SELECT tmp1.ids times,
          0 'done_jobtask_num',
          0 'nodone_jobtask_num'
   FROM
   (SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) ids 
    FROM information_schema.COLUMNS,(select @i:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp
    WHERE @i < DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp1
   GROUP BY tmp1.ids
) tmp2
GROUP BY tmp2.times