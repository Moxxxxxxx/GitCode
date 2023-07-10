SELECT 
  tmp2.times as '时间段',
  SUM(tmp2.done_job_num) as '完成作业单数',
  SUM(tmp2.nodone_job_linenum) as '未完成作业单数'
FROM
(
   SELECT DATE_FORMAT(jsc.updated_date, '%Y-%m-%d %H:00:00') times,
          COUNT(DISTINCT brj.job_id) done_job_num,
          0 'nodone_job_linenum'
   FROM evo_wcs_g2p.bucket_robot_job brj 
   LEFT JOIN evo_wcs_g2p.job_state_change jsc
   ON brj.job_id = jsc.job_id
   WHERE jsc.project_code = 'A51274' AND jsc.state = 'DONE' AND jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')
   group BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')

   UNION ALL 

   SELECT tt.theDayStartofhour times,
          0 'done_job_num',
          count(distinct case when brj.created_date <= theDayEndofhour then brj.job_id end) - count(distinct case when brj.updated_date <= theDayEndofhour then brj.job_id end) as 'nodone_job_linenum'
   FROM (
        SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
        FROM information_schema.COLUMNS,(select @i:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
        WHERE @i < DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)  
        )tt
   JOIN evo_wcs_g2p.bucket_robot_job brj 
   LEFT JOIN evo_wcs_g2p.job_state_change jsc
   ON brj.job_id = jsc.job_id
   WHERE jsc.project_code = 'A51274' AND jsc.state = 'DONE' AND jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')
   group by TIMESTAMPDIFF(HOUR,DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tt.theDayStartofhour)   
       
   UNION ALL 

   SELECT tmp1.ids times,
          0 'done_job_num',
          0 'nodone_job_linenum'
   FROM
   (SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) ids 
    FROM information_schema.COLUMNS,(select @i:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp
    WHERE @i < DATE_ADD(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp1
   GROUP BY tmp1.ids
) tmp2
GROUP BY tmp2.times