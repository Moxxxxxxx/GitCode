SET @begin_time = '2021-07-30 18:00:00'; -- 开始时间
SET @line_num = 6; -- 默认6小时的时间段
SET @interval_time = 60; -- 间隔时间 单位：分钟
SELECT
  CASE WHEN t2.rn=1 then CONCAT(@begin_time,' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*1 MINUTE))
       WHEN t2.rn=2 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*1 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
       WHEN t2.rn=3 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
       WHEN t2.rn=4 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
       WHEN t2.rn=5 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
       WHEN t2.rn=6 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
       end as '时间段',
  tmp.agv_code,
  tmp.agv_type_code,
  -- tp.agv_type_code as 'agv类型',
  CASE WHEN t2.rn=1 then tmp.free_time_1
       WHEN t2.rn=2 then tmp.free_time_2
       WHEN t2.rn=3 then tmp.free_time_3
       WHEN t2.rn=4 then tmp.free_time_4
       WHEN t2.rn=5 then tmp.free_time_5
       WHEN t2.rn=6 then tmp.free_time_6
       end as '空闲时间',
  CASE WHEN t2.rn=1 then tmp.free_rate_1
       WHEN t2.rn=2 then tmp.free_rate_2
       WHEN t2.rn=3 then tmp.free_rate_3
       WHEN t2.rn=4 then tmp.free_rate_4
       WHEN t2.rn=5 then tmp.free_rate_5
       WHEN t2.rn=6 then tmp.free_rate_6
       end as '空闲率'
FROM (
    SELECT
       AVG(tt.free_time_1) free_time_1,AVG(tt.free_rate_1) free_rate_1,
       AVG(tt.free_time_2) free_time_2,AVG(tt.free_rate_2) free_rate_2,
       AVG(tt.free_time_3) free_time_3,AVG(tt.free_rate_3) free_rate_3,
       AVG(tt.free_time_4) free_time_4,AVG(tt.free_rate_4) free_rate_4,
       AVG(tt.free_time_5) free_time_5,AVG(tt.free_rate_5) free_rate_5,
       AVG(tt.free_time_6) free_time_6,AVG(tt.free_rate_6) free_rate_6,
       tt.agv_code,
       tp.agv_type_id,
       tp1.id,
       tp1.agv_type_code
    FROM (
       select j.agv_code,
           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= @begin_time 
	         AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
	         THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= @begin_time AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
				   AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
				   THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
           WHEN j.job_accept_time< @begin_time AND j.job_finish_time >= @begin_time  
				   AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
				   THEN TIMESTAMPDIFF(SECOND,@begin_time,j.job_finish_time) 
           WHEN j.job_accept_time< @begin_time 
				   AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
				   THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
           ELSE 0 END) free_time_1,

           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= @begin_time 
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= @begin_time AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
           WHEN j.job_accept_time< @begin_time AND j.job_finish_time >= @begin_time  
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,@begin_time,j.job_finish_time) 
           WHEN j.job_accept_time< @begin_time 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_1,

           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
           ELSE 0 END) free_time_2,

           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) 
           AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) 
           THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_2,

           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
           ELSE 0 END) free_time_3,
           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_3,

           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
           ELSE 0 END) free_time_4,
           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_4,

           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
           ELSE 0 END) free_time_5,
           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_5,

           @interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
           ELSE 0 END) free_time_6,
           (@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
           WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),j.job_finish_time) 
           WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
           ELSE 0 END))/(@interval_time*60) free_rate_6

       from evo_rcs.agv_job_history j

       where 
          ((j.job_accept_time >=@begin_time and j.job_accept_time<DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
         or(j.job_accept_time<@begin_time and j.job_finish_time>@begin_time)
         or(j.job_accept_time<@begin_time and j.job_finish_time>=@begin_time and j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)))
         AND (j.job_type ='BUCKET_MOVE_JOB' OR j.job_type ='MOVE_JOB')
       GROUP BY j.agv_code
       )tt
    LEFT JOIN
     (
      SELECT ba.agv_code,ba.agv_type_id
      FROM evo_rcs.basic_agv ba
     )tp
     ON tt.agv_code = tp.agv_code
    LEFT JOIN
     (
      SELECT bat.id,bat.agv_type_code
      FROM evo_rcs.basic_agv_type bat
     )tp1
     ON tp.agv_type_id = tp1.id
)tmp,
(
SELECT 1 rn
UNION ALL 
SELECT 2 rn
UNION ALL 
SELECT 3 rn
UNION ALL 
SELECT 4 rn
UNION ALL 
SELECT 5 rn
UNION ALL 
SELECT 6 rn
)t2
GROUP BY t2.rn,tmp.agv_code