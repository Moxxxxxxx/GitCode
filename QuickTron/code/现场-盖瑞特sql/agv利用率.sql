SET @begin_time = '2021-08-26 12:00:00'; -- 开始时间
SET @line_num = 24; -- 默认6小时的时间段
SET @interval_time = 60; -- 间隔时间 单位：分钟

SELECT
  CASE WHEN t2.rn=1 then CONCAT(@begin_time,' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*1 MINUTE))
       WHEN t2.rn=2 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*1 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
       WHEN t2.rn=3 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
       WHEN t2.rn=4 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
       WHEN t2.rn=5 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
       WHEN t2.rn=6 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
       WHEN t2.rn=7 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE))
       WHEN t2.rn=8 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE))
       WHEN t2.rn=9 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE))
       WHEN t2.rn=10 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE))
       WHEN t2.rn=11 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE))
       WHEN t2.rn=12 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE))
       WHEN t2.rn=13 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE))
       WHEN t2.rn=14 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE))
       WHEN t2.rn=15 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE))
       WHEN t2.rn=16 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE))
       WHEN t2.rn=17 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE))
       WHEN t2.rn=18 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE))
       WHEN t2.rn=19 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE))
       WHEN t2.rn=20 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE))
       WHEN t2.rn=21 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE))
       WHEN t2.rn=22 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE))
       WHEN t2.rn=23 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE))
       WHEN t2.rn=24 then CONCAT(DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE),' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE))
       end as '时间段',
  tmp.agv_code as 'agv编码',
  CASE WHEN t2.rn=1 then CAST(tmp.free_time_1 as DECIMAL(10,0))
       WHEN t2.rn=2 then CAST(tmp.free_time_2 as DECIMAL(10,0))
       WHEN t2.rn=3 then CAST(tmp.free_time_3 as DECIMAL(10,0))
       WHEN t2.rn=4 then CAST(tmp.free_time_4 as DECIMAL(10,0))
       WHEN t2.rn=5 then CAST(tmp.free_time_5 as DECIMAL(10,0))
       WHEN t2.rn=6 then CAST(tmp.free_time_6 as DECIMAL(10,0))
       WHEN t2.rn=7 then CAST(tmp.free_time_7 as DECIMAL(10,0))
       WHEN t2.rn=8 then CAST(tmp.free_time_8 as DECIMAL(10,0))
       WHEN t2.rn=9 then CAST(tmp.free_time_9 as DECIMAL(10,0))
       WHEN t2.rn=10 then CAST(tmp.free_time_10 as DECIMAL(10,0))
       WHEN t2.rn=11 then CAST(tmp.free_time_11 as DECIMAL(10,0))
       WHEN t2.rn=12 then CAST(tmp.free_time_12 as DECIMAL(10,0))
       WHEN t2.rn=13 then CAST(tmp.free_time_13 as DECIMAL(10,0))
       WHEN t2.rn=14 then CAST(tmp.free_time_14 as DECIMAL(10,0))
       WHEN t2.rn=15 then CAST(tmp.free_time_15 as DECIMAL(10,0))
       WHEN t2.rn=16 then CAST(tmp.free_time_16 as DECIMAL(10,0))
       WHEN t2.rn=17 then CAST(tmp.free_time_17 as DECIMAL(10,0))
       WHEN t2.rn=18 then CAST(tmp.free_time_18 as DECIMAL(10,0))
       WHEN t2.rn=19 then CAST(tmp.free_time_19 as DECIMAL(10,0))
       WHEN t2.rn=20 then CAST(tmp.free_time_20 as DECIMAL(10,0))
       WHEN t2.rn=21 then CAST(tmp.free_time_21 as DECIMAL(10,0))
       WHEN t2.rn=22 then CAST(tmp.free_time_22 as DECIMAL(10,0))
       WHEN t2.rn=23 then CAST(tmp.free_time_23 as DECIMAL(10,0))
       WHEN t2.rn=24 then CAST(tmp.free_time_24 as DECIMAL(10,0))
       end as '工作时间',
  CASE WHEN t2.rn=1 then CONCAT(CAST(tmp.free_rate_1 as DECIMAL(10,2)),'%')
       WHEN t2.rn=2 then CONCAT(CAST(tmp.free_rate_2 as DECIMAL(10,2)),'%')
       WHEN t2.rn=3 then CONCAT(CAST(tmp.free_rate_3 as DECIMAL(10,2)),'%')
       WHEN t2.rn=4 then CONCAT(CAST(tmp.free_rate_4 as DECIMAL(10,2)),'%')
       WHEN t2.rn=5 then CONCAT(CAST(tmp.free_rate_5 as DECIMAL(10,2)),'%')
       WHEN t2.rn=6 then CONCAT(CAST(tmp.free_rate_6 as DECIMAL(10,2)),'%')
       WHEN t2.rn=7 then CONCAT(CAST(tmp.free_rate_7 as DECIMAL(10,2)),'%')
       WHEN t2.rn=8 then CONCAT(CAST(tmp.free_rate_8 as DECIMAL(10,2)),'%')  
       WHEN t2.rn=9 then CONCAT(CAST(tmp.free_rate_9 as DECIMAL(10,2)),'%')  
       WHEN t2.rn=10 then CONCAT(CAST(tmp.free_rate_10 as DECIMAL(10,2)),'%')
       WHEN t2.rn=11 then CONCAT(CAST(tmp.free_rate_11 as DECIMAL(10,2)),'%')
       WHEN t2.rn=12 then CONCAT(CAST(tmp.free_rate_12 as DECIMAL(10,2)),'%')
       WHEN t2.rn=13 then CONCAT(CAST(tmp.free_rate_13 as DECIMAL(10,2)),'%')
       WHEN t2.rn=14 then CONCAT(CAST(tmp.free_rate_14 as DECIMAL(10,2)),'%')
       WHEN t2.rn=15 then CONCAT(CAST(tmp.free_rate_15 as DECIMAL(10,2)),'%')
       WHEN t2.rn=16 then CONCAT(CAST(tmp.free_rate_16 as DECIMAL(10,2)),'%')
       WHEN t2.rn=17 then CONCAT(CAST(tmp.free_rate_17 as DECIMAL(10,2)),'%')
       WHEN t2.rn=18 then CONCAT(CAST(tmp.free_rate_18 as DECIMAL(10,2)),'%')
       WHEN t2.rn=19 then CONCAT(CAST(tmp.free_rate_19 as DECIMAL(10,2)),'%')
       WHEN t2.rn=20 then CONCAT(CAST(tmp.free_rate_20 as DECIMAL(10,2)),'%')
       WHEN t2.rn=21 then CONCAT(CAST(tmp.free_rate_21 as DECIMAL(10,2)),'%')
       WHEN t2.rn=22 then CONCAT(CAST(tmp.free_rate_22 as DECIMAL(10,2)),'%')
       WHEN t2.rn=23 then CONCAT(CAST(tmp.free_rate_23 as DECIMAL(10,2)),'%')
       WHEN t2.rn=24 then CONCAT(CAST(tmp.free_rate_24 as DECIMAL(10,2)),'%')
       end as '利用率'
FROM (
    SELECT
       tt.agv_code,
       AVG(tt.free_time_1) free_time_1,AVG(tt.free_rate_1) free_rate_1,
       AVG(tt.free_time_2) free_time_2,AVG(tt.free_rate_2) free_rate_2,
       AVG(tt.free_time_3) free_time_3,AVG(tt.free_rate_3) free_rate_3,
       AVG(tt.free_time_4) free_time_4,AVG(tt.free_rate_4) free_rate_4,
       AVG(tt.free_time_5) free_time_5,AVG(tt.free_rate_5) free_rate_5,
       AVG(tt.free_time_6) free_time_6,AVG(tt.free_rate_6) free_rate_6,
       AVG(tt.free_time_7) free_time_7,AVG(tt.free_rate_7) free_rate_7,
       AVG(tt.free_time_8) free_time_8,AVG(tt.free_rate_8) free_rate_8,
       AVG(tt.free_time_9) free_time_9,AVG(tt.free_rate_9) free_rate_9,
       AVG(tt.free_time_10) free_time_10,AVG(tt.free_rate_10) free_rate_10,
       AVG(tt.free_time_11) free_time_11,AVG(tt.free_rate_11) free_rate_11,
       AVG(tt.free_time_12) free_time_12,AVG(tt.free_rate_12) free_rate_12,
       AVG(tt.free_time_13) free_time_13,AVG(tt.free_rate_13) free_rate_13,
       AVG(tt.free_time_14) free_time_14,AVG(tt.free_rate_14) free_rate_14,
       AVG(tt.free_time_15) free_time_15,AVG(tt.free_rate_15) free_rate_15,
       AVG(tt.free_time_16) free_time_16,AVG(tt.free_rate_16) free_rate_16,
       AVG(tt.free_time_17) free_time_17,AVG(tt.free_rate_17) free_rate_17,
       AVG(tt.free_time_18) free_time_18,AVG(tt.free_rate_18) free_rate_18,
       AVG(tt.free_time_19) free_time_19,AVG(tt.free_rate_19) free_rate_19,
       AVG(tt.free_time_20) free_time_20,AVG(tt.free_rate_20) free_rate_20,
       AVG(tt.free_time_21) free_time_21,AVG(tt.free_rate_21) free_rate_21,
       AVG(tt.free_time_22) free_time_22,AVG(tt.free_rate_22) free_rate_22,
       AVG(tt.free_time_23) free_time_23,AVG(tt.free_rate_23) free_rate_23,
       AVG(tt.free_time_24) free_time_24,AVG(tt.free_rate_24) free_rate_24  
    FROM (
    SELECT tmp2.agv_code,
           SUM(CASE WHEN tmp1.updated_date>= @begin_time AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= @begin_time AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
           WHEN tmp1.updated_date< @begin_time AND tmp2.updated_date >= @begin_time  AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,tmp2.updated_date) 
           WHEN tmp1.updated_date< @begin_time AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
           ELSE 0 END) free_time_1,

           SUM(CASE WHEN tmp1.updated_date>= @begin_time AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= @begin_time AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
           WHEN tmp1.updated_date< @begin_time AND tmp2.updated_date >= @begin_time  AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,tmp2.updated_date) 
           WHEN tmp1.updated_date< @begin_time AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_1,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
           ELSE 0 END) free_time_2,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_2,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
           ELSE 0 END) free_time_3,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_3,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
           ELSE 0 END) free_time_4,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_4,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
           ELSE 0 END) free_time_5,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_5,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
           ELSE 0 END) free_time_6,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_6,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE))
           ELSE 0 END) free_time_7,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_7,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE))
           ELSE 0 END) free_time_8,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*7 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_8,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE))
           ELSE 0 END) free_time_9,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*8 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_9,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE))
           ELSE 0 END) free_time_10,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*9 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_10,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE))
           ELSE 0 END) free_time_11,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*10 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_11,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE))
           ELSE 0 END) free_time_12,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*11 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_12,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE))
           ELSE 0 END) free_time_13,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*12 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_13,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE))
           ELSE 0 END) free_time_14,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*13 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_14,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE))
           ELSE 0 END) free_time_15,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*14 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_15,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE))
           ELSE 0 END) free_time_16,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*15 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_16,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE))
           ELSE 0 END) free_time_17,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*16 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_17,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE))
           ELSE 0 END) free_time_18,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*17 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_18,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE))
           ELSE 0 END) free_time_19,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*18 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_19,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE))
           ELSE 0 END) free_time_20,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*19 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_20,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE))
           ELSE 0 END) free_time_21,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*20 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_21,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE))
           ELSE 0 END) free_time_22,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*21 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_22,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE))
           ELSE 0 END) free_time_23,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*22 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_23,

           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE))
           ELSE 0 END) free_time_24,
           SUM(CASE WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
           WHEN tmp1.updated_date>= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE)) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date >= DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE),tmp2.updated_date) 
           WHEN tmp1.updated_date< DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE) AND tmp2.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*23 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*24 MINUTE))
           ELSE 0 END)/(@interval_time*60)*100 free_rate_24

       FROM 
           (
           SELECT c.agv_code,c.job_id,c.updated_date
           FROM job_state_change c
           WHERE c.state = 'ALLOTTED_RESOURCE' AND job_type = 'TRAY_MOVE' 
           GROUP BY c.job_id
           )tmp1
           RIGHT JOIN
           (
           SELECT c.agv_code,c.job_id,c.updated_date
           FROM job_state_change c
           WHERE c.state = 'DONE' AND job_type = 'TRAY_MOVE' 
           GROUP BY c.job_id
           )tmp2
           ON tmp1.job_id =tmp2.job_id

       where 
          ((tmp1.updated_date >= @begin_time and tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
         or(tmp1.updated_date < @begin_time and tmp2.updated_date > @begin_time)
         or(tmp1.updated_date < @begin_time and tmp2.updated_date >= @begin_time and tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
       GROUP BY tmp2.agv_code
       )tt
			 GROUP BY tt.agv_code
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
UNION ALL 
SELECT 7 rn
UNION ALL 
SELECT 8 rn
UNION ALL 
SELECT 9 rn
UNION ALL 
SELECT 10 rn
UNION ALL 
SELECT 11 rn
UNION ALL 
SELECT 12 rn
UNION ALL 
SELECT 13 rn
UNION ALL 
SELECT 14 rn
UNION ALL 
SELECT 15 rn
UNION ALL 
SELECT 16 rn
UNION ALL 
SELECT 17 rn
UNION ALL 
SELECT 18 rn
UNION ALL 
SELECT 19 rn
UNION ALL 
SELECT 20 rn
UNION ALL
SELECT 21 rn
UNION ALL 
SELECT 22 rn
UNION ALL 
SELECT 23 rn
UNION ALL
SELECT 24 rn
)t2
GROUP BY t2.rn,tmp.agv_code