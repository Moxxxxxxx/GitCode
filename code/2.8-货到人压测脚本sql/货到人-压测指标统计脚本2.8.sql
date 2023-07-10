
-- by 黄利辉
-- modified by 冯雄锋 2021/6/23（2.8.2表结构变化：evo_rcs.rcs_agv_history_job变为evo_rcs.agv_job_history，相应字段改正）
-- modified by 冯雄锋 2021/7/6（第6段AGV空闲率代码有问题，导致最后一段统计值总是很大，已修改，6改成5）


-- 间隔X分钟统计
SET @begin_time = '2021-06-23 16:30:00'; -- 开始统计时间
select @begin_time,now();
-- SELECT @begin_time,NOW();
SET @interval_time = 30; -- 间隔时间 单位：分钟
-- 时间区间内，完成作业单数，完成订单行数，完成拣货件数


SELECT * from (
SELECT ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60) as '时间段',count(1) as '拍灯次数'
   FROM evo_wcs_g2p.station_task_group stg
 where stg.job_type = 'G2P_PICK'
 GROUP BY ceil(TIMESTAMPDIFF(SECOND, @begin_time,stg.updated_date)/@interval_time/60)
) tt
where tt.`时间段` > 0 and tt.`时间段`<7;


SELECT tt.rn as '时间段',
CASE WHEN tt.rn=1 then tmp.order_1
WHEN tt.rn=2 then tmp.order_2
WHEN tt.rn=3 then tmp.order_3
WHEN tt.rn=4 then tmp.order_4
WHEN tt.rn=5 then tmp.order_5
WHEN tt.rn=6 then tmp.order_6
end as '完成作业单数',
CASE WHEN tt.rn=1 then tmp2.detail_1
WHEN tt.rn=2 then tmp2.detail_2
WHEN tt.rn=3 then tmp2.detail_3
WHEN tt.rn=4 then tmp2.detail_4
WHEN tt.rn=5 then tmp2.detail_5
WHEN tt.rn=6 then tmp2.detail_6
end as '完成订单行数',
CASE WHEN tt.rn=1 then tmp2.qty_1
WHEN tt.rn=2 then tmp2.qty_2
WHEN tt.rn=3 then tmp2.qty_3
WHEN tt.rn=4 then tmp2.qty_4
WHEN tt.rn=5 then tmp2.qty_5
WHEN tt.rn=6 then tmp2.qty_6
end as '完成拣货件数'

FROM (
SELECT
SUM(CASE WHEN t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN 1 ELSE 0 END) as 'order_1',
SUM(CASE WHEN t.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN 1 ELSE 0 END) as 'order_2',
SUM(CASE WHEN t.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN 1 ELSE 0 END) as 'order_3',
SUM(CASE WHEN t.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN 1 ELSE 0 END) as 'order_4',
SUM(CASE WHEN t.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN 1 ELSE 0 END) as 'order_5',
SUM(CASE WHEN t.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN 1 ELSE 0 END) as 'order_6'

FROM evo_wcs_g2p.picking_work t
 WHERE t.state = 'DONE' AND t.updated_date > @begin_time AND t.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
) tmp,
(
SELECT
-- 时间区间内，完成订单行数
SUM(CASE WHEN t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN 1 ELSE 0 END) as 'detail_1',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN 1 ELSE 0 END) as 'detail_2',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN 1 ELSE 0 END) as 'detail_3',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN 1 ELSE 0 END) as 'detail_4',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN 1 ELSE 0 END) as 'detail_5',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN 1 ELSE 0 END) as 'detail_6',
-- 时间区间内，完成货品件数
SUM(CASE WHEN t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_1',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_2',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_3',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_4',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_5',
SUM(CASE WHEN t1.updated_date > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN t1.actual_quantity ELSE 0 END) as 'qty_6'

 FROM evo_wcs_g2p.picking_job t1 WHERE t1.state = 'DONE' AND t1.updated_date > @begin_time AND t1.updated_date <= DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
) tmp2,
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
)tt
GROUP BY tt.rn
;



-- 时间区间内，工作站空闲率
SELECT seq.station_code,

                (@interval_time*60-sum(CASE WHEN seq.entry_time >= @begin_time AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= @begin_time AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '1',

                (@interval_time*60-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.exit_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),seq.exit_time)
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '2',
                (@interval_time*60-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.exit_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),seq.exit_time)
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '3',
                (@interval_time*60-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.exit_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),seq.exit_time)
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '4',
                (@interval_time*60-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.exit_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),seq.exit_time)
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '5',
                (@interval_time*60-sum(CASE WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN timestampdiff(second,seq.entry_time,seq.exit_time)
                     WHEN seq.entry_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN timestampdiff(second,seq.entry_time,DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND seq.exit_time>= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),seq.exit_time)
                     WHEN seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND seq.entry_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) AND seq.exit_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN timestampdiff(second,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
                      ELSE 0 END))/(@interval_time*60) as '6'

  FROM evo_station.station_entry seq
 WHERE seq.entry_time >= @begin_time 
  AND seq.exit_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
GROUP BY seq.station_code;


-- agv 空闲率
SELECT t2.rn as '时间段',
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
SELECT AVG(tt.free_time_1) free_time_1,AVG(tt.free_rate_1) free_rate_1,
AVG(tt.free_time_2) free_time_2,AVG(tt.free_rate_2) free_rate_2,
AVG(tt.free_time_3) free_time_3,AVG(tt.free_rate_3) free_rate_3,
AVG(tt.free_time_4) free_time_4,AVG(tt.free_rate_4) free_rate_4,
AVG(tt.free_time_5) free_time_5,AVG(tt.free_rate_5) free_rate_5,
AVG(tt.free_time_6) free_time_6,AVG(tt.free_rate_6) free_rate_6
FROM (
select j.agv_code,

@interval_time*60-SUM(CASE WHEN j.job_accept_time>= @begin_time AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
     WHEN j.job_accept_time>= @begin_time AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
     WHEN j.job_accept_time< @begin_time AND j.job_finish_time >= @begin_time  AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,j.job_finish_time) 
     WHEN j.job_accept_time< @begin_time AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
     ELSE 0 END) free_time_1,

(@interval_time*60-SUM(CASE WHEN j.job_accept_time>= @begin_time AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
     WHEN j.job_accept_time>= @begin_time AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)) 
     WHEN j.job_accept_time< @begin_time AND j.job_finish_time >= @begin_time  AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,j.job_finish_time) 
     WHEN j.job_accept_time< @begin_time AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) THEN TIMESTAMPDIFF(SECOND,@begin_time,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE))
     ELSE 0 END))/(@interval_time*60) free_rate_1,

@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
     WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),j.job_finish_time) 
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
     ELSE 0 END) free_time_2,
(@interval_time*60-SUM(CASE WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,j.job_finish_time) 
     WHEN j.job_accept_time>= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_accept_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,j.job_accept_time,DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)) 
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),j.job_finish_time) 
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE))
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
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time >= DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),j.job_finish_time) 
     WHEN j.job_accept_time< DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) AND j.job_finish_time > DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE) THEN TIMESTAMPDIFF(SECOND,DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE),DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE))
     ELSE 0 END))/(@interval_time*60) free_rate_6

from evo_rcs.basic_agv ba
LEFT JOIN evo_rcs.agv_job_history j
  ON ba.agv_code = j.agv_code
where 
(
(
j.job_accept_time >=@begin_time and j.job_accept_time<DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
)
or
(
j.job_accept_time<@begin_time and j.job_finish_time>@begin_time
)
or 
(
j.job_accept_time<@begin_time and j.job_finish_time>=@begin_time and j.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
)
)
GROUP BY j.agv_code
) tt) tmp,
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
GROUP BY t2.rn

;



set @row_num_1 := 0;
set @num_agv_1 := 0;
set @num_win_1 := 0;
set @quantity_1 := 0;
set @last_group_job_id_1 := null;
set @last_agv_1 := null;

set @row_num_2 := 0;
set @num_agv_2 := 0;
set @num_win_2 := 0;
set @quantity_2 := 0;
set @last_group_job_id_2 := null;
set @last_agv_2 := null;

set @row_num_3 := 0;
set @num_agv_3 := 0;
set @num_win_3 := 0;
set @quantity_3 := 0;
set @last_group_job_id_3 := null;
set @last_agv_3 := null;

set @row_num_4 := 0;
set @num_agv_4 := 0;
set @num_win_4 := 0;
set @quantity_4 := 0;
set @last_group_job_id_4 := null;
set @last_agv_4 := null;

set @row_num_5 := 0;
set @num_agv_5 := 0;
set @num_win_5 := 0;
set @quantity_5 := 0;
set @last_group_job_id_5 := null;
set @last_agv_5 := null;

set @row_num_6 := 0;
set @num_agv_6 := 0;
set @num_win_6 := 0;
set @quantity_6 := 0;
set @last_group_job_id_6 := null;
set @last_agv_6 := null;


-- 性能指标：时间段、qty、进站次数、 k-value、 一次进站弹几次窗、 一次弹窗拣几件
SELECT tt.rn as '时间段',tt.qty as 'qty',tt.enter_times as '进站次数',tt.k_vlue as 'k-value',tt.oneInToWin as '一次进站弹几次窗',tt.onWinToNum as '一次弹窗拣几件'
FROM (
SELECT 
     1 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_1 := @row_num_1 + 1 as row_num,
          (case when @last_agv_1 = tmp.agv_code then @num_agv_1 else @num_agv_1 := @num_agv_1 + 1 end) as agv_num,
          (case when @last_group_job_id_1 = tmp.group_job_id then @num_win_1 else @num_win_1 := @num_win_1 + 1 end) as win_num, 
          @quantity_1 := @quantity_1 + tmp.quantity as qty,
          (@last_agv_1 := tmp.agv_code), (@last_group_job_id_1 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between @begin_time and DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta

UNION ALL

SELECT 
     2 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_2 := @row_num_2 + 1 as row_num,
          (case when @last_agv_2 = tmp.agv_code then @num_agv_2 else @num_agv_2 := @num_agv_2 + 1 end) as agv_num,
          (case when @last_group_job_id_2 = tmp.group_job_id then @num_win_2 else @num_win_2 := @num_win_2 + 1 end) as win_num, 
          @quantity_2 := @quantity_2 + tmp.quantity as qty,
          (@last_agv_2 := tmp.agv_code), (@last_group_job_id_2 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between DATE_ADD(@begin_time,INTERVAL @interval_time MINUTE) and DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta

UNION ALL

SELECT 
     3 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_3 := @row_num_3 + 1 as row_num,
          (case when @last_agv_3 = tmp.agv_code then @num_agv_3 else @num_agv_3 := @num_agv_3 + 1 end) as agv_num,
          (case when @last_group_job_id_3 = tmp.group_job_id then @num_win_3 else @num_win_3 := @num_win_3 + 1 end) as win_num, 
          @quantity_3 := @quantity_3 + tmp.quantity as qty,
          (@last_agv_3 := tmp.agv_code), (@last_group_job_id_3 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between DATE_ADD(@begin_time,INTERVAL @interval_time*2 MINUTE) and DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta

UNION ALL

SELECT 
     4 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_4 := @row_num_4 + 1 as row_num,
          (case when @last_agv_4 = tmp.agv_code then @num_agv_4 else @num_agv_4 := @num_agv_4 + 1 end) as agv_num,
          (case when @last_group_job_id_4 = tmp.group_job_id then @num_win_4 else @num_win_4 := @num_win_4 + 1 end) as win_num, 
          @quantity_4 := @quantity_4 + tmp.quantity as qty,
          (@last_agv_4 := tmp.agv_code), (@last_group_job_id_4 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between DATE_ADD(@begin_time,INTERVAL @interval_time*3 MINUTE) and DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta

UNION ALL

SELECT 
     5 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_5 := @row_num_5 + 1 as row_num,
          (case when @last_agv_5 = tmp.agv_code then @num_agv_5 else @num_agv_5 := @num_agv_5 + 1 end) as agv_num,
          (case when @last_group_job_id_5 = tmp.group_job_id then @num_win_5 else @num_win_5 := @num_win_5 + 1 end) as win_num, 
          @quantity_5 := @quantity_5 + tmp.quantity as qty,
          (@last_agv_5 := tmp.agv_code), (@last_group_job_id_5 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between DATE_ADD(@begin_time,INTERVAL @interval_time*4 MINUTE) and DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta

UNION ALL

SELECT 
     6 as 'rn',
     max(qty) as 'qty', 
     max(ta.agv_num) as 'enter_times',
     max(qty)/max(ta.agv_num) as 'k_vlue',  
     max(ta.win_num)/max(ta.agv_num) as 'oneInToWin', 
     max(qty)/max(ta.win_num) as 'onWinToNum'
FROM (
     SELECT 
          @row_num_6 := @row_num_6 + 1 as row_num,
          (case when @last_agv_6 = tmp.agv_code then @num_agv_6 else @num_agv_6 := @num_agv_6 + 1 end) as agv_num,
          (case when @last_group_job_id_6 = tmp.group_job_id then @num_win_6 else @num_win_6 := @num_win_6 + 1 end) as win_num, 
          @quantity_6 := @quantity_6 + tmp.quantity as qty,
          (@last_agv_6 := tmp.agv_code), (@last_group_job_id_6 := tmp.group_job_id)
     FROM (
          SELECT stg.group_job_id, stg.job_id, pj.agv_code, pj.station_code, pj.bucket_slot_code, pj.quantity, stg.updated_date 
          FROM evo_wcs_g2p.`station_task_group` stg 
          LEFT JOIN evo_wcs_g2p.picking_job pj ON stg.job_id = pj.job_id 
          WHERE stg.updated_date between DATE_ADD(@begin_time,INTERVAL @interval_time*5 MINUTE) and DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
          ORDER BY pj.station_code, stg.updated_date 
     ) tmp
) ta
) tt
;



SET @outNo = 0;
SET @last_bucket_code = '';
SELECT MAX(t2.go_station_num) max_station_num,
       ROUND(AVG(t2.go_station_num),4) avg_station_num
from (
SELECT tmp.bucket_code,tmp.outNO,SUM(tmp.is_out) move_times,COUNT(DISTINCT tmp.station_code) go_station_num
from (
SELECT bmj.bucket_code,bmj.bucket_move_type,bmj.station_code,bmj.source_waypoint_code,bmj.target_waypoint_code,bmj.created_date,map.out_code,
       IF(map.out_code is null,1,0) is_out,
       IF(map.out_code is null,IF(@last_bucket_code = bmj.bucket_code,@outNo,@outNo := @outNo + 1),@outNo := @outNo + 1) outNO,
       IF(@last_bucket_code = bmj.bucket_code,@last_bucket_code,@last_bucket_code := bmj.bucket_code) last_bucket_code
  FROM evo_wcs_g2p.bucket_move_job bmj
  LEFT JOIN mysql.map_info map
    ON map.pointCode = bmj.target_waypoint_code
   AND map.pointType = 'STORAGE'
  WHERE bmj.state <> 'ROLLBACK'
    AND LENGTH(bmj.agv_code) > 0
    AND bmj.created_date BETWEEN @begin_time AND DATE_ADD(@begin_time,INTERVAL @interval_time*6 MINUTE)
 ORDER BY bmj.bucket_code ASC,bmj.id ASC
) tmp
WHERE tmp.station_code IS not NULL
GROUP BY tmp.bucket_code,tmp.outNO
ORDER BY tmp.bucket_code,tmp.outNO
) t2
;
