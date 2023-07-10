--  设置搬运作业单的创建时间的起止时间点
set @start_time = '2022-10-20 00:00:00.000';
set @end_time = '2022-10-20 23:59:59.999';
-- select @start_time,@end_time;


select 
COALESCE(l.line_name, '未配置') AS `路线`,
date_format(tt.create_time, '%Y-%m-%d %H:00:00') as `小时`,
count(distinct tt.order_no)  as `小时内创建的搬运作业单数`,
count(distinct case when tt.init_job_move_start_cost is not null and tt.init_job_move_start_cost/1000 > 5 then tt.order_no end) as `INIT_JOB到MOVE_START耗时超过5秒的搬运作业单数`,
concat(round((count(distinct case when tt.init_job_move_start_cost is not null and tt.init_job_move_start_cost/1000 > 5 then tt.order_no end)/count(distinct tt.order_no))*100,2),'%') as `INIT_JOB到MOVE_START耗时超过5秒的超时率`,
round(avg(case when tt.init_job_move_start_cost is not null and tt.init_job_move_start_cost/1000 > 5 then tt.init_job_move_start_cost/1000 - 5 end),2) as `INIT_JOB到MOVE_START耗时超过5秒的平均超时时长（秒）`,
count(distinct case when tt.before_move_start_cost is not null and tt.before_move_start_cost/1000 > 5 then tt.order_no end) as `到MOVE_START之前耗时超过5秒的搬运作业单数`,
concat(round((count(distinct case when tt.before_move_start_cost is not null and tt.before_move_start_cost/1000 > 5 then tt.order_no end)/count(distinct tt.order_no))*100,2),'%') as `到MOVE_START之前耗时超过5秒的超时率`,
round(avg(case when tt.before_move_start_cost is not null and tt.before_move_start_cost/1000 > 5 then tt.before_move_start_cost/1000 - 5 end),2) as `到MOVE_START之前耗时超过5秒的平均超时时长（秒）`

from 
(select 
t.order_no,
t.create_time,
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end   start_point,  
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end target_point,
sum(case when tl.id=tm.move_start_link_id then tl.cost_time end) as init_job_move_start_cost,  --  INIT_JOB 到 MOVE_START 的耗时
sum(case when tl.id<=tm.move_start_link_id then tl.cost_time end) as before_move_start_cost    --  到MOVE_START 之前的耗时
from phoenix_rss.transport_order t
left join phoenix_rss.transport_order_link tl on tl.order_no = t.order_no 
-- 定位每个搬运作业单 execute_state='MOVE_START' 的ID
left join 
(select 
t.order_no,
min(tl.id) as move_start_link_id
from phoenix_rss.transport_order t
inner join phoenix_rss.transport_order_link tl on tl.order_no = t.order_no and tl.execute_state='MOVE_START'
where t.create_time BETWEEN @start_time and @end_time  
group by t.order_no)tm on tm.order_no=t.order_no
where t.create_time BETWEEN @start_time and @end_time 
group by t.order_no,t.create_time,start_point,target_point)tt
-- 配置的路线表
LEFT JOIN
(SELECT DISTINCT tmp1.id AS line_id
                    , tmp1.line_name
                    , tmp1.estimate_move_time_consuming
                    , tmp2.start_point_code
                    , tmp3.target_point_code
      FROM qt_smartreport.carry_job_line_info_v4 tmp1
               LEFT JOIN
           qt_smartreport.carry_job_start_point_code_v4 tmp2
           ON
               tmp1.id = tmp2.line_id
               LEFT JOIN
           qt_smartreport.carry_job_target_point_code_v4 tmp3
           ON
               tmp1.id = tmp3.line_id) l
     ON
                 tt.start_point = l.start_point_code
             AND
                 tt.target_point = l.target_point_code
group by 1,2