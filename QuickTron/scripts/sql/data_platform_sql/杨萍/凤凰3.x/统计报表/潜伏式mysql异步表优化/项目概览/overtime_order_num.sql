-- 用于：统计报表->项目概览->当日系统运营统计（超时作业单）

select 
count(distinct tt.order_no)      as create_order_num,   -- 下发搬运作业单量 
count(distinct case when tt.overtime = 1 then tt.order_no end)  as timeout_order_num,  -- 超时作业单数
COALESCE(count(distinct case when tt.overtime = 1 then tt.order_no end) / count(distinct tt.order_no),0) AS timeout_order_rate  -- 作业单超时率
from 
(select 
t.order_no,
t.create_time,
t.update_time,
unix_timestamp(t.update_time) - unix_timestamp(t.create_time)    as total_time_consuming,
COALESCE(tp.line_name, 'unknow')             as line_name,
COALESCE(CONCAT(t.start_point_code, ' - ', t.target_point_code), 'unknow')       as path_name,
tp.estimate_move_time_consuming,
case when (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) > tp.estimate_move_time_consuming * 60 then 1 else 0 end as overtime,
(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) - tp.estimate_move_time_consuming * 60   as timeout_duration
from phoenix_rss.transport_order t
left join 
(SELECT DISTINCT 
tmp1.id AS line_id
, tmp1.line_name
, tmp1.estimate_move_time_consuming
, tmp2.start_point_code
, tmp3.target_point_code
FROM qt_smartreport.carry_job_line_info_v4 tmp1
LEFT JOIN qt_smartreport.carry_job_start_point_code_v4 tmp2 
ON tmp1.id = tmp2.line_id
LEFT JOIN qt_smartreport.carry_job_target_point_code_v4 tmp3
ON tmp1.id = tmp3.line_id) tp 
ON t.start_point_code = tp.start_point_code AND t.target_point_code = tp.target_point_code
where t.create_time >= { now_start_time } ) tt 




#############################################################################################
---  检查
#############################################################################################


-- { now_time }
-- { start_time }
-- { end_time }
-- { now_start_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
set @now_start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 当天开始时间
select @now_time, @start_time, @end_time,@now_start_time;


select 
count(distinct tt.order_no)      as create_order_num,   -- 下发搬运作业单量 
count(distinct case when tt.overtime = 1 then tt.order_no end)  as timeout_order_num,  -- 超时作业单数
COALESCE(count(distinct case when tt.overtime = 1 then tt.order_no end) / count(distinct tt.order_no),0) AS timeout_order_rate  -- 作业单超时率
from 
(select 
t.order_no,
t.create_time,
t.update_time,
unix_timestamp(t.update_time) - unix_timestamp(t.create_time)    as total_time_consuming,
COALESCE(tp.line_name, 'unknow')             as line_name,
COALESCE(CONCAT(t.start_point_code, ' - ', t.target_point_code), 'unknow')       as path_name,
tp.estimate_move_time_consuming,
case when (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) > tp.estimate_move_time_consuming * 60 then 1 else 0 end as overtime,
(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) - tp.estimate_move_time_consuming * 60   as timeout_duration
from phoenix_rss.transport_order t
left join 
(SELECT DISTINCT 
tmp1.id AS line_id
, tmp1.line_name
, tmp1.estimate_move_time_consuming
, tmp2.start_point_code
, tmp3.target_point_code
FROM qt_smartreport.carry_job_line_info_v4 tmp1
LEFT JOIN qt_smartreport.carry_job_start_point_code_v4 tmp2 
ON tmp1.id = tmp2.line_id
LEFT JOIN qt_smartreport.carry_job_target_point_code_v4 tmp3
ON tmp1.id = tmp3.line_id) tp 
ON t.start_point_code = tp.start_point_code AND t.target_point_code = tp.target_point_code
where t.create_time >= @now_start_time) tt 