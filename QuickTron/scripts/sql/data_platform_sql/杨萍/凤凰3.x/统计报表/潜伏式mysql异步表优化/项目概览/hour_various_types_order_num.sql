-- 用于：统计报表->项目概览->分时搬运作业单量 

select 
hour_value AS x,  -- 小时
coalesce(sum(create_order_num), 0)   as create_order_num,  -- 下发作业单量
coalesce(sum(abnormal_order_num), 0) as abnormal_order_num,  -- 异常作业单量
coalesce(sum(canceled_order_num), 0) as canceled_order_num   -- 取消单量
from 
(select 
DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') as hour_value,
count(distinct order_no)        as create_order_num,
null                            as abnormal_order_num,
null                            as canceled_order_num
from phoenix_rss.transport_order
where create_time >= { now_start_time }
group by hour_value
union all
select 
DATE_FORMAT(update_time, '%Y-%m-%d %H:00:00')  as hour_value,
null    as create_order_num,
count(distinct case when order_state in ('ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED', 'PENDING') then order_no end)  as abnormal_order_num,
count(distinct case when order_state in ('CANCELED') then order_no end) as canceled_order_num
from phoenix_rss.transport_order
where update_time >= { now_start_time }
group by hour_value) t
group by hour_value




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
hour_value AS x,  -- 小时
coalesce(sum(create_order_num), 0)   as create_order_num,  -- 下发作业单量
coalesce(sum(abnormal_order_num), 0) as abnormal_order_num,  -- 异常作业单量
coalesce(sum(canceled_order_num), 0) as canceled_order_num   -- 取消单量
from 
(select 
DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') as hour_value,
count(distinct order_no)        as create_order_num,
null                            as abnormal_order_num,
null                            as canceled_order_num
from phoenix_rss.transport_order
where create_time >= @now_start_time
group by hour_value
union all
select 
DATE_FORMAT(update_time, '%Y-%m-%d %H:00:00')  as hour_value,
null    as create_order_num,
count(distinct case when order_state in ('ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED', 'PENDING') then order_no end)  as abnormal_order_num,
count(distinct case when order_state in ('CANCELED') then order_no end) as canceled_order_num
from phoenix_rss.transport_order
where update_time >= @now_start_time
group by hour_value) t
group by hour_value