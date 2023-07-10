-- 用于：统计报表->故障异常统计->系统故障统计->各类型模块系统故障统计

-- 新增故障次数 ：create_sys_error_num
-- 故障率（搬运作业单） ：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_sys_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- MTTR = end_error_time/end_sys_error_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- 当期MTBF = (theory_run_duration-error_duration)/sys_error_num

select
ts.alarm_service,  -- 系统服务模块
COALESCE(t1.create_sys_error_num,0) as create_sys_error_num,   -- 新增故障次数
COALESCE(t1.end_sys_error_num,0) as end_sys_error_num,     -- 结束故障次数
t1.end_error_time as end_error_time,           -- 结束故障时间
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.sys_error_num,0) as sys_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.theory_run_duration,0) as theory_run_duration,   -- 理论运行时长
COALESCE(t3.error_duration,0) as error_duration   -- 持续故障时长（去重）
from
--  各类型模块系统
(select
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts
-- part1:筛选小时范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join
(select
t1.alarm_service,
count(distinct case when t1.start_time BETWEEN { start_time } and { end_time } then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
count(distinct t1.error_id) as sys_error_num  -- 时间段内有持续的故障次数
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,bn.alarm_service
from qt_smartreport.qtr_hour_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.hour_start_time BETWEEN { start_time } and { end_time })t1
group by t1.alarm_service)t1 on t1.alarm_service = ts.alarm_service
-- part2:筛选小时范围内的新增订单量、新增任务量
left join
(select
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN { start_time } and { end_time })t2 on 1
-- part3:所筛选小时范围内理论运行时长、故障持续时长（去重）
left join
(select
alarm_service,
sum(theory_run_duration) as  theory_run_duration, -- 理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒）
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where alarm_service != 'ALL_SYS'
and hour_start_time BETWEEN { start_time } and { end_time }
group by alarm_service)t3 on t3.alarm_service = ts.alarm_service




#############################################################################################
---  检查
#############################################################################################
-- { now_time }
-- { start_time }
-- { end_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
select @now_time, @start_time, @end_time;


-- 新增故障次数 ：create_sys_error_num
-- 故障率（搬运作业单） ：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_sys_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- MTTR = end_error_time/end_sys_error_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- 当期MTBF = (theory_run_duration-error_duration)/sys_error_num


select
ts.alarm_service,  -- 系统服务模块
COALESCE(t1.create_sys_error_num,0) as create_sys_error_num,   -- 新增故障次数
COALESCE(t1.end_sys_error_num,0) as end_sys_error_num,     -- 结束故障次数
t1.end_error_time as end_error_time,           -- 结束故障时间
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.sys_error_num,0) as sys_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.theory_run_duration,0) as theory_run_duration,   -- 理论运行时长 
COALESCE(t3.error_duration,0) as error_duration   -- 持续故障时长（去重） 
from
--  各类型模块系统
(select
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts
-- part1:筛选小时范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join 
(select
t1.alarm_service,
count(distinct case when t1.start_time BETWEEN @start_time and @end_time then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
count(distinct t1.error_id) as sys_error_num  -- 时间段内有持续的故障次数
from 
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,bn.alarm_service
from qt_smartreport.qtr_hour_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.hour_start_time BETWEEN @start_time and @end_time)t1
group by t1.alarm_service)t1 on t1.alarm_service = ts.alarm_service
-- part2:筛选小时范围内的新增订单量、新增任务量
left join
(select
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time)t2 on 1
-- part3:所筛选小时范围内理论运行时长、故障持续时长（去重）
left join 
(select 
alarm_service,
sum(theory_run_duration) as  theory_run_duration, -- 理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒） 
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where alarm_service != 'ALL_SYS'
and hour_start_time BETWEEN @start_time and @end_time
group by alarm_service)t3 on t3.alarm_service = ts.alarm_service