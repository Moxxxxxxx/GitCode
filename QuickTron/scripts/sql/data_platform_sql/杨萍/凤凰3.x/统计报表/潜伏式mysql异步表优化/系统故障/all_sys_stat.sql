-- 用于：统计报表->故障异常统计->系统故障统计->全场系统故障统计

-- 新增故障次数 ：create_sys_error_num
-- 故障率（搬运作业单） ：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_sys_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- MTTR = end_error_time/end_sys_error_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- 当期MTBF = (theory_run_duration-error_duration)/sys_error_num


select
COALESCE(max(td.create_sys_error_num),0) as create_sys_error_num,   -- 新增故障次数
COALESCE(max(td.end_sys_error_num),0) as end_sys_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新人任务量
COALESCE(max(td.sys_error_num),0) as  sys_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.theory_run_duration),0) as  theory_run_duration,    -- 理论运行时长
COALESCE(max(td.error_duration),0) as  error_duration         -- 持续故障时长（去重）
from
(
-- part1:筛选小时范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
select
count(distinct case when t1.start_time BETWEEN { start_time } and { end_time } then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
null as create_order_num,
null as create_job_num,
count(distinct t1.error_id) as sys_error_num,  -- 时间段内有持续的故障次数
null as theory_run_duration,
null as error_duration
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time
from qt_smartreport.qtr_hour_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.hour_start_time BETWEEN { start_time } and { end_time })t1
-- part2:所筛选小时范围内 新增作业单数、机器人任务数
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
count(distinct tor.order_no)                      as create_order_num,  -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num,  -- 机器人任务数
null as sys_error_num,
null as theory_run_duration,
null as error_duration
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN { start_time } and { end_time }
-- part3:所筛选小时范围内机器人理论运行时长、故障持续时长
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,
null as create_job_num,
null as sys_error_num,
sum(theory_run_duration) as  theory_run_duration, -- 理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒）
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where alarm_service ='ALL_SYS'
and hour_start_time BETWEEN { start_time } and { end_time }
)td




















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
COALESCE(max(td.create_sys_error_num),0) as create_sys_error_num,   -- 新增故障次数
COALESCE(max(td.end_sys_error_num),0) as end_sys_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新人任务量
COALESCE(max(td.sys_error_num),0) as  sys_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.theory_run_duration),0) as  theory_run_duration,    -- 理论运行时长    
COALESCE(max(td.error_duration),0) as  error_duration         -- 持续故障时长（去重）  
from 
(
-- part1:筛选小时范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
select
count(distinct case when t1.start_time BETWEEN @start_time and @end_time then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
null as create_order_num,
null as create_job_num,
count(distinct t1.error_id) as sys_error_num,  -- 时间段内有持续的故障次数
null as theory_run_duration,
null as error_duration
from 
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time
from qt_smartreport.qtr_hour_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.hour_start_time BETWEEN @start_time and @end_time)t1
-- part2:所筛选小时范围内 新增作业单数、机器人任务数
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
count(distinct tor.order_no)                      as create_order_num,  -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num,  -- 机器人任务数
null as sys_error_num,
null as theory_run_duration,
null as error_duration
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time
-- part3:所筛选小时范围内机器人理论运行时长、故障持续时长
union all 
select 
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,
null as create_job_num,
null as sys_error_num,
sum(theory_run_duration) as  theory_run_duration, -- 理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒） 
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where alarm_service ='ALL_SYS'
and hour_start_time BETWEEN @start_time and @end_time
)td 