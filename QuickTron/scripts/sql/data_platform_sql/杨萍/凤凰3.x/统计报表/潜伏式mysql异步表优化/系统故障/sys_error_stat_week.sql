-- 用于：统计报表->故障异常统计->系统故障趋势统计->周

-- 新增故障次数：create_sys_error_num
-- 故障率（搬运作业单）：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量： create_order_num
-- 故障率（机器人任务）：使用 create_sys_error_num 和 create_job_num   注意数据的3种展示
-- 机器人任务量：create_job_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- MTTR = end_error_time/end_sys_error_num
-- 当期MTBF = (theory_run_duration-error_duration)/error_num
-- 累计MTBF = (accum_theory_run_duration-accum_error_duration)/accum_error_num


select
'week' as stat_time_type,  -- 周
date_format(t1.date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
t1.alarm_service,
COALESCE(t2.create_sys_error_num,0) as create_sys_error_num,
COALESCE(t2.end_sys_error_num,0) as end_sys_error_num,
t2.end_error_time,
COALESCE(t3.create_order_num) as create_order_num,
COALESCE(t3.create_job_num) as create_job_num,
t1.theory_run_duration,   -- 理论运行时长
t1.error_duration,        -- 故障时长
t1.error_num,             -- 故障次数
t1.accum_theory_run_duration,  --  累计理论运行时长
t1.accum_error_duration,      -- 累计故障时长
t1.accum_error_num             -- 累计故障次数
-- part1:所筛选周范围内理论运行时长、故障时长、故障次数、累计理论运行时长、累计故障时长、累计故障次数
from qt_smartreport.qtr_week_sys_error_mtbf_his t1
-- part2:筛选周范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join
(select
t1.date_value,
COALESCE(t1.alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct case when date(DATE_SUB(t1.start_time,INTERVAL WEEKDAY(t1.start_time) + 0 DAY))=t1.date_value then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and date(DATE_SUB(t1.end_time,INTERVAL WEEKDAY(t1.end_time) + 0 DAY))=t1.date_value then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and date(DATE_SUB(t1.end_time,INTERVAL WEEKDAY(t1.end_time) + 0 DAY))=t1.date_value then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time -- 已结束故障时长（秒）
from
(select t.date_value,t.error_id,bn.start_time,bn.end_time,bn.alarm_service
from qt_smartreport.qtr_week_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value BETWEEN { start_time } and { end_time })t1
group by t1.date_value,t1.alarm_service
with ROLLUP)t2 on t2.alarm_service_name=t1.alarm_service and t2.date_value=t1.date_value
-- part3:筛选周范围内的新增订单量、新增任务量
left join
(select
date(DATE_SUB(tor.create_time,INTERVAL WEEKDAY(tor.create_time) + 0 DAY)) as date_value,
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN { start_time } and { end_time }
group by date_value)t3 on t3.date_value=t1.date_value
where t1.date_value between { start_time } and { end_time }






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


-- 新增故障次数：create_sys_error_num
-- 故障率（搬运作业单）：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量： create_order_num
-- 故障率（机器人任务）：使用 create_sys_error_num 和 create_job_num   注意数据的3种展示
-- 机器人任务量：create_job_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- MTTR = end_error_time/end_sys_error_num
-- 当期MTBF = (theory_run_duration-error_duration)/error_num
-- 累计MTBF = (accum_theory_run_duration-accum_error_duration)/accum_error_num


select 
'week' as stat_time_type,  -- 周
date_format(t1.date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
t1.alarm_service,
COALESCE(t2.create_sys_error_num,0) as create_sys_error_num,
COALESCE(t2.end_sys_error_num,0) as end_sys_error_num,
t2.end_error_time,
COALESCE(t3.create_order_num) as create_order_num,
COALESCE(t3.create_job_num) as create_job_num,
t1.theory_run_duration,   -- 理论运行时长
t1.error_duration,        -- 故障时长
t1.error_num,             -- 故障次数
t1.accum_theory_run_duration,  --  累计理论运行时长
t1.accum_error_duration,      -- 累计故障时长
t1.accum_error_num             -- 累计故障次数
-- part1:所筛选周范围内理论运行时长、故障时长、故障次数、累计理论运行时长、累计故障时长、累计故障次数
from qt_smartreport.qtr_week_sys_error_mtbf_his t1
-- part2:筛选周范围内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join 
(select
t1.date_value,
COALESCE(t1.alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct case when date(DATE_SUB(t1.start_time,INTERVAL WEEKDAY(t1.start_time) + 0 DAY))=t1.date_value then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and date(DATE_SUB(t1.end_time,INTERVAL WEEKDAY(t1.end_time) + 0 DAY))=t1.date_value then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and date(DATE_SUB(t1.end_time,INTERVAL WEEKDAY(t1.end_time) + 0 DAY))=t1.date_value then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time -- 已结束故障时长（秒）
from
(select t.date_value,t.error_id,bn.start_time,bn.end_time,bn.alarm_service
from qt_smartreport.qtr_week_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value BETWEEN @start_time and @end_time)t1
group by t1.date_value,t1.alarm_service
with ROLLUP)t2 on t2.alarm_service_name=t1.alarm_service and t2.date_value=t1.date_value
-- part3:筛选周范围内的新增订单量、新增任务量
left join 
(select
date(DATE_SUB(tor.create_time,INTERVAL WEEKDAY(tor.create_time) + 0 DAY)) as date_value,
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time
group by date_value)t3 on t3.date_value=t1.date_value
where t1.date_value between @start_time and @end_time