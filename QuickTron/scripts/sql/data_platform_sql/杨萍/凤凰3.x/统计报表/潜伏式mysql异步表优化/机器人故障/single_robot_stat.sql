-- 用于：统计报表->故障异常统计->机器人故障统计->单个机器人故障统计

-- 新增故障次数 ：create_robot_error_num
-- 故障率（搬运作业单） ：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_robot_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- MTTR = end_error_time/end_robot_error_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- 当期MTBF = (theory_run_duration-error_duration)/robot_error_num

SELECT
br.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(t1.create_robot_error_num,0) as create_robot_error_num,   -- 新增故障次数
COALESCE(t1.end_robot_error_num,0) as end_robot_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as  end_error_time,           -- 结束故障时间
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.robot_error_num,0) as  robot_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.theory_run_duration,0) as  theory_run_duration,           -- 理论运行时长
COALESCE(t3.error_duration,0) as  error_duration        -- 持续故障时长
-- 机器人
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using'  and brt.robot_type_code = {robot_type_code}
-- part1:筛选小时范围内机器人新增故障次数、结束故障次数、已结束故障时长、时间段内有持续的故障次数、故障持续时长
left join
(select
t1.robot_code,
count(distinct case when t1.start_time BETWEEN { start_time } and { end_time } then t1.error_id end) as create_robot_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } then t1.error_id end) as end_robot_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN { start_time } and { end_time } and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
count(distinct t1.error_id) as robot_error_num  -- 时间段内有持续的故障次数
from
(select t.robot_code,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qtr_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.hour_start_time BETWEEN { start_time } and { end_time })t1
group by t1.robot_code)t1 on t1.robot_code=br.robot_code
-- part2:所筛选小时范围内 新增作业单数、机器人任务数
left join
(select
tocj.robot_code,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN { start_time } and { end_time }
group by tocj.robot_code)t2 on t2.robot_code=br.robot_code
-- part3:所筛选小时范围内机器人理论运行时长、故障持续时长
left join
(select robot_code,
sum(theory_run_duration) as theory_run_duration, -- 机器人理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒）
from qt_smartreport.qtr_hour_robot_error_mtbf_his
where hour_start_time between { start_time } and { end_time }
group by robot_code)t3 on t3.robot_code=br.robot_code



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


-- 新增故障次数 ：create_robot_error_num
-- 故障率（搬运作业单） ：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_robot_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- MTTR = end_error_time/end_robot_error_num
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- 当期MTBF = (theory_run_duration-error_duration)/robot_error_num

SELECT 
br.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(t1.create_robot_error_num,0) as create_robot_error_num,   -- 新增故障次数
COALESCE(t1.end_robot_error_num,0) as end_robot_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as  end_error_time,           -- 结束故障时间
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.robot_error_num,0) as  robot_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.theory_run_duration,0) as  theory_run_duration,           -- 理论运行时长
COALESCE(t3.error_duration,0) as  error_duration        -- 持续故障时长
-- 机器人
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using'  and brt.robot_type_code = {robot_type_code}
-- part1:筛选小时范围内机器人新增故障次数、结束故障次数、已结束故障时长、时间段内有持续的故障次数、故障持续时长
left join 
(select 
t1.robot_code,
count(distinct case when t1.start_time BETWEEN @start_time and @end_time then t1.error_id end) as create_robot_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time then t1.error_id end) as end_robot_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time BETWEEN @start_time and @end_time and date_format(t1.end_time, '%Y-%m-%d %H:00:00')=date_format(t1.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
count(distinct t1.error_id) as robot_error_num  -- 时间段内有持续的故障次数
from 
(select t.robot_code,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time 
from qt_smartreport.qtr_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id 
where t.hour_start_time BETWEEN @start_time and @end_time)t1 
group by t1.robot_code)t1 on t1.robot_code=br.robot_code					
-- part2:所筛选小时范围内 新增作业单数、机器人任务数
left join 
(select
tocj.robot_code,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time
group by tocj.robot_code)t2 on t2.robot_code=br.robot_code	
-- part3:所筛选小时范围内机器人理论运行时长、故障持续时长
left join 
(select robot_code,
sum(theory_run_duration) as theory_run_duration, -- 机器人理论运行时长（秒）
sum(error_duration) as error_duration -- 故障持续时长（秒） 
from qt_smartreport.qtr_hour_robot_error_mtbf_his 
where hour_start_time between @start_time and @end_time
group by robot_code)t3 on t3.robot_code=br.robot_code
