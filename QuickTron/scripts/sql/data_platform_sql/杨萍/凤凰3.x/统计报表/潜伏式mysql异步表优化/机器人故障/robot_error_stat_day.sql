-- 用于：统计报表->故障异常统计->机器人故障趋势统计->天

-- 新增故障次数：create_robot_error_num
-- 故障率（搬运作业单）：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示
-- 搬运作业单量： create_order_num  （在order_job_num_day.sql文件）
-- 故障率（机器人任务）：使用 create_robot_error_num 和 create_job_num   注意数据的3种展示
-- 机器人任务量：create_job_num  （在order_job_num_day.sql文件）
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- MTTR = end_robot_error_time/end_robot_error_num
-- 当期MTBF = (theory_run_duration-error_duration)/error_num
-- 累计MTBF = (accum_theory_run_duration-accum_error_duration)/accum_error_num


select
'day' as stat_time_type,  -- 天
date_format(tr.date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
tr.robot_code,
brt.robot_type_code,
brt.robot_type_name,
sum(create_robot_error_num) as create_robot_error_num,  -- 新增故障次数
sum(end_robot_error_num)    as end_robot_error_num,  -- 结束故障次数
sum(end_robot_error_time)   as end_robot_error_time, -- 已结束故障时长
sum(theory_run_duration)        as theory_run_duration,  -- 机器人理论运行时长
sum(error_duration)       as error_duration,  -- 机器人故障时长
sum(error_num) as error_num,   -- 机器人故障次数
sum(accum_theory_run_duration) as accum_theory_run_duration,  --  机器人累计理论运行时长
sum(accum_error_duration) as accum_error_duration,  -- 机器人累计故障时长
sum(accum_error_num) as accum_error_num   -- 机器人累计故障次数
from
(
-- part1:筛选天机器人新增故障次数、结束故障次数、已结束故障时长
select
tb.robot_code,
tb.date_value,
count(distinct case when date(tb.start_time)=tb.date_value then tb.error_id end) as create_robot_error_num,   -- 新增故障次数
count(distinct case when tb.end_time is not null and date(tb.end_time)=tb.date_value then tb.error_id end) as end_robot_error_num, -- 结束故障次数
sum(case when tb.end_time is not null and date(tb.end_time)=tb.date_value then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_robot_error_time,  -- 已结束故障时长（秒）
null as theory_run_duration,   -- 机器人理论运行时长
null as error_duration,        -- 机器人故障时长
null as error_num,             -- 机器人故障次数
null as accum_theory_run_duration,  --  机器人累计理论运行时长
null as accum_error_duration,      -- 机器人累计故障时长
null as accum_error_num             -- 机器人累计故障次数
from
(select t.robot_code,t.date_value,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qtr_day_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value BETWEEN { start_time } and { end_time })tb
inner join phoenix_basic.basic_robot br on br.robot_code=tb.robot_code and br.usage_state = 'using'
group by tb.robot_code,tb.date_value
-- part2:所筛选天机器人理论运行时长、机器人故障时长、机器人故障次数、机器人累计理论运行时长、机器人累计故障时长、机器人累计故障次数
union all
select
robot_code,
date_value,
null as create_robot_error_num,
null as end_robot_error_num,
null as end_robot_error_time,
theory_run_duration,   -- 机器人理论运行时长
error_duration,        -- 机器人故障时长
error_num,             -- 机器人故障次数
accum_theory_run_duration,  --  机器人累计理论运行时长
accum_error_duration,      -- 机器人累计故障时长
accum_error_num             -- 机器人累计故障次数
from qt_smartreport.qtr_day_robot_error_mtbf_his
where date_value between { start_time } and { end_time })tr
inner join phoenix_basic.basic_robot br on br.robot_code = tr.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by stat_time_value, tr.robot_code, brt.robot_type_code, brt.robot_type_name




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


-- 新增故障次数：create_robot_error_num
-- 故障率（搬运作业单）：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示  
-- 搬运作业单量： create_order_num  （在order_job_num_day.sql文件）
-- 故障率（机器人任务）：使用 create_robot_error_num 和 create_job_num   注意数据的3种展示
-- 机器人任务量：create_job_num  （在order_job_num_day.sql文件）
-- OEE = (theory_run_duration-error_duration)/theory_run_duration
-- MTTR = end_robot_error_time/end_robot_error_num
-- 当期MTBF = (theory_run_duration-error_duration)/error_num
-- 累计MTBF = (accum_theory_run_duration-accum_error_duration)/accum_error_num


select
'day' as stat_time_type,  -- 天
date_format(tr.date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
tr.robot_code,
brt.robot_type_code,
brt.robot_type_name,
sum(create_robot_error_num) as create_robot_error_num,  -- 新增故障次数
sum(end_robot_error_num)    as end_robot_error_num,  -- 结束故障次数
sum(end_robot_error_time)   as end_robot_error_time, -- 已结束故障时长
sum(theory_run_duration)        as theory_run_duration,  -- 机器人理论运行时长
sum(error_duration)       as error_duration,  -- 机器人故障时长
sum(error_num) as error_num,   -- 机器人故障次数
sum(accum_theory_run_duration) as accum_theory_run_duration,  --  机器人累计理论运行时长
sum(accum_error_duration) as accum_error_duration,  -- 机器人累计故障时长
sum(accum_error_num) as accum_error_num   -- 机器人累计故障次数
from
(
-- part1:筛选天机器人新增故障次数、结束故障次数、已结束故障时长
select
tb.robot_code,
tb.date_value,
count(distinct case when date(tb.start_time)=tb.date_value then tb.error_id end) as create_robot_error_num,   -- 新增故障次数
count(distinct case when tb.end_time is not null and date(tb.end_time)=tb.date_value then tb.error_id end) as end_robot_error_num, -- 结束故障次数
sum(case when tb.end_time is not null and date(tb.end_time)=tb.date_value then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_robot_error_time,  -- 已结束故障时长（秒）
null as theory_run_duration,   -- 机器人理论运行时长
null as error_duration,        -- 机器人故障时长
null as error_num,             -- 机器人故障次数
null as accum_theory_run_duration,  --  机器人累计理论运行时长
null as accum_error_duration,      -- 机器人累计故障时长
null as accum_error_num             -- 机器人累计故障次数
from
(select t.robot_code,t.date_value,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qtr_day_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value BETWEEN @start_time and @end_time)tb
inner join phoenix_basic.basic_robot br on br.robot_code=tb.robot_code and br.usage_state = 'using'
group by tb.robot_code,tb.date_value
-- part2:所筛选天机器人理论运行时长、机器人故障时长、机器人故障次数、机器人累计理论运行时长、机器人累计故障时长、机器人累计故障次数
union all 
select 
robot_code,
date_value,
null as create_robot_error_num,
null as end_robot_error_num,
null as end_robot_error_time,
theory_run_duration,   -- 机器人理论运行时长
error_duration,        -- 机器人故障时长
error_num,             -- 机器人故障次数
accum_theory_run_duration,  --  机器人累计理论运行时长
accum_error_duration,      -- 机器人累计故障时长
accum_error_num             -- 机器人累计故障次数
from qt_smartreport.qtr_day_robot_error_mtbf_his 
where date_value between @start_time and @end_time)tr
inner join phoenix_basic.basic_robot br on br.robot_code = tr.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by stat_time_value, tr.robot_code, brt.robot_type_code, brt.robot_type_name