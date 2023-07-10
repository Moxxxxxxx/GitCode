-- 用于：统计报表->项目概览->当日系统故障统计

select
COALESCE(t.create_sys_error_num,0) as breakdown_num,                 -- 故障次数
case
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) =0 then concat(COALESCE(t.create_sys_error_num, 0),'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(COALESCE(t.create_order_num,0)/COALESCE(t.create_sys_error_num,0),0))
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) < COALESCE(t.create_sys_error_num,0) then concat(round(COALESCE(t.create_sys_error_num,0)/COALESCE(t.create_order_num,0),0),'/','1')
end as order_breakdown_rate,-- 故障率（搬运作业单）
COALESCE(t.create_order_num,0) as order_num,  -- 订单量
case
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) =0 then concat(COALESCE(t.create_sys_error_num, 0),'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(COALESCE(t.create_job_num,0)/COALESCE(t.create_sys_error_num,0),0))
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) < COALESCE(t.create_sys_error_num,0) then concat(round(COALESCE(t.create_sys_error_num,0)/COALESCE(t.create_job_num,0),0),'/','1')
end as carry_job_breakdown_rate,      -- 故障率（搬运任务）
COALESCE(t.create_job_num,0) as carry_job_num,  -- 搬运任务数
case when COALESCE(t.theory_run_duration,0) != 0 then round((COALESCE(t.theory_run_duration,0) - COALESCE(t.error_duration,0))/COALESCE(t.theory_run_duration,0),4) else null end as oee,   -- OEE
case when COALESCE(t.end_sys_error_num,0) != 0 then COALESCE(t.end_error_time,0)/COALESCE(t.end_sys_error_num,0) else null end as mttr, -- MTTR
case when COALESCE(t.error_num,0) != 0 then (COALESCE(t.theory_run_duration,0) - COALESCE(t.error_duration,0))/COALESCE(t.error_num,0) else null end as mtbf, -- MTBF
case when COALESCE(t.accum_error_num,0) != 0 then (COALESCE(t.accum_theory_run_duration,0)-COALESCE(t.accum_error_duration,0))/COALESCE(t.accum_error_num,0) else null end as accum_mtbf  -- 累计MTBF

from
(select
max(create_sys_error_num) as create_sys_error_num,
max(end_sys_error_num) as end_sys_error_num,
max(end_error_time) as end_error_time,
max(create_order_num) as create_order_num,
max(create_job_num) as create_job_num,
max(theory_run_duration) as theory_run_duration,
max(error_duration) as error_duration,
max(error_num) as error_num,
max(accum_theory_run_duration) as accum_theory_run_duration,
max(accum_error_duration) as accum_error_duration,
max(accum_error_num) as accum_error_num
from
(
-- part1:当天系统新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when t1.start_time >= { now_start_time } then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time >= { now_start_time } then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time >= { now_start_time } then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
null as create_order_num,
null as create_job_num,
null as theory_run_duration,   -- 理论运行时长
null as error_duration,   -- 故障时长
null as error_num,  -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,  --  累计故障时长
null as accum_error_num   --  累计故障次数
from
(select t.date_value,t.error_id,bn.start_time,bn.end_time
from qt_smartreport.qtr_day_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value >= { now_start_time })t1
-- part2:当天新增作业单数、机器人任务数
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
count(distinct tor.order_no)                      as create_order_num,  -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num,  -- 机器人任务数
null as theory_run_duration,  -- 理论运行时长
null as error_duration,  -- 故障时长
null as error_num,  -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,  -- 累计故障时长
null as accum_error_num   -- 累计故障次数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time >= { now_start_time }
-- part3:当天系统理论运行时长、故障时长、故障次数
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
sum(theory_run_duration) as theory_run_duration,   -- 理论运行时长
sum(error_duration) as error_duration,        -- 故障时长
sum(error_num) as error_num,             -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,      -- 累计故障时长
null as accum_error_num             -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_mtbf_his t
where t.date_value >= { now_start_time }
-- part4:累计理论运行时长、累计故障时长
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
null as theory_run_duration,   -- 理论运行时长
null as error_duration,        -- 故障时长
null as error_num,             -- 故障次数
sum(t.theory_run_duration) as accum_theory_run_duration,  --  累计理论运行时长
sum(t.error_duration) as accum_error_duration,      -- 累计故障时长
null as accum_error_num             -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_mtbf_his t
-- part5:累计故障次数
union all
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
null as theory_run_duration,   -- 理论运行时长
null as error_duration,        -- 故障时长
null as error_num,             -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,      -- 累计故障时长
count(distinct t.error_id) as accum_error_num  -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_list_his t)td )t




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
COALESCE(t.create_sys_error_num,0) as breakdown_num,                 -- 故障次数
case 
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) =0 then concat(COALESCE(t.create_sys_error_num, 0),'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(COALESCE(t.create_order_num,0)/COALESCE(t.create_sys_error_num,0),0))
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_order_num,0) < COALESCE(t.create_sys_error_num,0) then concat(round(COALESCE(t.create_sys_error_num,0)/COALESCE(t.create_order_num,0),0),'/','1')
end as order_breakdown_rate,-- 故障率（搬运作业单）
COALESCE(t.create_order_num,0) as order_num,  -- 订单量
case 
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) =0 then concat(COALESCE(t.create_sys_error_num, 0),'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(COALESCE(t.create_job_num,0)/COALESCE(t.create_sys_error_num,0),0))
when COALESCE(t.create_sys_error_num,0) != 0 and  COALESCE(t.create_job_num,0) < COALESCE(t.create_sys_error_num,0) then concat(round(COALESCE(t.create_sys_error_num,0)/COALESCE(t.create_job_num,0),0),'/','1')
end as carry_job_breakdown_rate,      -- 故障率（搬运任务）
COALESCE(t.create_job_num,0) as carry_job_num,  -- 搬运任务数
case when COALESCE(t.theory_run_duration,0) != 0 then round((COALESCE(t.theory_run_duration,0) - COALESCE(t.error_duration,0))/COALESCE(t.theory_run_duration,0),4) else null end as oee,   -- OEE
case when COALESCE(t.end_sys_error_num,0) != 0 then COALESCE(t.end_error_time,0)/COALESCE(t.end_sys_error_num,0) else null end as mttr, -- MTTR 
case when COALESCE(t.error_num,0) != 0 then (COALESCE(t.theory_run_duration,0) - COALESCE(t.error_duration,0))/COALESCE(t.error_num,0) else null end as mtbf, -- MTBF 
case when COALESCE(t.accum_error_num,0) != 0 then (COALESCE(t.accum_theory_run_duration,0)-COALESCE(t.accum_error_duration,0))/COALESCE(t.accum_error_num,0) else null end as accum_mtbf  -- 累计MTBF 

from 
(select 
max(create_sys_error_num) as create_sys_error_num,
max(end_sys_error_num) as end_sys_error_num,
max(end_error_time) as end_error_time,
max(create_order_num) as create_order_num,
max(create_job_num) as create_job_num,
max(theory_run_duration) as theory_run_duration,
max(error_duration) as error_duration,
max(error_num) as error_num,
max(accum_theory_run_duration) as accum_theory_run_duration,
max(accum_error_duration) as accum_error_duration,
max(accum_error_num) as accum_error_num
from 
(
-- part1:当天系统新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when t1.start_time >= @now_start_time then t1.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when t1.end_time is not null and t1.end_time >= @now_start_time then t1.error_id end) as end_sys_error_num,  -- 结束故障次数
sum(case when t1.end_time is not null and t1.end_time >= @now_start_time then unix_timestamp(t1.end_time)-unix_timestamp(t1.start_time) end) as end_error_time, -- 已结束故障时长（秒）
null as create_order_num,
null as create_job_num,
null as theory_run_duration,   -- 理论运行时长
null as error_duration,   -- 故障时长
null as error_num,  -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,  --  累计故障时长
null as accum_error_num   --  累计故障次数
from
(select t.date_value,t.error_id,bn.start_time,bn.end_time
from qt_smartreport.qtr_day_sys_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value >= @now_start_time)t1
-- part2:当天新增作业单数、机器人任务数
union all 
select
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
count(distinct tor.order_no)                      as create_order_num,  -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num,  -- 机器人任务数
null as theory_run_duration,  -- 理论运行时长
null as error_duration,  -- 故障时长
null as error_num,  -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,  -- 累计故障时长
null as accum_error_num   -- 累计故障次数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time >= @now_start_time
-- part3:当天系统理论运行时长、故障时长、故障次数
union all 
select 
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
sum(theory_run_duration) as theory_run_duration,   -- 理论运行时长
sum(error_duration) as error_duration,        -- 故障时长
sum(error_num) as error_num,             -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,      -- 累计故障时长
null as accum_error_num             -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_mtbf_his t
where t.date_value >= @now_start_time
-- part4:累计理论运行时长、累计故障时长
union all 
select 
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
null as theory_run_duration,   -- 理论运行时长
null as error_duration,        -- 故障时长
null as error_num,             -- 故障次数
sum(t.theory_run_duration) as accum_theory_run_duration,  --  累计理论运行时长
sum(t.error_duration) as accum_error_duration,      -- 累计故障时长
null as accum_error_num             -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_mtbf_his t
-- part5:累计故障次数
union all 
select 
null as create_sys_error_num,
null as end_sys_error_num,
null as end_error_time,
null as create_order_num,  -- 新增作业单数
null as create_job_num,  -- 机器人任务数
null as theory_run_duration,   -- 理论运行时长
null as error_duration,        -- 故障时长
null as error_num,             -- 故障次数
null as accum_theory_run_duration,  --  累计理论运行时长
null as accum_error_duration,      -- 累计故障时长
count(distinct t.error_id) as accum_error_num  -- 累计故障次数
from qt_smartreport.qtr_day_sys_error_list_his t)td )t 