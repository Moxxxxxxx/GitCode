-- 用于：统计报表->故障异常统计->机器人故障趋势统计->天

select
'day' as stat_time_type,  -- 天
date_format(tor.create_time, '%Y-%m-%d 00:00:00') as stat_time_value,
count(distinct tor.order_no)                      as create_order_num, -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num  -- 机器人任务数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
left join phoenix_basic.basic_robot br on br.robot_code=tocj.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tor.create_time BETWEEN { start_time } and { end_time } 
and tocj.robot_code in { robot_code }   -- 机器人类型（也可没有）
and brt.robot_type_code in { robot_type_code }  -- 机器人编码（也可没有）
group by stat_time_value













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


select
'day' as stat_time_type,  -- 天
date_format(tor.create_time, '%Y-%m-%d 00:00:00') as stat_time_value,
count(distinct tor.order_no)                      as create_order_num, -- 新增作业单数
count(distinct tocj.job_sn)                       as create_job_num  -- 机器人任务数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
left join phoenix_basic.basic_robot br on br.robot_code=tocj.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tor.create_time BETWEEN @start_time and @end_time
-- and tocj.robot_code in { robot_code }   -- 机器人类型（也可没有）
-- and brt.robot_type_code in { robot_type_code }  -- 机器人编码（也可没有）
group by stat_time_value