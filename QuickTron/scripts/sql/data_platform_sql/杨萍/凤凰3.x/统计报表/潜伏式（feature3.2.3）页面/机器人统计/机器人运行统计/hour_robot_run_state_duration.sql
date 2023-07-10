# 当天之前的数据
select 
       t.hour_start_time as hour_value,
       t.robot_code,
       t.robot_type_code,
       t.robot_type_name,
       t.uptime_state_rate,
       t.uptime_state_duration,
       t.uptime_state_rate_fenmu,
       t.utilization_rate,
       t.utilization_duration,
       t.utilization_rate_fenmu,
       t.loading_busy_state_duration,
       t.empty_busy_state_duration,
       t.charging_state_duration,
       t.idle_state_duration,
       t.locked_state_duration,
       t.error_state_duration,
       t.offline_duration
from qt_smartreport.qt_hour_robot_state_duration_stat_his t
         inner join (select distinct robot_code from phoenix_basic.basic_robot where usage_state = 'using') br
                    on br.robot_code = t.robot_code
where t.hour_start_time BETWEEN {start_time} and {end_time}			
union all 
# 当天当前小时之前的数据
select 
       t.hour_start_time as hour_value,
       t.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,	   
       COALESCE(t.uptime_state_duration, 0) / 3600    as uptime_state_rate,
       t.uptime_state_duration,
	   3600 as uptime_state_rate_fenmu,
       COALESCE(t.loading_busy_state_duration, 0) / 3600         as utilization_rate,	   
       t.loading_busy_state_duration as utilization_duration,
	   3600 as utilization_rate_fenmu,
	   t.loading_busy_state_duration,
       t.empty_busy_state_duration,
       t.charging_state_duration,
       t.idle_state_duration,	 
       t.locked_state_duration,	   
       t.error_state_duration,	   
       t.offline_duration
from qt_smartreport.qt_hour_robot_state_duration_his t 
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_start_time BETWEEN {now_start_time} and {now_end_time}		
and t.hour_start_time BETWEEN {start_time} and {end_time}		
union all
# 当前小时的数据
select 
date_format({now_hour_start_time}, '%Y-%m-%d %H:00:00') as hour_value,
br.robot_code,
brt.robot_type_code,
brt.robot_type_name,	 
COALESCE(t1.uptime_state_duration, 0)/(UNIX_TIMESTAMP(sysdate())-UNIX_TIMESTAMP({now_hour_start_time})) as uptime_state_rate,
COALESCE(t1.uptime_state_duration,0) as uptime_state_duration,
UNIX_TIMESTAMP(sysdate())-UNIX_TIMESTAMP({now_hour_start_time}) as uptime_state_rate_fenmu,
COALESCE(t1.loading_busy_state_duration, 0) / (UNIX_TIMESTAMP(sysdate())-UNIX_TIMESTAMP({now_hour_start_time})) as utilization_rate,	
COALESCE(t1.loading_busy_state_duration,0) as utilization_duration,
UNIX_TIMESTAMP(sysdate())-UNIX_TIMESTAMP({now_hour_start_time}) as utilization_rate_fenmu,
COALESCE(t1.loading_busy_state_duration,0) as loading_busy_state_duration,
COALESCE(t1.empty_busy_state_duration,0) as empty_busy_state_duration,
COALESCE(t1.charging_state_duration,0) as charging_state_duration,
COALESCE(t1.idle_state_duration,0) as idle_state_duration,
COALESCE(t1.locked_state_duration,0) as locked_state_duration,
COALESCE(t1.error_state_duration,0) as error_state_duration,
(UNIX_TIMESTAMP(sysdate())-UNIX_TIMESTAMP({now_hour_start_time}))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  # 离线时长
from phoenix_basic.basic_robot br 
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id 
inner join 
(select 
ts.robot_code,
sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,  # 开动时长
sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,  # 利用时长
sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,   # 空闲作业时长
sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,  # 搬运作业时长
sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration, # 充电时长
sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,  # 空闲时长
sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,  #  锁定时长
sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration   # 异常时长
from 
(select 
t.robot_code,
t.state_id,
t.online_state,
t.work_state,
t.job_sn,
case when (t.is_error != 1 and t.work_state in ('BUSY', 'CHARGING')) or ((t.work_state = 'ERROR' or t.is_error = 1) and t.job_sn is not null)then 1 else 0 end  as   is_uptime_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1 else 0 end as is_loading_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1 else 0 end as is_empty_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end as  is_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end as  is_idle_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1 else 0 end as is_charging_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1 else 0 end as is_locked_state,
case when t.is_error = 1 or t.work_state = 'ERROR' then 1 else 0 end  as  is_error_state,
t.duration,
t.stat_state_duration	as  stat_duration			 				   
from 
(select 
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error,
t2.duration / 1000 as                           duration,
case when sysdate() < {now_next_hour_start_time} then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) - UNIX_TIMESTAMP({now_hour_start_time}) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, {now_next_hour_start_time})) - UNIX_TIMESTAMP({now_hour_start_time}) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < {now_hour_start_time}
group by robot_code)t1             # 找到每个机器人此小时前最后一条记录
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time}
group by robot_code)t3 on t3.robot_code=t1.robot_code    # 找到每个机器人此小时第一条记录
# 判断小时内最开始的一段时间内的状态
union all 
# 判断小时内发生的记录的持续时长
select 
t4.robot_code,	   
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error,
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and sysdate() >= {now_next_hour_start_time} then UNIX_TIMESTAMP({now_next_hour_start_time})-UNIX_TIMESTAMP(t4.create_time) when t5.the_hour_last_id is not null and sysdate() < {now_next_hour_start_time} then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end stat_state_duration
# 如果是小时内最后一条记录，时长要另做处理
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time})t4  # 小时内的所有记录
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id  # 定位每个机器人小时内的最后一条记录
)t 
left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn
)ts 
group by ts.robot_code)t1 on t1.robot_code=br.robot_code
where br.usage_state = 'using'