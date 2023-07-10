-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间

select
@now_time as create_time,
@now_time as update_time,
date(@dt_hour_start_time) as date_value,
DATE_FORMAT(@dt_hour_start_time, '%Y-%m-%d %H:00:00.000000') as hour_start_time,
DATE_FORMAT(@dt_next_hour_start_time, '%Y-%m-%d %H:00:00.000000') as  next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as decimal(20,10)) as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.basic_robot)br		
-- part2:机器人理论运行时间		
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration  -- 理论运行时长（秒）
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
UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST(@dt_next_hour_start_time,@now_time)))-UNIX_TIMESTAMP(@dt_hour_start_time) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）		
from 
-- 找到每个机器人计算小时之前的最后一条状态变化数据
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < @dt_hour_start_time
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算小时之内的第一条状态变化数据
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code
union all 
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
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from 
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history 
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time)t4 
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code
)t1 on t1.robot_code=br.robot_code
-- part3: 计算故障次数、故障时长
left join 
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,  -- 该小时故障时长
count(distinct error_id) as error_num  -- 该小时故障次数
from 
(select 
t.error_id,
t.error_code,
t.start_time,
t.end_time,
t.robot_code,
t.stat_start_time,
t.stat_end_time
from 
(select 
t1.*,
case when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
from 
(select 
t.*,
if(@uid = t.robot_error_str, @rank := @rank + 1, @rank := 1) as rank,
@uid:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location, 
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, 
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= @dt_next_hour_start_time then LEAST(@dt_next_hour_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uid := null, @cid := null, @rank := 0) r)t1  

left join 
(select 
t.*,
if(@uidd = t.robot_error_str, @rankk := @rankk + 1, @rankk := 1) as rankk,
@uidd:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location, 
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, 
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= @dt_next_hour_start_time then LEAST(@dt_next_hour_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uidd := null, @cidd := null, @rankk := 0) r)t2
on t2.robot_error_str = t1.robot_error_str and t1.rank - 1 = t2.rankk)t 
where t.xy_distance is null
or t.xy_distance > 1000)t
group by robot_code
)t2 on t2.robot_code=br.robot_code		
-- part4:计算累计故障次数  			
left join 
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from 
(select robot_code,error_id
FROM qt_smartreport.qtr_hour_robot_error_list_his
where hour_start_time < @dt_hour_start_time
union all 
select 
t.robot_code,
t.error_id
from 
(select 
t1.*,
case when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
from 
(select 
t.*,
if(@uid2 = t.robot_error_str, @rank2 := @rank2 + 1, @rank2 := 1) as rank2,
@uid2:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location, 
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, 
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= @dt_next_hour_start_time then LEAST(@dt_next_hour_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uid2 := null, @cid2 := null, @rank2 := 0) r)t1  

left join 
(select 
t.*,
if(@uidd2 = t.robot_error_str, @rankk2 := @rankk2 + 1, @rankk2 := 1) as rankk2,
@uidd2:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location, 
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, 
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= @dt_next_hour_start_time then LEAST(@dt_next_hour_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uidd2 := null, @cidd2 := null, @rankk2 := 0) r)t2
on t2.robot_error_str = t1.robot_error_str and t1.rank2 - 1 = t2.rankk2)t 
where t.xy_distance is null
   or t.xy_distance > 1000)t 
group by robot_code)t3 on t3.robot_code=br.robot_code	
-- part5:			
left join 
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该小时之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该小时之前累计故障时长
from qt_smartreport.qtr_hour_robot_error_mtbf_his
where hour_start_time < @dt_hour_start_time
group by robot_code)t4 on t4.robot_code=br.robot_code





-- part2：sqlserver逻辑

-- sqlserver时间参数
declare @now_time as datetime=sysdatetime() 
declare @dt_hour_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd HH:00:00')
declare @dt_next_hour_start_time as datetime=FORMAT(DATEADD(hh,1,sysdatetime()),'yyyy-MM-dd HH:00:00')
declare @dt_day_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd 00:00:00')
declare @dt_next_day_start_time as datetime=FORMAT(DATEADD(dd,1,sysdatetime()),'yyyy-MM-dd 00:00:00')
declare @dt_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),0),'yyyy-MM-dd 00:00:00')
declare @dt_next_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),7),'yyyy-MM-dd 00:00:00')


select
@now_time as create_time,
@now_time as update_time,
FORMAT(cast(@dt_hour_start_time as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast(@dt_hour_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast(@dt_next_hour_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then cast((COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0)) as decimal)/COALESCE(t2.error_num,0) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then cast(((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0))) as decimal)/COALESCE(t3.accum_error_num,0) else null end  as decimal(20,10)) as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.dbo.basic_robot)br		
-- part2:机器人理论运行时间		
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration  -- 理论运行时长（秒）
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
t2.duration / cast(1000 as decimal) as                           duration,
case when @dt_next_hour_start_time <= @now_time then DATEDIFF(ms,@dt_hour_start_time,coalesce(t3.the_hour_first_create_time, @dt_next_hour_start_time))/cast(1000 as decimal) else DATEDIFF(ms,@dt_hour_start_time,coalesce(t3.the_hour_first_create_time, @now_time))/cast(1000 as decimal) end as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）		
-- UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST(@dt_next_hour_start_time,@now_time)))-UNIX_TIMESTAMP(@dt_hour_start_time) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）		
from 
-- 找到每个机器人计算小时之前的最后一条状态变化数据
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.dbo.robot_state_history
where create_time < @dt_hour_start_time
group by robot_code)t1 
left join phoenix_rms.dbo.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算小时之内的第一条状态变化数据
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.dbo.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code
union all 
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
t4.duration / cast(1000 as decimal) as           duration,
case when t5.the_hour_last_id is not null and @dt_next_hour_start_time <= @now_time then DATEDIFF(ms,t4.create_time,@dt_next_hour_start_time)/cast(1000 as decimal) when t5.the_hour_last_id is not null and @dt_next_hour_start_time > @now_time then DATEDIFF(ms,t4.create_time,@now_time)/cast(1000 as decimal) else t4.duration / cast(1000 as decimal) end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
-- case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from 
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.dbo.robot_state_history 
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time)t4 
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.dbo.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code
)t1 on t1.robot_code=br.robot_code
-- part3: 计算故障次数、故障时长
left join 
(select robot_code,
sum(DATEDIFF(ms,stat_start_time,stat_end_time)/cast(1000 as decimal)) as error_duration,  -- 该小时故障时长
count(distinct error_id) as error_num  -- 该小时故障次数
from 
(select
t1.error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
t1.stat_start_time,
t1.stat_end_time
from 
(select 
t.*,
case when x1 is not null and y1 is not null and x2 is not null and y2 is not null then SQRT(POWER(cast(x2 as int)-cast(x1 as int),2)+POWER(cast(y2 as int)-cast(y1 as int),2))  end xy_distance
from 
(select 
t.*,
lag(x,1) over(partition by robot_code,error_code order by start_time asc) as x1,
lag(y,1) over(partition by robot_code,error_code order by start_time asc) as y1,
x as x2,
y as y2
from 
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
t1.point_location,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y,
case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= (case when @dt_next_hour_start_time <= @now_time then @dt_next_hour_start_time else @now_time end) then (case when @dt_next_hour_start_time <= @now_time then @dt_next_hour_start_time else @now_time end) else t1.end_time end as stat_end_time  -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
--  case when t1.end_time is null or t1.end_time >= @dt_next_hour_start_time then LEAST(@dt_next_hour_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.dbo.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.dbo.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t)t)t1 
where t1.xy_distance is null or t1.xy_distance > 1000)t
group by robot_code
)t2 on t2.robot_code=br.robot_code		
-- part4:计算累计故障次数  			
left join 
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from 
(select robot_code,error_id
FROM qt_smartreport.dbo.qtr_hour_robot_error_list_his
where hour_start_time < @dt_hour_start_time
union all 
select 
t1.robot_code,
t1.id as error_id
from 
(select 
t.*,
case when x1 is not null and y1 is not null and x2 is not null and y2 is not null then SQRT(POWER(cast(x2 as int)-cast(x1 as int),2)+POWER(cast(y2 as int)-cast(y1 as int),2))  end xy_distance
from 
(select 
t.*,
lag(x,1) over(partition by robot_code,error_code order by start_time asc) as x1,
lag(y,1) over(partition by robot_code,error_code order by start_time asc) as y1,
x as x2,
y as y2
from 
(select
t1.*,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y
from (select *
      from phoenix_basic.dbo.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.dbo.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t)t)t1
where t1.xy_distance is null or t1.xy_distance > 1000)t 
group by robot_code)t3 on t3.robot_code=br.robot_code	
-- part5:			
left join 
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该小时之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该小时之前累计故障时长
from qt_smartreport.dbo.qtr_hour_robot_error_mtbf_his
where hour_start_time < @dt_hour_start_time
group by robot_code)t4 on t4.robot_code=br.robot_code





-- part3：异步表兼容逻辑


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00") %}  -- dt所在小时的下一个小时的开始时间

{% if db_type=="MYSQL" %}
-- mysql逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_hour_start_time }}) as date_value,
DATE_FORMAT({{ dt_hour_start_time }}, '%Y-%m-%d %H:00:00.000000') as hour_start_time,
DATE_FORMAT({{ dt_next_hour_start_time }}, '%Y-%m-%d %H:00:00.000000') as  next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as decimal(20,10)) as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.basic_robot)br
-- part2:机器人理论运行时间
left join
(select
ts.robot_code,
sum(stat_state_duration) as theory_run_duration  -- 理论运行时长（秒）
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
UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST({{ dt_next_hour_start_time }},{{ now_time }})))-UNIX_TIMESTAMP({{ dt_hour_start_time }}) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
from
-- 找到每个机器人计算小时之前的最后一条状态变化数据
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {{ dt_hour_start_time }}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算小时之内的第一条状态变化数据
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t3 on t3.robot_code=t1.robot_code
union all
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
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }})t4
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code
)t1 on t1.robot_code=br.robot_code
-- part3: 计算故障次数、故障时长
left join
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,  -- 该小时故障时长
count(distinct error_id) as error_num  -- 该小时故障次数
from
(select
t.error_id,
t.error_code,
t.start_time,
t.end_time,
t.robot_code,
t.stat_start_time,
t.stat_end_time
from
(select
t1.*,
case when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
from
(select
t.*,
if(@uid = t.robot_error_str, @rank := @rank + 1, @rank := 1) as rank,
@uid:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location,
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x,
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= {{ dt_next_hour_start_time }} then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uid := null, @cid := null, @rank := 0) r)t1

left join
(select
t.*,
if(@uidd = t.robot_error_str, @rankk := @rankk + 1, @rankk := 1) as rankk,
@uidd:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location,
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x,
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= {{ dt_next_hour_start_time }} then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uidd := null, @cidd := null, @rankk := 0) r)t2
on t2.robot_error_str = t1.robot_error_str and t1.rank - 1 = t2.rankk)t
where t.xy_distance is null
or t.xy_distance > 1000)t
group by robot_code
)t2 on t2.robot_code=br.robot_code
-- part4:计算累计故障次数
left join
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from
(select robot_code,error_id
FROM qt_smartreport.qtr_hour_robot_error_list_his
where hour_start_time < {{ dt_hour_start_time }}
union all
select
t.robot_code,
t.error_id
from
(select
t1.*,
case when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
from
(select
t.*,
if(@uid2 = t.robot_error_str, @rank2 := @rank2 + 1, @rank2 := 1) as rank2,
@uid2:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location,
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x,
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= {{ dt_next_hour_start_time }} then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uid2 := null, @cid2 := null, @rank2 := 0) r)t1

left join
(select
t.*,
if(@uidd2 = t.robot_error_str, @rankk2 := @rankk2 + 1, @rankk2 := 1) as rankk2,
@uidd2:=t.robot_error_str AS robot_error_str_b
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
concat(t1.robot_code, '-', t1.error_code) as robot_error_str,
t1.point_location,
substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x,
substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= {{ dt_next_hour_start_time }} then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					order by robot_error_str asc, t1.start_time asc)t,(select @uidd2 := null, @cidd2 := null, @rankk2 := 0) r)t2
on t2.robot_error_str = t1.robot_error_str and t1.rank2 - 1 = t2.rankk2)t
where t.xy_distance is null
   or t.xy_distance > 1000)t
group by robot_code)t3 on t3.robot_code=br.robot_code
-- part5:
left join
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该小时之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该小时之前累计故障时长
from qt_smartreport.qtr_hour_robot_error_mtbf_his
where hour_start_time < {{ dt_hour_start_time }}
group by robot_code)t4 on t4.robot_code=br.robot_code
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then cast((COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0)) as decimal)/COALESCE(t2.error_num,0) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then cast(((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0))) as decimal)/COALESCE(t3.accum_error_num,0) else null end  as decimal(20,10)) as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.basic_robot)br
-- part2:机器人理论运行时间
left join
(select
ts.robot_code,
sum(stat_state_duration) as theory_run_duration  -- 理论运行时长（秒）
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
t2.duration / cast(1000 as decimal) as                           duration,
case when {{ dt_next_hour_start_time }} <= {{ now_time }} then DATEDIFF(ms,{{ dt_hour_start_time }},coalesce(t3.the_hour_first_create_time, {{ dt_next_hour_start_time }}))/cast(1000 as decimal) else DATEDIFF(ms,{{ dt_hour_start_time }},coalesce(t3.the_hour_first_create_time, {{ now_time }}))/cast(1000 as decimal) end as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
-- UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST({{ dt_next_hour_start_time }},{{ now_time }})))-UNIX_TIMESTAMP({{ dt_hour_start_time }}) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
from
-- 找到每个机器人计算小时之前的最后一条状态变化数据
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {{ dt_hour_start_time }}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算小时之内的第一条状态变化数据
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t3 on t3.robot_code=t1.robot_code
union all
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
t4.duration / cast(1000 as decimal) as           duration,
case when t5.the_hour_last_id is not null and {{ dt_next_hour_start_time }} <= {{ now_time }} then DATEDIFF(ms,t4.create_time,{{ dt_next_hour_start_time }})/cast(1000 as decimal) when t5.the_hour_last_id is not null and {{ dt_next_hour_start_time }} > {{ now_time }} then DATEDIFF(ms,t4.create_time,{{ now_time }})/cast(1000 as decimal) else t4.duration / cast(1000 as decimal) end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
-- case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }})t4
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code
)t1 on t1.robot_code=br.robot_code
-- part3: 计算故障次数、故障时长
left join
(select robot_code,
sum(DATEDIFF(ms,stat_start_time,stat_end_time)/cast(1000 as decimal)) as error_duration,  -- 该小时故障时长
count(distinct error_id) as error_num  -- 该小时故障次数
from
(select
t1.error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
t1.stat_start_time,
t1.stat_end_time
from
(select
t.*,
case when x1 is not null and y1 is not null and x2 is not null and y2 is not null then SQRT(POWER(cast(x2 as int)-cast(x1 as int),2)+POWER(cast(y2 as int)-cast(y1 as int),2))  end xy_distance
from
(select
t.*,
lag(x,1) over(partition by robot_code,error_code order by start_time asc) as x1,
lag(y,1) over(partition by robot_code,error_code order by start_time asc) as y1,
x as x2,
y as y2
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
t1.point_location,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y,
case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= (case when {{ dt_next_hour_start_time }} <= {{ now_time }} then {{ dt_next_hour_start_time }} else {{ now_time }} end) then (case when {{ dt_next_hour_start_time }} <= {{ now_time }} then {{ dt_next_hour_start_time }} else {{ now_time }} end) else t1.end_time end as stat_end_time  -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
--  case when t1.end_time is null or t1.end_time >= {{ dt_next_hour_start_time }} then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t)t)t1
where t1.xy_distance is null or t1.xy_distance > 1000)t
group by robot_code
)t2 on t2.robot_code=br.robot_code
-- part4:计算累计故障次数
left join
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from
(select robot_code,error_id
FROM qt_smartreport.qtr_hour_robot_error_list_his
where hour_start_time < {{ dt_hour_start_time }}
union all
select
t1.robot_code,
t1.id as error_id
from
(select
t.*,
case when x1 is not null and y1 is not null and x2 is not null and y2 is not null then SQRT(POWER(cast(x2 as int)-cast(x1 as int),2)+POWER(cast(y2 as int)-cast(y1 as int),2))  end xy_distance
from
(select
t.*,
lag(x,1) over(partition by robot_code,error_code order by start_time asc) as x1,
lag(y,1) over(partition by robot_code,error_code order by start_time asc) as y1,
x as x2,
y as y2
from
(select
t1.*,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t)t)t1
where t1.xy_distance is null or t1.xy_distance > 1000)t
group by robot_code)t3 on t3.robot_code=br.robot_code
-- part5:
left join
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该小时之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该小时之前累计故障时长
from qt_smartreport.qtr_hour_robot_error_mtbf_his
where hour_start_time < {{ dt_hour_start_time }}
group by robot_code)t4 on t4.robot_code=br.robot_code
{% endif %}