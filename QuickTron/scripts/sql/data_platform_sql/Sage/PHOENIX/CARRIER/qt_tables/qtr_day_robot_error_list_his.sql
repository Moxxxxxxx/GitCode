-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间


select @now_time                as create_time,
       @now_time                as update_time,
       date(@dt_day_start_time) as date_value,
       t.error_id,
       t.error_code,
       t.start_time,
       t.end_time,
       t.warning_spec,
       t.alarm_module,
       t.alarm_service,
       t.alarm_type,
       t.alarm_level,
       t3.alarm_name            as alarm_detail,
       t.param_value,
       t.job_order,
       t.robot_job,
       t.robot_code,
       t.device_code,
       t.server_code,
       t.transport_object,
       t.stat_start_time,
       t.stat_end_time
from (select t1.*,
             case
                 when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null
                     then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
      from (select t.*,
                   if(@uid = t.robot_error_str, @rank := @rank + 1, @rank := 1) as rank,   
@uid:=t.robot_error_str AS robot_error_str_b
            from
                (select
                t1.id as error_id, t1.robot_code, t1.error_code, concat(t1.robot_code, '-', t1.error_code) as robot_error_str, t1.start_time, t1.end_time, t1.warning_spec, t1.alarm_module, t1.alarm_service, t1.alarm_type, t1.alarm_level, t3.alarm_name as alarm_detail, t1.param_value, t1.job_order, t1.robot_job, t1.device_code, t1.server_code, t1.transport_object, t1.point_location, substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y, GREATEST(t1.start_time, @dt_day_start_time) as stat_start_time, case when t1.end_time is null or t1.end_time >= LEAST(@dt_next_day_start_time, @now_time) then LEAST(@dt_next_day_start_time, @now_time) else t1.end_time end as stat_end_time
                from (select *
                from phoenix_basic.basic_notification
                where alarm_module = 'robot'
                and alarm_level >= 3
                and (
                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce (end_time, @now_time ) < @dt_next_day_start_time ) or
                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce (end_time, @now_time ) >= @dt_next_day_start_time ) or
                (start_time < @dt_day_start_time and coalesce (end_time, @now_time) >= @dt_next_day_start_time and coalesce (end_time, @now_time) < @dt_next_day_start_time) or
                (start_time < @dt_day_start_time and coalesce (end_time, @now_time) >= @dt_next_day_start_time)
                )) t1
                -- 注意：一定是用的inner join 
                inner join (select robot_code, COALESCE (DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time, min(id) as first_error_id
                from phoenix_basic.basic_notification
                where alarm_module = 'robot'
                and alarm_level >= 3
                and (
                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce (end_time, @now_time ) < @dt_next_day_start_time ) or
                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce (end_time, @now_time ) >= @dt_next_day_start_time ) or
                (start_time < @dt_day_start_time and coalesce (end_time, @now_time) >= @dt_next_day_start_time and coalesce (end_time, @now_time) < @dt_next_day_start_time) or
                (start_time < @dt_day_start_time and coalesce (end_time, @now_time) >= @dt_next_day_start_time)
                )
                group by robot_code, COALESCE (DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
                left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code
                order by robot_error_str asc, t1.start_time asc) t, (select @uid := null, @cid := null, @rank := 0) r) t1
               left join
           (select t.*,
                   if(@uidd = t.robot_error_str, @rankk := @rankk + 1, @rankk := 1) as rankk,
                   @uidd := t.robot_error_str                                       AS robot_error_str_b

            from (select t1.id                                                                                   as error_id,
                         t1.robot_code,
                         t1.error_code,
                         concat(t1.robot_code, '-', t1.error_code)                                               as robot_error_str,
                         t1.start_time,
                         t1.end_time,
                         t1.warning_spec,
                         t1.alarm_module,
                         t1.alarm_service,
                         t1.alarm_type,
                         t1.alarm_level,
                         t3.alarm_name                                                                           as alarm_detail,
                         t1.param_value,
                         t1.job_order,
                         t1.robot_job,
                         t1.device_code,
                         t1.server_code,
                         t1.transport_object,
                         t1.point_location,
                         substring_index(substring_index(t1.point_location, "x=", -1), ",", 1)                   as x,
                         substring_index(substring_index(replace(t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
                         GREATEST(t1.start_time, @dt_day_start_time)                                             as stat_start_time,
                         case
                             when t1.end_time is null or t1.end_time >= LEAST(@dt_next_day_start_time, @now_time)
                                 then LEAST(@dt_next_day_start_time, @now_time)
                             else t1.end_time end                                                                as stat_end_time
                  from (select *
                        from phoenix_basic.basic_notification
                        where alarm_module = 'robot'
                          and alarm_level >= 3
                          and (
                                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                 coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                 coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
                                (start_time < @dt_day_start_time and
                                 coalesce(end_time, @now_time) >= @dt_next_day_start_time and
                                 coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                (start_time < @dt_day_start_time and
                                 coalesce(end_time, @now_time) >= @dt_next_day_start_time)
                            )) t1
                           -- 注意：一定是用的inner join 
                           inner join (select robot_code,
                                              COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                                              min(id)                                                            as first_error_id
                                       from phoenix_basic.basic_notification
                                       where alarm_module = 'robot'
                                         and alarm_level >= 3
                                         and (
                                               (start_time >= @dt_day_start_time and
                                                start_time < @dt_next_day_start_time and
                                                coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                               (start_time >= @dt_day_start_time and
                                                start_time < @dt_next_day_start_time and
                                                coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
                                               (start_time < @dt_day_start_time and
                                                coalesce(end_time, @now_time) >= @dt_next_day_start_time and
                                                coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                               (start_time < @dt_day_start_time and
                                                coalesce(end_time, @now_time) >= @dt_next_day_start_time)
                                           )
                                       group by robot_code,
                                                COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                                      on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
                           left join phoenix_basic.basic_error_info t3 on t3.error_code = t1.error_code
                  order by robot_error_str asc, t1.start_time asc) t,
                 (select @uidd := null, @cidd := null, @rankk := 0) r) t2
           on t2.robot_error_str = t1.robot_error_str and t1.rank - 1 = t2.rankk) t
         left join phoenix_basic.basic_error_info t3 on t3.error_code = t.error_code
where t.xy_distance is null
   or t.xy_distance > 1000
   
   
   
   





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
FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd') as date_value,
t1.error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t1.alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object,
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
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t3.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.point_location,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y,
t1.transport_object,
case when t1.start_time >= @dt_day_start_time then t1.start_time else @dt_day_start_time end as stat_start_time,
-- GREATEST(t1.start_time,@dt_day_start_time)  as stat_start_time,
case when t1.end_time is null or t1.end_time >= (case when @dt_next_day_start_time <= @now_time then @dt_next_day_start_time else @now_time end) then (case when @dt_next_day_start_time <= @now_time then @dt_next_day_start_time else @now_time end) else t1.end_time end as stat_end_time
-- case when t1.end_time is null or t1.end_time >= LEAST(@dt_next_day_start_time,@now_time) then LEAST(@dt_next_day_start_time,@now_time) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.dbo.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) < @dt_next_day_start_time ) or
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) >= @dt_next_day_start_time ) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
            )) t1
			-- 注意：一定是用的inner join 
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.dbo.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
								(start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) < @dt_next_day_start_time ) or
								(start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) >= @dt_next_day_start_time ) or
								(start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and coalesce(end_time, @now_time) < @dt_next_day_start_time) or
								(start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
							)
                     group by robot_code,COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.dbo.basic_error_info t3 on t3.error_code =t1.error_code)t)t)t1 
where t1.xy_distance is null or t1.xy_distance > 1000




-- part3：异步表兼容逻辑


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的下一天的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
select {{ now_time }}                as create_time,
       {{ now_time }}                as update_time,
       date({{ dt_day_start_time }}) as date_value,
       t.error_id,
       t.error_code,
       t.start_time,
       t.end_time,
       t.warning_spec,
       t.alarm_module,
       t.alarm_service,
       t.alarm_type,
       t.alarm_level,
       t3.alarm_name            as alarm_detail,
       t.param_value,
       t.job_order,
       t.robot_job,
       t.robot_code,
       t.device_code,
       t.server_code,
       t.transport_object,
       t.stat_start_time,
       t.stat_end_time
from (select t1.*,
             case
                 when t1.x is not null and t1.y is not null and t2.x is not null and t2.y is not null
                     then SQRT(power((t1.x - t2.x), 2) + power((t1.y - t2.y), 2)) end as xy_distance
      from (select t.*,
                   if(@uid = t.robot_error_str, @rank := @rank + 1, @rank := 1) as rank,
@uid:=t.robot_error_str AS robot_error_str_b
            from
                (select
                t1.id as error_id, t1.robot_code, t1.error_code, concat(t1.robot_code, '-', t1.error_code) as robot_error_str, t1.start_time, t1.end_time, t1.warning_spec, t1.alarm_module, t1.alarm_service, t1.alarm_type, t1.alarm_level, t3.alarm_name as alarm_detail, t1.param_value, t1.job_order, t1.robot_job, t1.device_code, t1.server_code, t1.transport_object, t1.point_location, substring_index(substring_index(t1.point_location, "x=", -1), ",", 1) as x, substring_index(substring_index(replace (t1.point_location, ")", ""), "y=", -1), ",", 1) as y, GREATEST(t1.start_time, {{ dt_day_start_time }}) as stat_start_time, case when t1.end_time is null or t1.end_time >= LEAST({{ dt_next_day_start_time }}, {{ now_time }}) then LEAST({{ dt_next_day_start_time }}, {{ now_time }}) else t1.end_time end as stat_end_time
                from (select *
                from phoenix_basic.basic_notification
                where alarm_module = 'robot'
                and alarm_level >= 3
                and (
                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }} ) < {{ dt_next_day_start_time }} ) or
                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }} ) >= {{ dt_next_day_start_time }} ) or
                (start_time < {{ dt_day_start_time }} and coalesce (end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                (start_time < {{ dt_day_start_time }} and coalesce (end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
                )) t1
                -- 注意：一定是用的inner join
                inner join (select robot_code, COALESCE (DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time, min(id) as first_error_id
                from phoenix_basic.basic_notification
                where alarm_module = 'robot'
                and alarm_level >= 3
                and (
                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }} ) < {{ dt_next_day_start_time }} ) or
                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }} ) >= {{ dt_next_day_start_time }} ) or
                (start_time < {{ dt_day_start_time }} and coalesce (end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and coalesce (end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                (start_time < {{ dt_day_start_time }} and coalesce (end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
                )
                group by robot_code, COALESCE (DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
                left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code
                order by robot_error_str asc, t1.start_time asc) t, (select @uid := null, @cid := null, @rank := 0) r) t1
               left join
           (select t.*,
                   if(@uidd = t.robot_error_str, @rankk := @rankk + 1, @rankk := 1) as rankk,
                   @uidd := t.robot_error_str                                       AS robot_error_str_b

            from (select t1.id                                                                                   as error_id,
                         t1.robot_code,
                         t1.error_code,
                         concat(t1.robot_code, '-', t1.error_code)                                               as robot_error_str,
                         t1.start_time,
                         t1.end_time,
                         t1.warning_spec,
                         t1.alarm_module,
                         t1.alarm_service,
                         t1.alarm_type,
                         t1.alarm_level,
                         t3.alarm_name                                                                           as alarm_detail,
                         t1.param_value,
                         t1.job_order,
                         t1.robot_job,
                         t1.device_code,
                         t1.server_code,
                         t1.transport_object,
                         t1.point_location,
                         substring_index(substring_index(t1.point_location, "x=", -1), ",", 1)                   as x,
                         substring_index(substring_index(replace(t1.point_location, ")", ""), "y=", -1), ",", 1) as y,
                         GREATEST(t1.start_time, {{ dt_day_start_time }})                                             as stat_start_time,
                         case
                             when t1.end_time is null or t1.end_time >= LEAST({{ dt_next_day_start_time }}, {{ now_time }})
                                 then LEAST({{ dt_next_day_start_time }}, {{ now_time }})
                             else t1.end_time end                                                                as stat_end_time
                  from (select *
                        from phoenix_basic.basic_notification
                        where alarm_module = 'robot'
                          and alarm_level >= 3
                          and (
                                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and
                                 coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                                (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and
                                 coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }}) or
                                (start_time < {{ dt_day_start_time }} and
                                 coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and
                                 coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                                (start_time < {{ dt_day_start_time }} and
                                 coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
                            )) t1
                           -- 注意：一定是用的inner join
                           inner join (select robot_code,
                                              COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                                              min(id)                                                            as first_error_id
                                       from phoenix_basic.basic_notification
                                       where alarm_module = 'robot'
                                         and alarm_level >= 3
                                         and (
                                               (start_time >= {{ dt_day_start_time }} and
                                                start_time < {{ dt_next_day_start_time }} and
                                                coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                                               (start_time >= {{ dt_day_start_time }} and
                                                start_time < {{ dt_next_day_start_time }} and
                                                coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }}) or
                                               (start_time < {{ dt_day_start_time }} and
                                                coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and
                                                coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
                                               (start_time < {{ dt_day_start_time }} and
                                                coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
                                           )
                                       group by robot_code,
                                                COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                                      on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
                           left join phoenix_basic.basic_error_info t3 on t3.error_code = t1.error_code
                  order by robot_error_str asc, t1.start_time asc) t,
                 (select @uidd := null, @cidd := null, @rankk := 0) r) t2
           on t2.robot_error_str = t1.robot_error_str and t1.rank - 1 = t2.rankk) t
         left join phoenix_basic.basic_error_info t3 on t3.error_code = t.error_code
where t.xy_distance is null
   or t.xy_distance > 1000
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
t1.error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t1.alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object,
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
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t3.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.point_location,
CASE WHEN CHARINDEX('x=',t1.point_location)!=0 THEN left(right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1),CHARINDEX(',y',right(t1.point_location,LEN(t1.point_location)-CHARINDEX('x=',t1.point_location)-1))-1) END AS x,
case when CHARINDEX('y=',t1.point_location)!=0 then left(right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1),CHARINDEX(',pointCode',right(t1.point_location,len(t1.point_location)-CHARINDEX('y=',t1.point_location)-1))-1) end as y,
t1.transport_object,
case when t1.start_time >= {{ dt_day_start_time }} then t1.start_time else {{ dt_day_start_time }} end as stat_start_time,
-- GREATEST(t1.start_time,{{ dt_day_start_time }})  as stat_start_time,
case when t1.end_time is null or t1.end_time >= (case when {{ dt_next_day_start_time }} <= {{ now_time }} then {{ dt_next_day_start_time }} else {{ now_time }} end) then (case when {{ dt_next_day_start_time }} <= {{ now_time }} then {{ dt_next_day_start_time }} else {{ now_time }} end) else t1.end_time end as stat_end_time
-- case when t1.end_time is null or t1.end_time >= LEAST({{ dt_next_day_start_time }},{{ now_time }}) then LEAST({{ dt_next_day_start_time }},{{ now_time }}) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }} ) < {{ dt_next_day_start_time }} ) or
              (start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }} ) >= {{ dt_next_day_start_time }} ) or
              (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
              (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
            )) t1
			-- 注意：一定是用的inner join
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
								(start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }} ) < {{ dt_next_day_start_time }} ) or
								(start_time >= {{ dt_day_start_time }} and start_time < {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }} ) >= {{ dt_next_day_start_time }} ) or
								(start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
								(start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
							)
                     group by robot_code,COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code)t)t)t1
where t1.xy_distance is null or t1.xy_distance > 1000
{% endif %}