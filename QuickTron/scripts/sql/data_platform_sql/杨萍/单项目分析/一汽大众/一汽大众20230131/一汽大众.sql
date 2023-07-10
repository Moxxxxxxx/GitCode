select 
date_format(create_time,'%Y-%m-%d') as date_value,
count(distinct robot_code) as robot_code_num
from phoenix_rms.robot_state_history
where create_time>='2023-01-01 00:00:00'
and online_state='REGISTERED'
group by 1

-- 

select 
*
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
and start_time>='2023-01-01 00:00:00'
order by robot_code,start_time



-- 

select 
date_format(start_time,'%Y-%m-%d') as date_value,
count(0) as num 
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
and start_time>='2023-01-01 00:00:00'
group by 1



-----
-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time='2023-01-01 00:00:00';  -- 当天开始时间
set @dt_next_day_start_time='2023-02-01 00:00:00'; --  明天开始时间


select 
t1.id                                     as error_id,
t1.error_code,
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
t1.point_location,
case when t1.point_location like '%pointCode=%' then substring_index(substring_index(t1.point_location,'pointCode=',-1),')',1) end as point_code,
substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x,
substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y,										
GREATEST(t1.start_time,@dt_day_start_time)  as stat_start_time,
case when t1.end_time is null or t1.end_time >= LEAST(@dt_next_day_start_time,@now_time) then LEAST(@dt_next_day_start_time,@now_time) else t1.end_time end as stat_end_time,
t1.start_time,
t1.end_time,
date_format(t1.start_time,'%Y-%m-%d') as start_date_value,
date_format(t1.end_time,'%Y-%m-%d') as end_date_value,
UNIX_TIMESTAMP(t1.end_time)-UNIX_TIMESTAMP(t1.start_time) as error_duration
from (select *
      from phoenix_basic.basic_notification
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
                            COALESCE(end_time, 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
								(start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) < @dt_next_day_start_time ) or
								(start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and coalesce(end_time, @now_time ) >= @dt_next_day_start_time ) or
								(start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and coalesce(end_time, @now_time) < @dt_next_day_start_time) or
								(start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
							)
                     group by robot_code, COALESCE(end_time, 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
										order by t1.robot_code,t1.start_time asc
										
										
										
										
										
=IF(E3=E2,SQRT((B3-B2)^2+(C3-C2)^2))										

01-12:69
01-13:73
01-15:81
01-16:76



规则1：筛出距离大于1米的
规则2:筛出故障时长>10秒的



57414_KAhc、57414_cyac、57414_eMbQ、57414_tAQJ、57414_NnJ2、57414_wzWJ、57414_h5Fc、57414_P2ia、57414_Wsbn、57414_D8Kh




57414_KAhc	20
57414_cyac	13
57414_eMbQ	13
57414_tAQJ	9
57414_NnJ2	8
57414_wzWJ	8
57414_h5Fc	7
57414_P2ia	7
57414_Wsbn	6
57414_D8Kh	5



丢定位码点
最多的车
报错前top3对应的车

DSP0x4110	行驶方向控制偏差超过规定阈值。
DSP0x4301 一段距离内未收到有效定位数据 
DSP0x4306	定位数据模块一直未收到正确的外部定位数据（3帧确认）




57414_wdF6
57414_KAhc --
57414_NnJ2  --
57414_fBsP
57414_tBeJ
57414_y5sZ
57414_6tZh
57414_T2Q7
57414_is8p
57414_PzBc



57414_wdF6
57414_KAhc
57414_NnJ2
57414_fBsP
57414_tBeJ





1、同一个机器人算出与前一条距离
2、