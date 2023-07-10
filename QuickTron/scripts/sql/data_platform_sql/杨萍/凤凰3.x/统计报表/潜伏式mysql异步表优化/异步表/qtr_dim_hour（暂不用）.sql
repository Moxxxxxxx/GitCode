set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;




-- step1:删除相关数据（qtr_dim_hour） 小时维表
TRUNCATE TABLE qt_smartreport.qtr_dim_hour;


-- step2:插入数据（异步表）qt_smartreport.qtr_dim_hour	
-- {{ dt_relative_time(dt) }}
-- {{ now_time }}
-- {{ dt_hour_start_time }}
-- {{ dt_next_hour_start_time }}
-- {{ dt_day_start_time }}
-- {{ dt_next_day_start_time }}
-- {{ dt_week_start_time }}
-- {{ dt_next_week_start_time }}	


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S.000000'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00.000000") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00.000000") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的下一周的开始时间





insert into qt_smartreport.qtr_dim_hour(create_time,update_time,hour_start_time,next_hour_start_time) 
values 
({{ now_time }},{{ now_time }},'00:00:00','01:00:00'),
({{ now_time }},{{ now_time }},'01:00:00','02:00:00'),
({{ now_time }},{{ now_time }},'02:00:00','03:00:00'),
({{ now_time }},{{ now_time }},'03:00:00','04:00:00'),
({{ now_time }},{{ now_time }},'04:00:00','05:00:00'),
({{ now_time }},{{ now_time }},'05:00:00','06:00:00'),
({{ now_time }},{{ now_time }},'06:00:00','07:00:00'),
({{ now_time }},{{ now_time }},'07:00:00','08:00:00'),
({{ now_time }},{{ now_time }},'08:00:00','09:00:00'),
({{ now_time }},{{ now_time }},'09:00:00','10:00:00'),
({{ now_time }},{{ now_time }},'10:00:00','11:00:00'),
({{ now_time }},{{ now_time }},'11:00:00','12:00:00'),
({{ now_time }},{{ now_time }},'12:00:00','13:00:00'),
({{ now_time }},{{ now_time }},'13:00:00','14:00:00'),
({{ now_time }},{{ now_time }},'14:00:00','15:00:00'),
({{ now_time }},{{ now_time }},'15:00:00','16:00:00'),
({{ now_time }},{{ now_time }},'16:00:00','17:00:00'),
({{ now_time }},{{ now_time }},'17:00:00','18:00:00'),
({{ now_time }},{{ now_time }},'18:00:00','19:00:00'),
({{ now_time }},{{ now_time }},'19:00:00','20:00:00'),
({{ now_time }},{{ now_time }},'20:00:00','21:00:00'),
({{ now_time }},{{ now_time }},'21:00:00','22:00:00'),
({{ now_time }},{{ now_time }},'22:00:00','23:00:00'),
({{ now_time }},{{ now_time }},'23:00:00','00:00:00');