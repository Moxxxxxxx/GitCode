set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;


-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_hour_transport_order_link_detail_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time,link_id, upstream_order_no, order_no,link_create_time,event_time, execute_state, order_state,robot_code, first_classification,robot_type_code,robot_type_name,cost_time)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_hour_start_time) as date_value,
@dt_hour_start_time as hour_start_time,
@dt_next_hour_start_time as next_hour_start_time,
tol.id as link_id,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.event_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_code,
brt.robot_type_name,
tol.cost_time/1000 as cost_time 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
order by tol.order_no,tol.id asc


--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_hour_transport_order_link_detail_his	
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



-- 插入逻辑 
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_hour_start_time }}) as date_value,
{{ dt_hour_start_time }} as hour_start_time,
{{ dt_next_hour_start_time }} as next_hour_start_time,
tol.id as link_id,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.event_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_code,
brt.robot_type_name,
tol.cost_time/1000 as cost_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
order by tol.order_no,tol.id asc