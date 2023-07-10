set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;


-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_hour_transport_order_detail_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time, upstream_order_no, order_no, scene_type,start_point,start_area,target_point, target_area, order_state,dispatch_robot_code_num, dispatch_robot_code_str,dispatch_robot_classification_str, total_time_consuming,empty_move_distance, empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming, put_down_time_consuming,guide_time_consuming, robot_rotate_num, order_create_time,order_completed_time)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_hour_start_time) as date_value,
@dt_hour_start_time as hour_start_time,
@dt_next_hour_start_time as next_hour_start_time,
t.upstream_order_no,
t.order_no,
t.scenario  as scene_type,
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end as start_point,
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknow' end as start_area,
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end as target_point,
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknow' end as target_area,
t.order_state,
tr.dispatch_robot_code_num,
tr.dispatch_robot_code_str,
tr.dispatch_robot_classification_str,
nullif(tc.total_cost,0)/1000 as total_time_consuming,
nullif(tj.empty_move_distance,0) as empty_move_distance,
case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/tc.move_cost else null end as empty_move_speed,
nullif(tj.loading_move_distance,0) as loading_move_distance,
case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/tc.rack_move_cost else null end as loading_move_speed,
nullif(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,
nullif(tc.move_cost,0)/1000 as move_time_consuming,
nullif(tc.lift_cost,0)/1000 as lift_up_time_consuming,
nullif(tc.rack_move_cost,0)/1000 as rack_move_time_consuming,
nullif(tc.put_cost,0)/1000 as put_down_time_consuming,
nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
t.create_time as order_create_time,
tc.order_update_time as order_completed_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join 
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by tc.order_no)tr on tr.order_no = t.order_no
left join 
(select 
t.order_no,
sum(rasd.rotate_count)   as robot_rotate_num,
sum(rasd.actual_move_distance * 1000)  as order_actual_move_distance,
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.actual_move_distance * 1000 end) as empty_move_distance,
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.actual_move_distance * 1000 end) as loading_move_distance,
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by t.order_no)tj on tj.order_no = t.order_no 
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time



--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_hour_transport_order_detail_his	
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
t.upstream_order_no,
t.order_no,
t.scenario  as scene_type,
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end as start_point,
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknow' end as start_area,
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end as target_point,
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknow' end as target_area,
t.order_state,
tr.dispatch_robot_code_num,
tr.dispatch_robot_code_str,
tr.dispatch_robot_classification_str,
nullif(tc.total_cost,0)/1000 as total_time_consuming,
nullif(tj.empty_move_distance,0) as empty_move_distance,
case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/tc.move_cost else null end as empty_move_speed,
nullif(tj.loading_move_distance,0) as loading_move_distance,
case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/tc.rack_move_cost else null end as loading_move_speed,
nullif(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,
nullif(tc.move_cost,0)/1000 as move_time_consuming,
nullif(tc.lift_cost,0)/1000 as lift_up_time_consuming,
nullif(tc.rack_move_cost,0)/1000 as rack_move_time_consuming,
nullif(tc.put_cost,0)/1000 as put_down_time_consuming,
nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
t.create_time as order_create_time,
tc.order_update_time as order_completed_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by tc.order_no)tr on tr.order_no = t.order_no
left join
(select
t.order_no,
sum(rasd.rotate_count)   as robot_rotate_num,
sum(rasd.actual_move_distance * 1000)  as order_actual_move_distance,
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.actual_move_distance * 1000 end) as empty_move_distance,
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.actual_move_distance * 1000 end) as loading_move_distance,
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by t.order_no)tj on tj.order_no = t.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
