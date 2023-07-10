set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;


-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_transport_upstream_order_detail_stat_his(create_time,update_time,date_value, upstream_order_no, scene_type,stat_time,start_point, start_area, target_point,target_area,upstream_order_state, dispatch_robot_code_num,dispatch_robot_code_str,dispatch_robot_classification_str,total_time_consuming, empty_move_distance,empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming,put_down_time_consuming,guide_time_consuming, robot_rotate_num,dispatch_order_no,dispatch_order_num,upstream_order_create_time,upstream_order_completed_time)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t.upstream_order_no,
tr.scenario  as scene_type,
date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00')     as stat_time,
case when tr.start_point_code <> '' and tr.start_point_code is not null then tr.start_point_code else 'unknow' end as start_point,
case when tr.start_area_code <> '' and tr.start_area_code is not null then tr.start_area_code else 'unknow' end as start_area,
case when tr.target_point_code <> '' and tr.target_point_code is not null then tr.target_point_code else 'unknow' end as target_point,
case when tr.target_area_code <> '' and tr.target_area_code is not null then tr.target_area_code else 'unknow' end as target_area,
tr.order_state as upstream_order_state,
t.dispatch_robot_code_num,
t.dispatch_robot_code_str,
t.dispatch_robot_classification_str,
nullif (tsc.total_time_consuming,0) as total_time_consuming,
nullif(tj.empty_move_distance, 0) as empty_move_distance,
case when coalesce (tsc.move_time_consuming,0) !=0 then nullif(tj.empty_move_distance, 0)/tsc.move_time_consuming else null end as empty_move_speed,
nullif(tj.loading_move_distance, 0) as loading_move_distance,
case when coalesce(tsc.rack_move_time_consuming,0) !=0 then nullif(tj.loading_move_distance, 0)/tsc.rack_move_time_consuming else null end as loading_move_speed,
nullif(tsc.waiting_robot_time_consuming,0) as waiting_robot_time_consuming,
nullif(tsc.move_time_consuming,0) as move_time_consuming,
nullif(tsc.lift_up_time_consuming,0) as lift_up_time_consuming,
nullif(tsc.rack_move_time_consuming,0) as rack_move_time_consuming,
nullif(tsc.put_down_time_consuming,0) as put_down_time_consuming,
nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
t.dispatch_order_num,
t.order_no_num,
t.upstream_order_create_time,
t.upstream_order_completed_time

from 
(select 
tc.upstream_order_no,
min(t.create_time)                                                as upstream_order_create_time,
max(case when t.order_state = 'COMPLETED' then tc.order_update_time end) as upstream_order_completed_time,
max(tc.order_update_time)                                                as upstream_order_update_time,
count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
count(distinct t.order_no)                                        as order_no_num,
group_concat(distinct t.order_no)                                 as dispatch_order_num,
max(t.id)      as latest_id
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no =tc.order_no 
left join phoenix_rss.transport_order_link tk on t.order_no = tk.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= @dt_day_start_time and tc.update_time < @dt_next_day_start_time
group by tc.upstream_order_no)t 
left join 
(select 
upstream_order_no,
sum(total_cost)/1000 as total_time_consuming,
sum(assign_cost)/1000 as waiting_robot_time_consuming,
sum(move_cost)/1000 as move_time_consuming,
sum(lift_cost)/1000 as lift_up_time_consuming,
sum(rack_move_cost)/1000 as rack_move_time_consuming,
sum(put_cost)/1000 as put_down_time_consuming
from phoenix_rss.transport_order_carrier_cost
where update_time >= @dt_day_start_time and update_time < @dt_next_day_start_time
group by upstream_order_no)tsc on tsc.upstream_order_no = t.upstream_order_no
 left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
left join 
(select 
tc.upstream_order_no,
sum(rasd.rotate_count) as robot_rotate_num,
sum(rasd.actual_move_distance * 1000) as order_actual_move_distance,
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.actual_move_distance * 1000 end) as empty_move_distance,
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.actual_move_distance * 1000 end) as loading_move_distance,
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >= @dt_day_start_time and tc.update_time < @dt_next_day_start_time
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no




--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_transport_upstream_order_detail_stat_his	
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
date({{ dt_day_start_time }}) as date_value,
t.upstream_order_no,
tr.scenario  as scene_type,
date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00')     as stat_time,
case when tr.start_point_code <> '' and tr.start_point_code is not null then tr.start_point_code else 'unknow' end as start_point,
case when tr.start_area_code <> '' and tr.start_area_code is not null then tr.start_area_code else 'unknow' end as start_area,
case when tr.target_point_code <> '' and tr.target_point_code is not null then tr.target_point_code else 'unknow' end as target_point,
case when tr.target_area_code <> '' and tr.target_area_code is not null then tr.target_area_code else 'unknow' end as target_area,
tr.order_state as upstream_order_state,
t.dispatch_robot_code_num,
t.dispatch_robot_code_str,
t.dispatch_robot_classification_str,
nullif (tsc.total_time_consuming,0) as total_time_consuming,
nullif(tj.empty_move_distance, 0) as empty_move_distance,
case when coalesce (tsc.move_time_consuming,0) !=0 then nullif(tj.empty_move_distance, 0)/tsc.move_time_consuming else null end as empty_move_speed,
nullif(tj.loading_move_distance, 0) as loading_move_distance,
case when coalesce(tsc.rack_move_time_consuming,0) !=0 then nullif(tj.loading_move_distance, 0)/tsc.rack_move_time_consuming else null end as loading_move_speed,
nullif(tsc.waiting_robot_time_consuming,0) as waiting_robot_time_consuming,
nullif(tsc.move_time_consuming,0) as move_time_consuming,
nullif(tsc.lift_up_time_consuming,0) as lift_up_time_consuming,
nullif(tsc.rack_move_time_consuming,0) as rack_move_time_consuming,
nullif(tsc.put_down_time_consuming,0) as put_down_time_consuming,
nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
t.dispatch_order_num,
t.order_no_num,
t.upstream_order_create_time,
t.upstream_order_completed_time

from
(select
tc.upstream_order_no,
min(t.create_time)                                                as upstream_order_create_time,
max(case when t.order_state = 'COMPLETED' then tc.order_update_time end) as upstream_order_completed_time,
max(tc.order_update_time)                                                as upstream_order_update_time,
count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
count(distinct t.order_no)                                        as order_no_num,
group_concat(distinct t.order_no)                                 as dispatch_order_num,
max(t.id)      as latest_id
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no =tc.order_no
left join phoenix_rss.transport_order_link tk on t.order_no = tk.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >= {{ dt_day_start_time }} and tc.update_time < {{ dt_next_day_start_time }}
group by tc.upstream_order_no)t
left join
(select
upstream_order_no,
sum(total_cost)/1000 as total_time_consuming,
sum(assign_cost)/1000 as waiting_robot_time_consuming,
sum(move_cost)/1000 as move_time_consuming,
sum(lift_cost)/1000 as lift_up_time_consuming,
sum(rack_move_cost)/1000 as rack_move_time_consuming,
sum(put_cost)/1000 as put_down_time_consuming
from phoenix_rss.transport_order_carrier_cost
where update_time >= {{ dt_day_start_time }} and update_time < {{ dt_next_day_start_time }}
group by upstream_order_no)tsc on tsc.upstream_order_no = t.upstream_order_no
 left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
left join
(select
tc.upstream_order_no,
sum(rasd.rotate_count) as robot_rotate_num,
sum(rasd.actual_move_distance * 1000) as order_actual_move_distance,
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.actual_move_distance * 1000 end) as empty_move_distance,
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.actual_move_distance * 1000 end) as loading_move_distance,
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) as guide_time_consuming
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >= {{ dt_day_start_time }} and tc.update_time < {{ dt_next_day_start_time }}
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no