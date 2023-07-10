-- 表1：qt_smartreport.qtr_day_transport_order_link_detail_stat_his

-- step1:删除相关数据（qtr_day_transport_order_link_detail_stat_his）
DELETE
FROM qt_smartreport.qtr_day_transport_order_link_detail_stat_his
WHERE date_value = date_add(current_date(), interval -1 day);  	



-- step2:插入相关数据（qtr_day_transport_order_link_detail_stat_his）
insert into qt_smartreport.qtr_day_transport_order_link_detail_stat_his(create_time,update_time,date_value,link_id, upstream_order_no, order_no,link_create_time,event_time, execute_state, order_state,robot_code, first_classification,robot_type_code,robot_type_name,cost_time)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day)                    as date_value,
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
where tc.update_time >=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and tc.update_time < date_format(current_date(), '%Y-%m-%d 00:00:00')
order by tol.order_no,tol.id asc




-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_transport_order_link_detail_stat_his;
insert into qt_smartreport.qtr_day_transport_order_link_detail_stat_his(create_time,update_time,date_value,link_id, upstream_order_no, order_no,link_create_time,event_time, execute_state, order_state,robot_code, first_classification,robot_type_code,robot_type_name,cost_time)
select created_time as create_time,updated_time as update_time,date_value,link_id, upstream_order_no, order_no,link_create_time,event_time, execute_state, order_state,robot_code, first_classification,null as robot_type_code,robot_type_name,cost_time
from qt_smartreport.qt_day_transport_order_link_detail_stat_his;



--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_transport_order_detail_stat_his

-- step1:删除相关数据（qtr_transport_order_detail_stat_his）
DELETE
FROM qt_smartreport.qtr_transport_order_detail_stat_his
WHERE date_value = date_add(current_date(), interval -1 day);  


-- step2:插入相关数据（qtr_transport_order_detail_stat_his）
insert into qt_smartreport.qtr_transport_order_detail_stat_his(create_time,update_time,date_value, upstream_order_no, order_no, scene_type,start_point,start_area,target_point, target_area, order_state,
dispatch_robot_code_num, dispatch_robot_code_str,dispatch_robot_classification_str, total_time_consuming,
empty_move_distance, empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming, put_down_time_consuming,guide_time_consuming, robot_rotate_num, order_create_time,order_completed_time)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day)                       as date_value,
t.upstream_order_no,
t.order_no,
t.scenario  as scene_type,
       case
           when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
           else 'unknow' end                                                                   start_point,
       case
           when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code
           else 'unknow' end                                                                   start_area,
       case
           when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code
           else 'unknow' end                                                                   target_point,
       case
           when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code
           else 'unknow' end                                                                   target_area,
       case
           when t.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
           when t.order_state = 'EXECUTING' then '正在执行'
           when t.order_state = 'COMPLETED' then '已完成'
           when t.order_state = 'CANCELED' then '取消'
           when t.order_state = 'PENDING' then '挂起'
           when t.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
           when t.order_state = 'ABNORMAL_CANCELED' then '异常取消'
           end                                                                              as order_state,
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
where tc.update_time >=date_add(current_date(), interval -1 day) and tc.update_time < date(sysdate())
group by tc.order_no)tr on tr.order_no = t.order_no
left join 
(select t.order_no,
                           sum(rasd.rotate_count)                                 as robot_rotate_num,
                           sum(rasd.actual_move_distance * 1000)                  as order_actual_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_LIFT_UP' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 0)
                                       then rasd.actual_move_distance * 1000 end) as empty_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_PUT_DOWN' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 1)
                                       then rasd.actual_move_distance * 1000 end) as loading_move_distance,
                           sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))        as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=date_add(current_date(), interval -1 day) and tc.update_time < date(sysdate())
group by t.order_no)tj on tj.order_no = t.order_no 
where tc.update_time >=date_add(current_date(), interval -1 day) and tc.update_time < date(sysdate())



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_transport_order_detail_stat_his;
insert into qt_smartreport.qtr_transport_order_detail_stat_his(create_time,update_time,date_value, upstream_order_no, order_no, scene_type,start_point,start_area,target_point, target_area, order_state,
dispatch_robot_code_num, dispatch_robot_code_str,dispatch_robot_classification_str, total_time_consuming,
empty_move_distance, empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming, put_down_time_consuming,guide_time_consuming, robot_rotate_num, order_create_time,order_completed_time)
select created_time as create_time,updated_time as update_time,date_value, upstream_order_no, order_no, scene_type,start_point,start_area,target_point, target_area, order_state,
dispatch_robot_code_num, dispatch_robot_code_str,dispatch_robot_classification_str, total_time_consuming,
empty_move_distance, empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming, put_down_time_consuming,guide_time_consuming, robot_rotate_num, order_create_time,order_completed_time
from qt_smartreport.qt_transport_order_detail_stat_his;




--------------------------------------------------------------------------------
-- 表3：qt_smartreport.qtr_transport_upstream_order_detail_stat_his

-- step1:删除相关数据（qtr_transport_upstream_order_detail_stat_his）
DELETE
FROM qt_smartreport.qtr_transport_upstream_order_detail_stat_his
WHERE date_value = date_add(current_date(), interval -1 day);  	


-- step2:插入相关数据（qtr_transport_upstream_order_detail_stat_his）
insert into qt_smartreport.qtr_transport_upstream_order_detail_stat_his(create_time,update_time,date_value, upstream_order_no, scene_type,stat_time,start_point, start_area, target_point,target_area,upstream_order_state, dispatch_robot_code_num,dispatch_robot_code_str,dispatch_robot_classification_str,total_time_consuming, empty_move_distance,empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming,put_down_time_consuming,guide_time_consuming, robot_rotate_num,dispatch_order_no,dispatch_order_num,upstream_order_create_time,upstream_order_completed_time)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day)                       as date_value,
t.upstream_order_no,
tr.scenario  as scene_type,
date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00')                       as stat_time,
case
    when tr.start_point_code <> '' and tr.start_point_code is not null then tr.start_point_code
    else 'unknow' end                                                                   start_point,
case
    when tr.start_area_code <> '' and tr.start_area_code is not null then tr.start_area_code
    else 'unknow' end                                                                   start_area,
case
    when tr.target_point_code <> '' and tr.target_point_code is not null then tr.target_point_code
    else 'unknow' end                                                                   target_point,
case
    when tr.target_area_code <> '' and tr.target_area_code is not null then tr.target_area_code
    else 'unknow' end                                                                   target_area,
case
    when tr.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
    when tr.order_state = 'EXECUTING' then '正在执行'
    when tr.order_state = 'COMPLETED' then '已完成'
    when tr.order_state = 'CANCELED' then '取消'
    when tr.order_state = 'PENDING' then '挂起'
    when tr.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
    when tr.order_state = 'ABNORMAL_CANCELED' then '异常取消'
    end                                                                              as upstream_order_state,
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
where tc.update_time >=date_add(current_date(), interval -1 day) and tc.update_time < date(sysdate())
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
where update_time >=date_add(current_date(), interval -1 day) and update_time < date(sysdate())
group by upstream_order_no)tsc on tsc.upstream_order_no = t.upstream_order_no
 left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
left join 
(select tc.upstream_order_no,
                           sum(rasd.rotate_count)                                 as robot_rotate_num,
                           sum(rasd.actual_move_distance * 1000)                  as order_actual_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_LIFT_UP' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 0)
                                       then rasd.actual_move_distance * 1000 end) as empty_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_PUT_DOWN' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 1)
                                       then rasd.actual_move_distance * 1000 end) as loading_move_distance,
                           sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))         as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=date_add(current_date(), interval -1 day) and tc.update_time < date(sysdate())
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no




-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_transport_upstream_order_detail_stat_his;
insert into qt_smartreport.qtr_transport_upstream_order_detail_stat_his(create_time,update_time,date_value, upstream_order_no, scene_type,stat_time,start_point, start_area, target_point,target_area,upstream_order_state, dispatch_robot_code_num,dispatch_robot_code_str,dispatch_robot_classification_str,total_time_consuming, empty_move_distance,empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming,put_down_time_consuming,guide_time_consuming, robot_rotate_num,dispatch_order_no,dispatch_order_num,upstream_order_create_time,upstream_order_completed_time)
select created_time as create_time,updated_time as update_time,date_value, upstream_order_no, scene_type,stat_time,start_point, start_area, target_point,target_area,upstream_order_state, dispatch_robot_code_num,dispatch_robot_code_str,dispatch_robot_classification_str,total_time_consuming, empty_move_distance,empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming,put_down_time_consuming,guide_time_consuming, robot_rotate_num,dispatch_order_no,dispatch_order_num,upstream_order_create_time,upstream_order_completed_time
from qt_smartreport.qt_transport_upstream_order_detail_stat_his;