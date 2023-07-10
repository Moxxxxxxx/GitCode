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
t.upstream_order_no,   -- 上游作业单ID
t.order_no,  -- 搬运作业单ID
t.scenario  as scene_type,  -- 场景类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end as start_point,  -- 起始点
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknown' end as start_area,   -- 起始区域
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end as target_point,  -- 目标点
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknown' end as target_area,  -- 目标区域
t.order_state,  -- 搬运作业单状态
tj.dispatch_robot_code_num,  -- 分配机器人数量
tj.dispatch_robot_code_str,  -- 分配的机器人
tj.dispatch_robot_classification_str,  -- 分配的机器人类型
nullif(tc.total_cost,0)/1000 as total_time_consuming,  -- 总耗时（秒）
nullif(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,  -- 分车耗时(秒)
nullif(tc.move_cost,0)/1000 as empty_move_time_consuming,  -- 空车移动耗时(秒)
cast(nullif(tj.empty_move_distance,0) as decimal(20,10)) as empty_move_distance,   -- 空车移动距离(米)
cast(case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/(tc.move_cost/1000) else null end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度(米/秒)
nullif(tj.empty_parking_count,0) as empty_parking_count,   -- 交控停车次数（空车移动）
nullif(tj.empty_parking_time,0) as empty_parking_time,   -- 交控停车时长（空车移动）
nullif(tp.empty_guide_time,0) as empty_guide_time,   -- 末端引导时长（空车移动）
nullif(tj.empty_robot_rotate_num,0) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
nullif(COALESCE(tp.before_liftup_cost_time,0)+COALESCE(tp.do_liftup_cost_time,0)+COALESCE(tp.after_liftup_cost_time,0),0) as lift_up_time_consuming, -- 顶升耗时（秒）
nullif(tp.liftup_is_rectification_num,0) as lift_up_is_rectification_num,  -- 顶升发生纠偏次数
nullif(tc.rack_move_cost,0)/1000 as loading_move_time_consuming,  -- 带载移动耗时(秒)
cast(nullif(tj.loading_move_distance,0) as decimal(20,10)) as loading_move_distance,  -- 带载移动距离(米)
cast(case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/(tc.rack_move_cost/1000) else null end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度(米/秒)
nullif(tj.loading_parking_count,0) as loading_parking_count,   -- 交控停车次数（带载移动）
nullif(tj.loading_parking_time,0) as loading_parking_time,   -- 交控停车时长（带载移动）
nullif(tp.loading_guide_time,0) as loading_guide_time,   -- 末端引导时长（带载移动）
nullif(tj.loading_robot_rotate_num,0) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
nullif(coalesce(tp.before_putdown_cost_time,0)+COALESCE(tp.do_putdown_cost_time,0),0) as put_down_time_consuming,  -- 放下耗时（秒）
nullif(tp.putdown_is_rectification_num,0) as put_down_is_rectification_num,  -- 放下发生纠偏次数
nullif(tp.guide_time_consuming,0)  as guide_time_consuming,  -- 末端引导耗时(秒)
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,  -- 机器人旋转次数
t.create_time as order_create_time,   -- 作业单创建时间
tc.order_update_time as order_completed_time  -- 作业单完成时间

from phoenix_rss.transport_order_carrier_cost tc  -- 作业单到达终点状态一分钟内会写数据
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no -- 作业单表
/*
-- 根据作业单链路表找分配机器人
left join 
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num,  -- 分配机器人数量 
group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,  -- 分配的机器人
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by tc.order_no)tr on tr.order_no = t.order_no
*/
-- action
left join 
(select 
t.order_no,
sum((case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000)  as order_actual_move_distance,  --  移动距离（米）
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000 end) as empty_move_distance,   -- 空车移动距离(米)
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000 end) as loading_move_distance,   -- 带载移动距离(米)
sum(rasd.rotate_count) as robot_rotate_num, -- 机器人旋转次数
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.rotate_count end) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.rotate_count end) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_count end) as  empty_parking_count,   -- 交控停车次数（空车移动）
sum(case when tas.is_loading=1 then tas.parking_count end) as  loading_parking_count,   -- 交控停车次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_time/1000 end) as empty_parking_time,   -- 交控停车时长（空车移动）
sum(case when tas.is_loading=1 then tas.parking_time/1000 end) as loading_parking_time,   -- 交控停车时长（带载移动）
count(distinct tj.robot_code)                   as dispatch_robot_code_num,  -- 分配机器人数量 
group_concat(distinct tj.robot_code)            as dispatch_robot_code_str,  -- 分配的机器人
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
left join phoenix_rts.action_statistics_data tas on tas.action_uid = rasd.action_uid
left join phoenix_basic.basic_robot br on br.robot_code = tj.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
-- where tc.order_no='SIRack_167602067589110322'
group by t.order_no)tj on tj.order_no = t.order_no 
-- operation
left join 
(select 
order_no,
sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time)) as before_liftup_cost_time,  -- 顶升前确定耗时
sum(UNIX_TIMESTAMP(do_liftup_end_time) - UNIX_TIMESTAMP(do_liftup_start_time)) as do_liftup_cost_time,  -- 顶升动作耗时
sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_start_time)) as after_liftup_cost_time,  -- 顶升后确定耗时
count(distinct case when (UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
sum(UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time)) as before_putdown_cost_time,  -- 降下前确定耗时
sum(UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time)) as do_putdown_cost_time,  -- 降下动作耗时
count(distinct case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming,   -- 末端引导耗时(秒)
sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as loading_guide_time   -- 末端引导时长（带载移动）
from 
(select 
t.order_no,
t1.action_uid,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code, 
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_before_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_before_liftup_end_time,
max(case when t2.operation_name='DoLiftUp' then t2.start_time end) as do_liftup_start_time,
max(case when t2.operation_name='DoLiftUp' then t2.end_time end) as do_liftup_end_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_after_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_after_liftup_end_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data t1 on t1.job_sn = tj.job_sn
inner join phoenix_rms.job_action_operation_record t2 on t2.action_uid =t1.action_uid 
and t2.operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp','doGuideBeforePutDown','DoPutDown','terminalGuide')
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by t.order_no,t1.action_uid,t1.is_loading,t1.action_code)t
group by order_no)tp on tp.order_no = t.order_no 
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time





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
t.upstream_order_no,   -- 上游作业单ID
t.order_no,  -- 搬运作业单ID
t.scenario  as scene_type,  -- 场景类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end as start_point,  -- 起始点
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknown' end as start_area,   -- 起始区域
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end as target_point,  -- 目标点
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknown' end as target_area,  -- 目标区域
t.order_state,  -- 搬运作业单状态
tr.dispatch_robot_code_num,  -- 分配机器人数量
tr.dispatch_robot_code_str,  -- 分配的机器人
tr.dispatch_robot_classification_str,  -- 分配的机器人类型
nullif(tc.total_cost,0)/cast(1000 as decimal) as total_time_consuming,  -- 总耗时（秒）
nullif(tc.assign_cost,0)/cast(1000 as decimal) as waiting_robot_time_consuming,  -- 分车耗时(秒)
nullif(tc.move_cost,0)/cast(1000 as decimal) as empty_move_time_consuming,  -- 空车移动耗时(秒)
cast(nullif(tj.empty_move_distance,0) as decimal(20,10)) as empty_move_distance,   -- 空车移动距离(米)
cast(case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/(tc.move_cost/cast(1000 as decimal)) else null end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度(米/秒)
nullif(tj.empty_parking_count,0) as empty_parking_count,   -- 交控停车次数（空车移动）
nullif(tj.empty_parking_time,0) as empty_parking_time,   -- 交控停车时长（空车移动）
nullif(tp.empty_guide_time,0) as empty_guide_time,   -- 末端引导时长（空车移动）
nullif(tj.empty_robot_rotate_num,0) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
nullif(COALESCE(tp.before_liftup_cost_time,0)+COALESCE(tp.do_liftup_cost_time,0)+COALESCE(tp.after_liftup_cost_time,0),0) as lift_up_time_consuming, -- 顶升耗时（秒）
nullif(tp.liftup_is_rectification_num,0) as lift_up_is_rectification_num,  -- 顶升发生纠偏次数
nullif(tc.rack_move_cost,0)/cast(1000 as decimal) as loading_move_time_consuming,  -- 带载移动耗时(秒)
cast(nullif(tj.loading_move_distance,0)as decimal(20,10)) as loading_move_distance,  -- 带载移动距离(米)
cast(case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/(tc.rack_move_cost/cast(1000 as decimal)) else null end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度(米/秒)
nullif(tj.loading_parking_count,0) as loading_parking_count,   -- 交控停车次数（带载移动）
nullif(tj.loading_parking_time,0) as loading_parking_time,   -- 交控停车时长（带载移动）
nullif(tp.loading_guide_time,0) as loading_guide_time,   -- 末端引导时长（带载移动）
nullif(tj.loading_robot_rotate_num,0) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
nullif(coalesce(tp.before_putdown_cost_time,0)+COALESCE(tp.do_putdown_cost_time,0),0) as put_down_time_consuming,  -- 放下耗时（秒）
nullif(tp.putdown_is_rectification_num,0) as put_down_is_rectification_num,  -- 放下发生纠偏次数
nullif(tp.guide_time_consuming,0)  as guide_time_consuming,  -- 末端引导耗时(秒)
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,  -- 机器人旋转次数
t.create_time as order_create_time,   -- 作业单创建时间
tc.order_update_time as order_completed_time  -- 作业单完成时间

from phoenix_rss.dbo.transport_order_carrier_cost tc  -- 作业单到达终点状态一分钟内会写数据
inner join phoenix_rss.dbo.transport_order t on t.order_no = tc.order_no -- 作业单表
-- 根据作业单链路表找分配机器人
left join
(select
t1.order_no,
t1.dispatch_robot_code_num,  -- 分配机器人数量
t2.group_concat_robot_code as dispatch_robot_code_str,  -- 分配的机器人
t3.group_concat_first_classification as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num  -- 分配机器人数量
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by tc.order_no)t1
left join
(select order_no,
stuff ((select ',' + T.robot_code
from (select distinct tc.order_no,tk.robot_code
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time) T
where A.order_no=T.order_no
for XML PATH('')
),1,1,'') as group_concat_robot_code
from (select distinct tc.order_no,tk.robot_code
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time) A
group by order_no)t2 on t2.order_no=t1.order_no
left join
(select order_no,
stuff ((select ',' + T.first_classification
from (select distinct tc.order_no,brt.first_classification
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order_carrier_job tk on tk.order_no = tc.order_no
left join phoenix_basic.dbo.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.dbo.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time) T
where A.order_no=T.order_no
for XML PATH('')
),1,1,'') as group_concat_first_classification
from (select distinct tc.order_no,brt.first_classification
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order_carrier_job tk on tk.order_no = tc.order_no
left join phoenix_basic.dbo.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.dbo.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time) A
group by order_no)t3 on t3.order_no=t1.order_no)tr on tr.order_no = t.order_no
-- action
left join
(select
t.order_no,
sum((case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal))  as order_actual_move_distance,  --  移动距离（米）
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal) end) as empty_move_distance,   -- 空车移动距离(米)
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal) end) as loading_move_distance,   -- 带载移动距离(米)
sum(rasd.rotate_count) as robot_rotate_num, -- 机器人旋转次数
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.rotate_count end) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.rotate_count end) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_count end) as  empty_parking_count,   -- 交控停车次数（空车移动）
sum(case when tas.is_loading=1 then tas.parking_count end) as  loading_parking_count,   -- 交控停车次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_time/cast(1000 as decimal) end) as empty_parking_time,   -- 交控停车时长（空车移动）
sum(case when tas.is_loading=1 then tas.parking_time/cast(1000 as decimal) end) as loading_parking_time   -- 交控停车时长（带载移动）
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.dbo.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.dbo.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
left join phoenix_rts.dbo.action_statistics_data tas on tas.action_uid = rasd.action_uid
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by t.order_no)tj on tj.order_no = t.order_no
-- operation
left join
(select
order_no,
sum(DATEDIFF(ms,do_rack_check_with_upcamera_before_liftup_start_time,do_rack_check_with_upcamera_before_liftup_end_time)/cast(1000 as decimal)) as before_liftup_cost_time,  -- 顶升前确定耗时
-- sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time)) as before_liftup_cost_time,  -- 顶升前确定耗时
sum(DATEDIFF(ms,do_liftup_start_time,do_liftup_end_time)/cast(1000 as decimal)) as do_liftup_cost_time,  -- 顶升动作耗时
-- sum(UNIX_TIMESTAMP(do_liftup_end_time) - UNIX_TIMESTAMP(do_liftup_start_time)) as do_liftup_cost_time,  -- 顶升动作耗时
sum(DATEDIFF(ms,do_rack_check_with_upcamera_after_liftup_start_time,do_rack_check_with_upcamera_after_liftup_end_time)/cast(1000 as decimal)) as after_liftup_cost_time,  -- 顶升后确定耗时
-- sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_start_time)) as after_liftup_cost_time,  -- 顶升后确定耗时
count(distinct case when (DATEDIFF(ms,do_rack_check_with_upcamera_before_liftup_start_time,do_rack_check_with_upcamera_before_liftup_end_time)/cast(1000 as decimal))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
-- count(distinct case when (UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
sum(DATEDIFF(ms,do_guide_before_putdown_start_time,do_guide_before_putdown_end_time)/cast(1000 as decimal)) as before_putdown_cost_time,  -- 降下前确定耗时
-- sum(UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time)) as before_putdown_cost_time,  -- 降下前确定耗时
sum(DATEDIFF(ms,do_putdown_start_time,do_putdown_end_time)/cast(1000 as decimal)) as do_putdown_cost_time,  -- 降下动作耗时
-- sum(UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time)) as do_putdown_cost_time,  -- 降下动作耗时
count(distinct case when (DATEDIFF(ms,do_guide_before_putdown_start_time,do_guide_before_putdown_end_time)/cast(1000 as decimal))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
-- count(distinct case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
sum(DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) as guide_time_consuming,   -- 末端引导耗时(秒)
-- sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming,   -- 末端引导耗时(秒)
sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
-- sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) end) as loading_guide_time   -- 末端引导时长（带载移动）
-- sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as loading_guide_time   -- 末端引导时长（带载移动）
from
(select
t.order_no,
t1.action_uid,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_before_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_before_liftup_end_time,
max(case when t2.operation_name='DoLiftUp' then t2.start_time end) as do_liftup_start_time,
max(case when t2.operation_name='DoLiftUp' then t2.end_time end) as do_liftup_end_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_after_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_after_liftup_end_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rss.dbo.transport_order_carrier_cost tc
inner join phoenix_rss.dbo.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.dbo.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.dbo.job_action_statistics_data t1 on t1.job_sn = tj.job_sn
inner join phoenix_rms.dbo.job_action_operation_record t2 on t2.action_uid =t1.action_uid
and t2.operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp','doGuideBeforePutDown','DoPutDown','terminalGuide')
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time
group by t.order_no,t1.action_uid,t1.is_loading,t1.action_code)t
group by order_no)tp on tp.order_no = t.order_no
where tc.update_time >= @dt_hour_start_time and tc.update_time < @dt_next_hour_start_time





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
t.upstream_order_no,   -- 上游作业单ID
t.order_no,  -- 搬运作业单ID
t.scenario  as scene_type,  -- 场景类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end as start_point,  -- 起始点
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknown' end as start_area,   -- 起始区域
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end as target_point,  -- 目标点
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknown' end as target_area,  -- 目标区域
t.order_state,  -- 搬运作业单状态
tj.dispatch_robot_code_num,  -- 分配机器人数量
tj.dispatch_robot_code_str,  -- 分配的机器人
tj.dispatch_robot_classification_str,  -- 分配的机器人类型
nullif(tc.total_cost,0)/1000 as total_time_consuming,  -- 总耗时（秒）
nullif(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,  -- 分车耗时(秒)
nullif(tc.move_cost,0)/1000 as empty_move_time_consuming,  -- 空车移动耗时(秒)
cast(nullif(tj.empty_move_distance,0) as decimal(20,10)) as empty_move_distance,   -- 空车移动距离(米)
cast(case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/(tc.move_cost/1000) else null end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度(米/秒)
nullif(tj.empty_parking_count,0) as empty_parking_count,   -- 交控停车次数（空车移动）
nullif(tj.empty_parking_time,0) as empty_parking_time,   -- 交控停车时长（空车移动）
nullif(tp.empty_guide_time,0) as empty_guide_time,   -- 末端引导时长（空车移动）
nullif(tj.empty_robot_rotate_num,0) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
nullif(COALESCE(tp.before_liftup_cost_time,0)+COALESCE(tp.do_liftup_cost_time,0)+COALESCE(tp.after_liftup_cost_time,0),0) as lift_up_time_consuming, -- 顶升耗时（秒）
nullif(tp.liftup_is_rectification_num,0) as lift_up_is_rectification_num,  -- 顶升发生纠偏次数
nullif(tc.rack_move_cost,0)/1000 as loading_move_time_consuming,  -- 带载移动耗时(秒)
cast(nullif(tj.loading_move_distance,0) as decimal(20,10)) as loading_move_distance,  -- 带载移动距离(米)
cast(case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/(tc.rack_move_cost/1000) else null end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度(米/秒)
nullif(tj.loading_parking_count,0) as loading_parking_count,   -- 交控停车次数（带载移动）
nullif(tj.loading_parking_time,0) as loading_parking_time,   -- 交控停车时长（带载移动）
nullif(tp.loading_guide_time,0) as loading_guide_time,   -- 末端引导时长（带载移动）
nullif(tj.loading_robot_rotate_num,0) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
nullif(coalesce(tp.before_putdown_cost_time,0)+COALESCE(tp.do_putdown_cost_time,0),0) as put_down_time_consuming,  -- 放下耗时（秒）
nullif(tp.putdown_is_rectification_num,0) as put_down_is_rectification_num,  -- 放下发生纠偏次数
nullif(tp.guide_time_consuming,0)  as guide_time_consuming,  -- 末端引导耗时(秒)
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,  -- 机器人旋转次数
t.create_time as order_create_time,   -- 作业单创建时间
tc.order_update_time as order_completed_time  -- 作业单完成时间

from phoenix_rss.transport_order_carrier_cost tc  -- 作业单到达终点状态一分钟内会写数据
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no -- 作业单表
/*
-- 根据作业单链路表找分配机器人
left join
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num,  -- 分配机器人数量
group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,  -- 分配的机器人
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by tc.order_no)tr on tr.order_no = t.order_no
*/
-- action
left join
(select 
t.order_no,
sum((case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000)  as order_actual_move_distance,  --  移动距离（米）
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000 end) as empty_move_distance,   -- 空车移动距离(米)
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / 1000 end) as loading_move_distance,   -- 带载移动距离(米)
sum(rasd.rotate_count) as robot_rotate_num, -- 机器人旋转次数
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.rotate_count end) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.rotate_count end) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_count end) as  empty_parking_count,   -- 交控停车次数（空车移动）
sum(case when tas.is_loading=1 then tas.parking_count end) as  loading_parking_count,   -- 交控停车次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_time/1000 end) as empty_parking_time,   -- 交控停车时长（空车移动）
sum(case when tas.is_loading=1 then tas.parking_time/1000 end) as loading_parking_time,   -- 交控停车时长（带载移动）
count(distinct tj.robot_code)                   as dispatch_robot_code_num,  -- 分配机器人数量 
group_concat(distinct tj.robot_code)            as dispatch_robot_code_str,  -- 分配的机器人
group_concat(distinct brt.first_classification) as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
left join phoenix_rts.action_statistics_data tas on tas.action_uid = rasd.action_uid
left join phoenix_basic.basic_robot br on br.robot_code = tj.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by t.order_no)tj on tj.order_no = t.order_no
-- operation
left join
(select
order_no,
sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time)) as before_liftup_cost_time,  -- 顶升前确定耗时
sum(UNIX_TIMESTAMP(do_liftup_end_time) - UNIX_TIMESTAMP(do_liftup_start_time)) as do_liftup_cost_time,  -- 顶升动作耗时
sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_start_time)) as after_liftup_cost_time,  -- 顶升后确定耗时
count(distinct case when (UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
sum(UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time)) as before_putdown_cost_time,  -- 降下前确定耗时
sum(UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time)) as do_putdown_cost_time,  -- 降下动作耗时
count(distinct case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming,   -- 末端引导耗时(秒)
sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as loading_guide_time   -- 末端引导时长（带载移动）
from
(select
t.order_no,
t1.action_uid,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_before_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_before_liftup_end_time,
max(case when t2.operation_name='DoLiftUp' then t2.start_time end) as do_liftup_start_time,
max(case when t2.operation_name='DoLiftUp' then t2.end_time end) as do_liftup_end_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_after_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_after_liftup_end_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data t1 on t1.job_sn = tj.job_sn
inner join phoenix_rms.job_action_operation_record t2 on t2.action_uid =t1.action_uid
and t2.operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp','doGuideBeforePutDown','DoPutDown','terminalGuide')
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by t.order_no,t1.action_uid,t1.is_loading,t1.action_code)t
group by order_no)tp on tp.order_no = t.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
t.upstream_order_no,   -- 上游作业单ID
t.order_no,  -- 搬运作业单ID
t.scenario  as scene_type,  -- 场景类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end as start_point,  -- 起始点
case when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code else 'unknown' end as start_area,   -- 起始区域
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end as target_point,  -- 目标点
case when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code else 'unknown' end as target_area,  -- 目标区域
t.order_state,  -- 搬运作业单状态
tr.dispatch_robot_code_num,  -- 分配机器人数量
tr.dispatch_robot_code_str,  -- 分配的机器人
tr.dispatch_robot_classification_str,  -- 分配的机器人类型
nullif(tc.total_cost,0)/cast(1000 as decimal) as total_time_consuming,  -- 总耗时（秒）
nullif(tc.assign_cost,0)/cast(1000 as decimal) as waiting_robot_time_consuming,  -- 分车耗时(秒)
nullif(tc.move_cost,0)/cast(1000 as decimal) as empty_move_time_consuming,  -- 空车移动耗时(秒)
cast(nullif(tj.empty_move_distance,0) as decimal(20,10)) as empty_move_distance,   -- 空车移动距离(米)
cast(case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/(tc.move_cost/cast(1000 as decimal)) else null end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度(米/秒)
nullif(tj.empty_parking_count,0) as empty_parking_count,   -- 交控停车次数（空车移动）
nullif(tj.empty_parking_time,0) as empty_parking_time,   -- 交控停车时长（空车移动）
nullif(tp.empty_guide_time,0) as empty_guide_time,   -- 末端引导时长（空车移动）
nullif(tj.empty_robot_rotate_num,0) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
nullif(COALESCE(tp.before_liftup_cost_time,0)+COALESCE(tp.do_liftup_cost_time,0)+COALESCE(tp.after_liftup_cost_time,0),0) as lift_up_time_consuming, -- 顶升耗时（秒）
nullif(tp.liftup_is_rectification_num,0) as lift_up_is_rectification_num,  -- 顶升发生纠偏次数
nullif(tc.rack_move_cost,0)/cast(1000 as decimal) as loading_move_time_consuming,  -- 带载移动耗时(秒)
cast(nullif(tj.loading_move_distance,0) as decimal(20,10)) as loading_move_distance,  -- 带载移动距离(米)
cast(case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/(tc.rack_move_cost/cast(1000 as decimal)) else null end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度(米/秒)
nullif(tj.loading_parking_count,0) as loading_parking_count,   -- 交控停车次数（带载移动）
nullif(tj.loading_parking_time,0) as loading_parking_time,   -- 交控停车时长（带载移动）
nullif(tp.loading_guide_time,0) as loading_guide_time,   -- 末端引导时长（带载移动）
nullif(tj.loading_robot_rotate_num,0) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
nullif(coalesce(tp.before_putdown_cost_time,0)+COALESCE(tp.do_putdown_cost_time,0),0) as put_down_time_consuming,  -- 放下耗时（秒）
nullif(tp.putdown_is_rectification_num,0) as put_down_is_rectification_num,  -- 放下发生纠偏次数
nullif(tp.guide_time_consuming,0)  as guide_time_consuming,  -- 末端引导耗时(秒)
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,  -- 机器人旋转次数
t.create_time as order_create_time,   -- 作业单创建时间
tc.order_update_time as order_completed_time  -- 作业单完成时间

from phoenix_rss.transport_order_carrier_cost tc  -- 作业单到达终点状态一分钟内会写数据
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no -- 作业单表
-- 根据作业单链路表找分配机器人
left join
(select
t1.order_no,
t1.dispatch_robot_code_num,  -- 分配机器人数量
t2.group_concat_robot_code as dispatch_robot_code_str,  -- 分配的机器人
t3.group_concat_first_classification as dispatch_robot_classification_str  -- 分配的机器人类型（一级分类）
from
(select tc.order_no,
count(distinct tk.robot_code)                   as dispatch_robot_code_num  -- 分配机器人数量
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by tc.order_no)t1
left join
(select order_no,
stuff ((select ',' + T.robot_code
from (select distinct tc.order_no,tk.robot_code
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}) T
where A.order_no=T.order_no
for XML PATH('')
),1,1,'') as group_concat_robot_code
from (select distinct tc.order_no,tk.robot_code
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_carrier_job tk on tk.order_no = tc.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}) A
group by order_no)t2 on t2.order_no=t1.order_no
left join
(select order_no,
stuff ((select ',' + T.first_classification
from (select distinct tc.order_no,brt.first_classification
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_carrier_job tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}) T
where A.order_no=T.order_no
for XML PATH('')
),1,1,'') as group_concat_first_classification
from (select distinct tc.order_no,brt.first_classification
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_carrier_job tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}) A
group by order_no)t3 on t3.order_no=t1.order_no)tr on tr.order_no = t.order_no
-- action
left join
(select
t.order_no,
sum((case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal))  as order_actual_move_distance,  --  移动距离（米）
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal) end) as empty_move_distance,   -- 空车移动距离(米)
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then (case when rasd.actual_move_distance<0 then 0 when rasd.actual_move_distance>9999999 then 9999999 else rasd.actual_move_distance end) / cast(1000 as decimal) end) as loading_move_distance,   -- 带载移动距离(米)
sum(rasd.rotate_count) as robot_rotate_num, -- 机器人旋转次数
sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.rotate_count end) as empty_robot_rotate_num, -- 机器人旋转次数（空车移动）
sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1) then rasd.rotate_count end) as loading_robot_rotate_num, -- 机器人旋转次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_count end) as  empty_parking_count,   -- 交控停车次数（空车移动）
sum(case when tas.is_loading=1 then tas.parking_count end) as  loading_parking_count,   -- 交控停车次数（带载移动）
sum(case when tas.is_loading=0 then tas.parking_time/cast(1000 as decimal) end) as empty_parking_time,   -- 交控停车时长（空车移动）
sum(case when tas.is_loading=1 then tas.parking_time/cast(1000 as decimal) end) as loading_parking_time   -- 交控停车时长（带载移动）
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
left join phoenix_rts.action_statistics_data tas on tas.action_uid = rasd.action_uid
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by t.order_no)tj on tj.order_no = t.order_no
-- operation
left join
(select
order_no,
sum(DATEDIFF(ms,do_rack_check_with_upcamera_before_liftup_start_time,do_rack_check_with_upcamera_before_liftup_end_time)/cast(1000 as decimal)) as before_liftup_cost_time,  -- 顶升前确定耗时
-- sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time)) as before_liftup_cost_time,  -- 顶升前确定耗时
sum(DATEDIFF(ms,do_liftup_start_time,do_liftup_end_time)/cast(1000 as decimal)) as do_liftup_cost_time,  -- 顶升动作耗时
-- sum(UNIX_TIMESTAMP(do_liftup_end_time) - UNIX_TIMESTAMP(do_liftup_start_time)) as do_liftup_cost_time,  -- 顶升动作耗时
sum(DATEDIFF(ms,do_rack_check_with_upcamera_after_liftup_start_time,do_rack_check_with_upcamera_after_liftup_end_time)/cast(1000 as decimal)) as after_liftup_cost_time,  -- 顶升后确定耗时
-- sum(UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_start_time)) as after_liftup_cost_time,  -- 顶升后确定耗时
count(distinct case when (DATEDIFF(ms,do_rack_check_with_upcamera_before_liftup_start_time,do_rack_check_with_upcamera_before_liftup_end_time)/cast(1000 as decimal))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
-- count(distinct case when (UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time))>0.5 then action_uid end) as  liftup_is_rectification_num,  -- 发生顶升纠偏次数 -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
sum(DATEDIFF(ms,do_guide_before_putdown_start_time,do_guide_before_putdown_end_time)/cast(1000 as decimal)) as before_putdown_cost_time,  -- 降下前确定耗时
-- sum(UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time)) as before_putdown_cost_time,  -- 降下前确定耗时
sum(DATEDIFF(ms,do_putdown_start_time,do_putdown_end_time)/cast(1000 as decimal)) as do_putdown_cost_time,  -- 降下动作耗时
-- sum(UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time)) as do_putdown_cost_time,  -- 降下动作耗时
count(distinct case when (DATEDIFF(ms,do_guide_before_putdown_start_time,do_guide_before_putdown_end_time)/cast(1000 as decimal))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
-- count(distinct case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then action_uid end) as putdown_is_rectification_num,  -- 发生降下纠偏次数 -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
sum(DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) as guide_time_consuming,   -- 末端引导耗时(秒)
-- sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming,   -- 末端引导耗时(秒)
sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
-- sum(case when action_code = 'MOVE_LIFT_UP' or (action_code = 'MOVE' and is_loading = 0) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as empty_guide_time,   -- 末端引导时长（空车移动）
sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (DATEDIFF(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)) end) as loading_guide_time   -- 末端引导时长（带载移动）
-- sum(case when action_code = 'MOVE_PUT_DOWN' or (action_code = 'MOVE' and is_loading = 1) then (unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)) end) as loading_guide_time   -- 末端引导时长（带载移动）
from
(select
t.order_no,
t1.action_uid,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_before_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_before_liftup_end_time,
max(case when t2.operation_name='DoLiftUp' then t2.start_time end) as do_liftup_start_time,
max(case when t2.operation_name='DoLiftUp' then t2.end_time end) as do_liftup_end_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_after_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_after_liftup_end_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data t1 on t1.job_sn = tj.job_sn
inner join phoenix_rms.job_action_operation_record t2 on t2.action_uid =t1.action_uid
and t2.operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp','doGuideBeforePutDown','DoPutDown','terminalGuide')
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
group by t.order_no,t1.action_uid,t1.is_loading,t1.action_code)t
group by order_no)tp on tp.order_no = t.order_no
where tc.update_time >= {{ dt_hour_start_time }} and tc.update_time < {{ dt_next_hour_start_time }}
{% endif %}