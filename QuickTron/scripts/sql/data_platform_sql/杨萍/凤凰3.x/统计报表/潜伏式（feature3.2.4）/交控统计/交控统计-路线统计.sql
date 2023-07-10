-- 交控统计-Acition统计
-- 设置Action开始的时间所在范围
set @start_time = '2022-10-26 00:00:00.000';
set @end_time = '2022-10-26 23:59:59.999';
select @start_time,@end_time; 



select 
COALESCE(l.line_name, '未配置')  AS   `路线`, 
sum(t.actual_move_distance)/sum(t.estimate_distance) as `绕路距离比例`, 
sum(t.empty_move_distance)/sum(t.empty_move_time) as `空车移动速度（米/秒）`,  
sum(t.loading_move_distance)/sum(t.loading_move_time) as `带载移动速度（米/秒）`,  
sum(t.parking_count)/count(distinct t.action_uid) as `平均单次移动交控停车次数`,
sum(t.parking_time)/count(distinct t.action_uid) as `平均单次移动交控停车时长（秒）`,
sum(t.deadlock_count)/count(distinct t.action_uid) as `平均单次移动死锁次数`,
sum(t.deadlock_time)/count(distinct t.action_uid) as `平均单次移动解死锁时长（秒）`,
sum(t.quit_drl_count) as `退出区控次数`,
sum(t.rotate_count)/count(distinct t.action_uid) as `平均单次移动机器人旋转次数`,
sum(t.rack_rotate_times)/count(distinct t.action_uid) as `平均单次移动货架旋转次数`
from 
(select 
tja.msg_id as action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
tas.estimate_distance/1000 as  estimate_distance,  -- 预估行驶距离（米）
tas.rts_map_distance/1000 as actual_move_distance,  -- 实际行驶距离(米)
case when tja.is_loading = 0 then tas.rts_map_distance/1000 end as empty_move_distance,  -- 空车移动距离（米）
case when tja.is_loading = 0 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as empty_move_time,  -- 空车移动时长（秒）

case when tja.is_loading = 1 then tas.rts_map_distance/1000 end as loading_move_distance,  -- 带载移动距离（米）
case when tja.is_loading = 1 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as loading_move_time,  -- 带载移动时长（秒）
tas.parking_count,     -- 停车次数
tas.parking_time/1000 as parking_time,     -- 停车累计时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/1000 as  deadlock_time,    -- 死锁累计时长（秒）
tas.quit_drl_count ,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count,      -- 机器人旋转次数
tja.rack_rotate_times   --  货架旋转次数 
from phoenix_rms.job_action_statistics_data tja 
left join phoenix_rts.action_statistics_data tas on tas.action_uid = tja.msg_id 
left join phoenix_rss.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.transport_order t on tj.order_id = t.id
where tja.action_begin_time BETWEEN @start_time AND @end_time)t 
LEFT JOIN
(SELECT DISTINCT tmp1.id AS line_id
               , tmp1.line_name
               , tmp1.estimate_move_time_consuming
               , tmp2.start_point_code
               , tmp3.target_point_code
 FROM qt_smartreport.carry_job_line_info_v4 tmp1
          LEFT JOIN
      qt_smartreport.carry_job_start_point_code_v4 tmp2
      ON
          tmp1.id = tmp2.line_id
          LEFT JOIN
      qt_smartreport.carry_job_target_point_code_v4 tmp3
      ON
          tmp1.id = tmp3.line_id) l
ON
            t.order_start_point = l.start_point_code
        AND
            t.order_target_point = l.target_point_code
group by 1