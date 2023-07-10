搬运作业单分时统计（开发页面）qt_smartreport.qt_transport_order_create_stat_detail当天逻辑

select t.order_id,
       t.create_time                                                 as order_create_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.create_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point	   
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id		 
where  date_format(t.create_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
;


----------------------------------------------------------------------------
--下单
select t.order_id,
       t.create_time                                            as order_create_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1) as scene_type,
       date_format(t.create_time, '%Y-%m-%d %H:00:00')          as stat_time,
       t.order_type,
       t.dispatch_robot_code                                    as robot_code,
       coalesce(t1.start_point, 'unknow')                       as start_point,
       coalesce(t1.target_point, 'unknow')                      as target_point
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
where date_format(t.create_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
union all
select order_id,
       order_create_time,
       scene_type,
       stat_time,
       order_type,
       robot_code,
       start_point,
       target_point
from qt_smartreport.qt_transport_order_create_stat_detail
