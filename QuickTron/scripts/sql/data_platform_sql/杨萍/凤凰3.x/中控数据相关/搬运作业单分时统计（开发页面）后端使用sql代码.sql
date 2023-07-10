code1:搬运作业单单量分时趋势

SELECT
    stat_time
    ,COUNT(DISTINCT order_id) AS orde_num  -- 订单完成量
    ,'order_done' AS order_sta_tag
FROM
    (
        SELECT 
            t.order_id AS order_id
            ,t.update_time                                                 as order_done_time
            ,substring(t.order_type, 1, instr(t.order_type, '_') - 1) AS scene_type
            ,date_format(t.update_time, '%Y-%m-%d %H:00:00') AS stat_time
            ,t.order_type
            ,t.dispatch_robot_code AS robot_code
            ,coalesce(t1.start_point, 'unknow') AS start_point
            ,coalesce(t1.target_point, 'unknow') AS target_point
        FROM
            phoenix_rms.transport_order t
        LEFT JOIN 
            phoenix_rss.rss_carrier_order t1 
        ON 
            t1.order_id = t.order_id	 
        WHERE 
            t.state = 'DONE'
            AND
            date_format(t.update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
        UNION ALL
        SELECT
            order_id
            ,order_done_time
            ,scene_type
            ,stat_time
            ,order_type
            ,robot_code
            ,start_point
            ,target_point
        FROM
            qt_smartreport.qt_transport_order_stat_detail
    ) t1
WHERE
    {w}
GROUP BY
    stat_time

UNION ALL

SELECT
    stat_time
    ,COUNT(DISTINCT order_id) AS orde_num  -- 下单量
    ,'order_create' AS order_sta_tag
FROM
    (
        SELECT 
            t.order_id
            ,t.create_time as order_create_time
            ,substring(t.order_type, 1, instr(t.order_type, '_') - 1) as scene_type
            ,date_format(t.create_time, '%Y-%m-%d %H:00:00') as stat_time
            ,t.order_type
            ,t.dispatch_robot_code as robot_code
            ,coalesce(t1.start_point, 'unknow') as start_point
            ,coalesce(t1.target_point, 'unknow') as target_point
        FROM 
            phoenix_rms.transport_order t
        LEFT JOIN 
            phoenix_rss.rss_carrier_order t1 
        ON 
            t1.order_id = t.order_id
        WHERE 
            date_format(t.create_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
        UNION ALL
        SELECT 
            order_id
            ,order_create_time
            ,scene_type
            ,stat_time
            ,order_type
            ,robot_code
            ,start_point
            ,target_point
        FROM 
            qt_smartreport.qt_transport_order_create_stat_detail
    ) t2
WHERE
    {w}
GROUP BY
    stat_time
	
	
	
	
	
code2:分时统计趋势

--历史
SELECT order_id
     , scene_type
     , stat_time
     , order_type
     , robot_code
     , start_point
     , target_point
     , total_time_consuming
     , init_job_time_consuming
     , move_time_consuming
     , lift_up_time_consuming
     , rack_move_time_consuming
     , put_down_time_consuming
     , guide_time_consuming
FROM qt_smartreport.qt_transport_order_stat_detail
WHERE {w}

UNION ALL

--当天
select t.order_id,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.update_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as total_time_consuming,
       coalesce(t2.init_job_time_consuming, 0)                       as init_job_time_consuming,
       coalesce(t3.move_time_consuming, 0)                           as move_time_consuming,
       coalesce(t4.lift_up_time_consuming, 0)                        as lift_up_time_consuming,
       coalesce(t5.rack_move_time_consuming, 0)                      as rack_move_time_consuming,
       coalesce(t6.put_down_time_consuming, 0)                       as put_down_time_consuming,
       null                                                          as guide_time_consuming
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as init_job_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'INIT_JOB') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'WAITING_ROBOT') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t2 on t2.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t3 on t3.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as lift_up_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t4 on t4.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as rack_move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t5 on t5.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as put_down_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t6 on t6.order_id = t.order_id
where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
  
  
  
  
code3: 分时统计明细 


 