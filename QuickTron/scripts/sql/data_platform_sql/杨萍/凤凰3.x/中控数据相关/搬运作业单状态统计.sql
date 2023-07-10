##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_state_time_consuming_detail
(
    `id`                   int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date         NOT NULL COMMENT '日期',
    `order_id`             varchar(100) NOT NULL COMMENT '作业单ID',
    `start_point`          varchar(100)          DEFAULT NULL COMMENT '起始点',
    `target_point`         varchar(100)          DEFAULT NULL COMMENT '目标点',
    `order_type`           varchar(100)          DEFAULT NULL COMMENT '作业单类型',
    `order_create_time`    datetime              DEFAULT NULL COMMENT '作业单创建时间',
    `order_done_time`      datetime              DEFAULT NULL COMMENT '作业单完成时间',
    `transport_cost_time`  decimal(10, 3)        DEFAULT NULL COMMENT '总耗时(秒)',
    `init_job_cost_time`   decimal(10, 3)        DEFAULT NULL COMMENT '分车耗时(秒)',
    `move_cost_time`       decimal(10, 3)        DEFAULT NULL COMMENT '空车移动耗时(秒)',
    `lift_up_cost_time`    decimal(10, 3)        DEFAULT NULL COMMENT '顶升耗时(秒)',
    `rack_move_cost_time`  decimal(10, 3)        DEFAULT NULL COMMENT '带载耗时(秒)',
    `put_down_cost_time`   decimal(10, 3)        DEFAULT NULL COMMENT '放下耗时(秒)',
    `again_move_cost_time` decimal(10, 3)        DEFAULT NULL COMMENT '二次移动耗时(秒)',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单状态耗时明细';	




##step2:删除当天相关数据
DELETE
FROM qt_smartreport.qt_transport_order_state_time_consuming_detail
WHERE date_value = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  
  
  


##step3:插入当天相关数据
insert into qt_smartreport.qt_transport_order_state_time_consuming_detail(date_value, order_id, start_point,
                                                                          target_point, order_type, order_create_time,
                                                                          order_done_time, transport_cost_time,
                                                                          init_job_cost_time, move_cost_time,
                                                                          lift_up_cost_time, rack_move_cost_time,
                                                                          put_down_cost_time, again_move_cost_time)
select date_value,
       order_id,
       max(start_point)          as start_point,
       max(target_point)         as target_point,
       max(order_type)           as order_type,
       max(create_time)          as order_create_time,
       max(update_time)          as order_done_time,
       sum(transport_cost_time)  as transport_cost_time,
       sum(init_job_cost_time)   as init_job_cost_time,
       sum(move_cost_time)       as move_cost_time,
       sum(lift_up_cost_time)    as lift_up_cost_time,
       sum(rack_move_cost_time)  as rack_move_cost_time,
       sum(put_down_cost_time)   as put_down_cost_time,
       sum(again_move_cost_time) as again_move_cost_time
from (select date_format(t.update_time, '%Y-%m-%d')                             as date_value,
             t.order_id,
             coalesce(case
                 when t1.order_id is not null then t1.start_point
                 when t2.order_id is not null then t2.source_point_code end,'unknow')     as `start_point`,
             coalesce(case
                 when t1.order_id is not null then t1.target_point
                 when t2.order_id is not null then t2.target_point_code end ,'unknow')    as `target_point`,
             t.order_type,
             t.create_time,
             t.update_time,
             sum(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as transport_cost_time,
             null                                                               as init_job_cost_time,
             null                                                               as move_cost_time,
             null                                                               as lift_up_cost_time,
             null                                                               as rack_move_cost_time,
             null                                                               as put_down_cost_time,
             null                                                               as again_move_cost_time
      from phoenix_rms.transport_order t
               left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
               left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
      where t.state = 'DONE'
        and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by t.order_id

      union all

      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as init_job_cost_time,
             null                                                        as move_cost_time,
             null                                                        as lift_up_cost_time,
             null                                                        as rack_move_cost_time,
             null                                                        as put_down_cost_time,
             null                                                        as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'INIT_JOB') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'WAITING_ROBOT') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3, 4, 5) t
      group by date_value, order_id


      union all

      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             null                                                        as init_job_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as move_cost_time,
             null                                                        as lift_up_cost_time,
             null                                                        as rack_move_cost_time,
             null                                                        as put_down_cost_time,
             null                                                        as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'MOVE_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3, 4, 5) t
      group by date_value, order_id


      union all


      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             null                                                        as init_job_cost_time,
             null                                                        as move_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as lift_up_cost_time,
             null                                                        as rack_move_cost_time,
             null                                                        as put_down_cost_time,
             null                                                        as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'LIFT_UP_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'LIFT_UP_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
      group by date_value, order_id


      union all


      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             null                                                        as init_job_cost_time,
             null                                                        as move_cost_time,
             null                                                        as lift_up_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as rack_move_cost_time,
             null                                                        as put_down_cost_time,
             null                                                        as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'RACK_MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'RACK_MOVE_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
      group by date_value, order_id

      union all

      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             null                                                        as init_job_cost_time,
             null                                                        as move_cost_time,
             null                                                        as lift_up_cost_time,
             null                                                        as rack_move_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as put_down_cost_time,
             null                                                        as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'PUT_DOWN_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'PUT_DOWN_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
      group by date_value, order_id

      union all

      select date_value,
             order_id,
             null                                                        as start_point,
             null                                                        as target_point,
             null                                                        as order_type,
             null                                                        as create_time,
             null                                                        as update_time,
             null                                                        as transport_cost_time,
             null                                                        as init_job_cost_time,
             null                                                        as move_cost_time,
             null                                                        as lift_up_cost_time,
             null                                                        as rack_move_cost_time,
             null                                                        as put_down_cost_time,
             sum(unix_timestamp(done_time) - unix_timestamp(start_time)) as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'AGAIN_MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
                      and date_format(t1.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'AGAIN_MOVE_START') t2
                 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
      group by date_value, order_id
     ) a
group by 1, 2;



	

###全量逻辑
#搬运作业单状态耗时明细
select date_value,
       order_id,
       max(start_point)          as start_point,
       max(target_point)         as target_point,
       max(order_type)           as order_type,
       max(create_time)          as order_create_time,
       max(update_time)          as order_done_time,
       sum(transport_cost_time)  as transport_cost_time,
       sum(init_job_cost_time)   as init_job_cost_time,
       sum(move_cost_time)       as move_cost_time,
       sum(lift_up_cost_time)    as lift_up_cost_time,
       sum(rack_move_cost_time)  as rack_move_cost_time,
       sum(put_down_cost_time)   as put_down_cost_time,
       sum(again_move_cost_time) as again_move_cost_time
from (select date_format(t.update_time, '%Y-%m-%d')                         as date_value,
             t.order_id,
             case
                 when t1.order_id is not null then t1.start_point
                 when t2.order_id is not null then t2.source_point_code end as `start_point`,
             case
                 when t1.order_id is not null then t1.target_point
                 when t2.order_id is not null then t2.target_point_code end as `target_point`,
             t.order_type,
             t.create_time,
             t.update_time,
             unix_timestamp(t.update_time) - unix_timestamp(t.create_time)  as transport_cost_time,
             null                                                           as init_job_cost_time,
             null                                                           as move_cost_time,
             null                                                           as lift_up_cost_time,
             null                                                           as rack_move_cost_time,
             null                                                           as put_down_cost_time,
             null                                                           as again_move_cost_time
      from phoenix_rms.transport_order t
               left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
               left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
      where t.state = 'DONE'
--   and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')


      union all

      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as init_job_cost_time,
             null                                                   as move_cost_time,
             null                                                   as lift_up_cost_time,
             null                                                   as rack_move_cost_time,
             null                                                   as put_down_cost_time,
             null                                                   as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'INIT_JOB') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'WAITING_ROBOT') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t


      union all

      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             null                                                   as init_job_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as move_cost_time,
             null                                                   as lift_up_cost_time,
             null                                                   as rack_move_cost_time,
             null                                                   as put_down_cost_time,
             null                                                   as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'MOVE_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t


      union all


      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             null                                                   as init_job_cost_time,
             null                                                   as move_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as lift_up_cost_time,
             null                                                   as rack_move_cost_time,
             null                                                   as put_down_cost_time,
             null                                                   as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'LIFT_UP_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'LIFT_UP_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t


      union all


      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             null                                                   as init_job_cost_time,
             null                                                   as move_cost_time,
             null                                                   as lift_up_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as rack_move_cost_time,
             null                                                   as put_down_cost_time,
             null                                                   as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'RACK_MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'RACK_MOVE_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t

      union all

      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             null                                                   as init_job_cost_time,
             null                                                   as move_cost_time,
             null                                                   as lift_up_cost_time,
             null                                                   as rack_move_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as put_down_cost_time,
             null                                                   as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'PUT_DOWN_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'PUT_DOWN_START') t2 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
			
      union all
	  
      select date_value,
             order_id,
             null                                                   as start_point,
             null                                                   as target_point,
             null                                                   as order_type,
             null                                                   as create_time,
             null                                                   as update_time,
             null                                                   as transport_cost_time,
             null                                                   as init_job_cost_time,
             null                                                   as move_cost_time,
             null                                                   as lift_up_cost_time,
             null                                                   as rack_move_cost_time,
             null                                                   as put_down_cost_time,
             unix_timestamp(done_time) - unix_timestamp(start_time) as again_move_cost_time
      from (select t1.date_value,
                   t1.order_id,
                   t1.state,
                   t1.create_time      as done_time,
                   max(t2.create_time) as start_time
            from (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'AGAIN_MOVE_DONE') t1
                     left join
                 (select date_format(t1.update_time, '%Y-%m-%d') as date_value,
                         t.order_id,
                         t.state,
                         t.create_time
                  from phoenix_rms.transport_order_link t
                           inner join phoenix_rms.transport_order t1 on t1.order_id = t.order_id and t1.state = 'DONE'
--                       and date_format(t1.update_time, '%Y-%m-%d') =
--                           date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                  where t.state = 'AGAIN_MOVE_START') t2
                 on t2.order_id = t1.order_id and t2.create_time < t1.create_time
            group by 1, 2, 3) t
     ) a
group by 1, 2;