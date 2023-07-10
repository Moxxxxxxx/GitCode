##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_path_efficiency
(
    `id`                         int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                 date         NOT NULL COMMENT '日期',
    `start_point`                varchar(100) NOT NULL COMMENT '起始点',
    `target_point`               varchar(100) NOT NULL COMMENT '目标点',
    `order_type`                 varchar(100)          DEFAULT NULL COMMENT '作业单类型',
    `transport_order_num`        varchar(100)          DEFAULT NULL COMMENT '搬运次数',
    `total_transport_cost_time`  decimal(10, 3)        DEFAULT NULL COMMENT '总搬运耗时(秒)',
    `median_transport_cost_time` decimal(10, 3)        DEFAULT NULL COMMENT '中位数搬运耗时(秒)',
    `avg_transport_cost_time`    decimal(10, 3)        DEFAULT NULL COMMENT '平均搬运耗时(秒)',
    `min_transport_cost_time`    decimal(10, 3)        DEFAULT NULL COMMENT '最短搬运耗时(秒)',
    `max_transport_cost_time`    decimal(10, 3)        DEFAULT NULL COMMENT '最长搬运耗时(秒)',
    `created_time`               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单路线效率';	







##step2:删除当天相关数据
DELETE
FROM qt_smartreport.qt_transport_order_path_efficiency
WHERE date_value = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  
   
  
  
  
  
##step3:插入当天相关数据
insert into qt_smartreport.qt_transport_order_path_efficiency(date_value, start_point, target_point, order_type,
                                                              transport_order_num,
                                                              total_transport_cost_time, median_transport_cost_time,
                                                              avg_transport_cost_time,
                                                              min_transport_cost_time, max_transport_cost_time)
select date_value,
       start_point,
       target_point,
       order_type,
       sum(transport_order_num)        as transport_order_num,
       sum(total_transport_cost_time)  as total_transport_cost_time,
       sum(median_transport_cost_time) as median_transport_cost_time,
       sum(avg_transport_cost_time)    as avg_transport_cost_time,
       sum(min_transport_cost_time)    as min_transport_cost_time,
       sum(max_transport_cost_time)    as max_transport_cost_time
from (SELECT date_format(t.update_time, '%Y-%m-%d')                                                     as `date_value`,
             coalesce(case
                 when t1.order_id is not null then t1.start_point
                 when t2.order_id is not null
                     then t2.source_point_code end,'unknow')                                                      as `start_point`,
             coalesce(case
                 when t1.order_id is not null then t1.target_point
                 when t2.order_id is not null
                     then t2.target_point_code end,'unknow')                                                      as `target_point`,
             t.order_type                                                                               as `order_type`,
             coalesce(count(distinct t.order_id), 0)                                                    as `transport_order_num`,
             cast(sum(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `total_transport_cost_time`,
             null                                                                                       as `median_transport_cost_time`,
             cast(avg(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `avg_transport_cost_time`,

             cast(min(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `min_transport_cost_time`,
             cast(max(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `max_transport_cost_time`
      from phoenix_rms.transport_order t
               left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
               left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
      where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2, 3, 4

      union all
      select date_value,
             start_point,
             target_point,
             order_type,
             null                           as transport_order_num,
             null                           as total_transport_cost_time,
             avg(total_transport_cost_time) as median_transport_cost_time,
             null                           as avg_transport_cost_time,
             null                           as min_transport_cost_time,
             null                           as max_transport_cost_time


      from (select a.date_value,
                   a.start_point,
                   a.target_point,
                   a.order_type,
                   a.total_transport_cost_time
            from (SELECT date_format(t.update_time, '%Y-%m-%d')                        as `date_value`,
                         coalesce(case
                             when t1.order_id is not null then t1.start_point
                             when t2.order_id is not null
                                 then t2.source_point_code end,'unknow')                         as `start_point`,
                         coalesce(case
                             when t1.order_id is not null then t1.target_point
                             when t2.order_id is not null
                                 then t2.target_point_code end ,'unknow')                        as `target_point`,
                         t.order_type,
                         unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as `total_transport_cost_time`
                  from phoenix_rms.transport_order t
                           left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
                           left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
                  where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                 ) a
                     left join
                 (SELECT date_format(t.update_time, '%Y-%m-%d')                        as `date_value`,
                         coalesce(case
                             when t1.order_id is not null then t1.start_point
                             when t2.order_id is not null
                                 then t2.source_point_code end ,'unknow')                        as `start_point`,
                         coalesce(case
                             when t1.order_id is not null then t1.target_point
                             when t2.order_id is not null
                                 then t2.target_point_code end ,'unknow')                        as `target_point`,
                         t.order_type,
                         unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as `total_transport_cost_time`
                  from phoenix_rms.transport_order t
                           left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
                           left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
                  where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                 ) b on a.date_value = b.date_value and a.start_point = b.start_point and
                        a.target_point = b.target_point and
                        a.order_type = b.order_type
            group by a.date_value,
                     a.start_point,
                     a.target_point,
                     a.order_type,
                     a.total_transport_cost_time
            HAVING SUM(a.total_transport_cost_time = b.total_transport_cost_time) >=
                   ABS(SUM(SIGN(a.total_transport_cost_time - b.total_transport_cost_time)))
           ) t
      group by date_value,
               start_point,
               target_point,
               order_type
     ) a
group by 1, 2, 3, 4;










###全量逻辑
#搬运作业单路线效率
select date_value,
       start_point,
       target_point,
       order_type,
       sum(transport_order_num)        as transport_order_num,
       sum(total_transport_cost_time)  as total_transport_cost_time,
       sum(median_transport_cost_time) as median_transport_cost_time,
       sum(avg_transport_cost_time)    as avg_transport_cost_time,
       sum(min_transport_cost_time)    as min_transport_cost_time,
       sum(max_transport_cost_time)    as max_transport_cost_time
from (SELECT date_format(t.update_time, '%Y-%m-%d')                                                     as `date_value`,
             case
                 when t1.order_id is not null then t1.start_point
                 when t2.order_id is not null
                     then t2.source_point_code end                                                      as `start_point`,
             case
                 when t1.order_id is not null then t1.target_point
                 when t2.order_id is not null
                     then t2.target_point_code end                                                      as `target_point`,
             t.order_type                                                                               as `order_type`,
             coalesce(count(distinct t.order_id), 0)                                                    as `transport_order_num`,
             cast(sum(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `total_transport_cost_time`,
             null                                                                                       as `median_transport_cost_time`,
             cast(avg(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `avg_transport_cost_time`,

             cast(min(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `min_transport_cost_time`,
             cast(max(unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) as decimal(10, 3)) as `max_transport_cost_time`
      from phoenix_rms.transport_order t
               left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
               left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
      where t.state = 'DONE'
--   and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2, 3, 4

      union all
      select date_value,
             start_point,
             target_point,
             order_type,
             null                           as transport_order_num,
             null                           as total_transport_cost_time,
             avg(total_transport_cost_time) as median_transport_cost_time,
             null                           as avg_transport_cost_time,
             null                           as min_transport_cost_time,
             null                           as max_transport_cost_time


      from (select a.date_value,
                   a.start_point,
                   a.target_point,
                   a.order_type,
                   a.total_transport_cost_time
            from (SELECT date_format(t.update_time, '%Y-%m-%d')                        as `date_value`,
                         case
                             when t1.order_id is not null then t1.start_point
                             when t2.order_id is not null
                                 then t2.source_point_code end                         as `start_point`,
                         case
                             when t1.order_id is not null then t1.target_point
                             when t2.order_id is not null
                                 then t2.target_point_code end                         as `target_point`,
                         t.order_type,
                         unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as `total_transport_cost_time`
                  from phoenix_rms.transport_order t
                           left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
                           left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
                  where t.state = 'DONE'
--   and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                 ) a
                     left join
                 (SELECT date_format(t.update_time, '%Y-%m-%d')                        as `date_value`,
                         case
                             when t1.order_id is not null then t1.start_point
                             when t2.order_id is not null
                                 then t2.source_point_code end                         as `start_point`,
                         case
                             when t1.order_id is not null then t1.target_point
                             when t2.order_id is not null
                                 then t2.target_point_code end                         as `target_point`,
                         t.order_type,
                         unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as `total_transport_cost_time`
                  from phoenix_rms.transport_order t
                           left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
                           left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
                  where t.state = 'DONE'
--   and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                 ) b on a.date_value = b.date_value and a.start_point = b.start_point and
                        a.target_point = b.target_point and
                        a.order_type = b.order_type
            group by a.date_value,
                     a.start_point,
                     a.target_point,
                     a.order_type,
                     a.total_transport_cost_time
            HAVING SUM(a.total_transport_cost_time = b.total_transport_cost_time) >=
                   ABS(SUM(SIGN(a.total_transport_cost_time - b.total_transport_cost_time)))
           ) t
      group by date_value,
               start_point,
               target_point,
               order_type
     ) a
group by 1, 2, 3, 4;