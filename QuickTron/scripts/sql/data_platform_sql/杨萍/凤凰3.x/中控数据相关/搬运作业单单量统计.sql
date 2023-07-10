##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_detail_stat
(
    `id`                         int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                 datetime  NOT NULL COMMENT '统计时间',
    `date_value`                 date               DEFAULT NULL COMMENT '日期',
    `hour_value`                 varchar(100)       DEFAULT NULL COMMENT '小时',
    `order_type`                 varchar(100)       DEFAULT NULL COMMENT '作业单类型',
    `create_order_num`           varchar(100)       DEFAULT NULL COMMENT '新增单量',
    `done_order_num`             varchar(100)       DEFAULT NULL COMMENT '完成单量',
    `robot_num`                  varchar(100)       DEFAULT NULL COMMENT '作业机器人数量',
    `day_accum_create_order_num` varchar(100)       DEFAULT NULL COMMENT '当日累计单量',
    `day_accum_done_order_num`   varchar(100)       DEFAULT NULL COMMENT '当日累计完成单量',
    `time_type`                  varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单单量统计明细';	
	
	
	
##step2:删除当天相关数据
DELETE
FROM qt_smartreport.qt_transport_order_detail_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  
  
  
  
  
  
  
  
##step3:插入当天相关数据
insert into qt_smartreport.qt_transport_order_detail_stat(time_value, order_type, create_order_num, done_order_num,
                                                          robot_num, day_accum_create_order_num,
                                                          day_accum_done_order_num, time_type)
select `time_value`,
       `order_type`,
       coalesce(sum(create_order_num), 0)           as `create_order_num`,
       coalesce(sum(done_order_num), 0)             as `done_order_num`,
       coalesce(sum(robot_num), 0)                  as `robot_num`,
       coalesce(sum(day_accum_create_order_num), 0) as `day_accum_create_order_num`,
       coalesce(sum(day_accum_done_order_num), 0)   as `day_accum_done_order_num`,
       '天'                                          as `time_type`
from (select date_format(create_time, '%Y-%m-%d') as `time_value`,
             order_type                           as `order_type`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`,
             count(distinct robot_code)           as `robot_num`,
             count(distinct order_id)             as `day_accum_create_order_num`,
             null                                 as `day_accum_done_order_num`
      from phoenix_rms.transport_order
      where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `time_value`,
             order_type                           as `order_type`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`,
             null                                 as `robot_num`,
             null                                 as `day_accum_create_order_num`,
             count(distinct order_id)             as `day_accum_done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
        and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2) t
group by 1, 2

union all

select `time_value`,
       `order_type`,
       coalesce(sum(`create_order_num`), 0)           as `create_order_num`,
       coalesce(sum(`done_order_num`), 0)             as `done_order_num`,
       coalesce(sum(`robot_num`), 0)                  as `robot_num`,
       coalesce(sum(`day_accum_create_order_num`), 0) as `day_accum_create_order_num`,
       coalesce(sum(`day_accum_done_order_num`), 0)   as `day_accum_done_order_num`,
       '小时'                                           as `time_type`
from (select t1.`time_value`,
             t1.`order_type`,
             t1.`create_order_num`,
             null                       as `done_order_num`,
             t1.`robot_num`,
             sum(t2.`create_order_num`) as `day_accum_create_order_num`,
             null                       as `day_accum_done_order_num`
      from (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct robot_code)                    as `robot_num`,
                   count(distinct order_id)                      as `create_order_num`
            from phoenix_rms.transport_order
            where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t1
               left join
           (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `create_order_num`
            from phoenix_rms.transport_order
            where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t2
           on t1.`order_type` = t2.`order_type` and
              date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
      where t2.`time_value` <= t1.`time_value`
      group by 1, 2, 3, 4, 5

      union all

      select t1.`time_value`,
             t1.`order_type`,
             null                     as `create_order_num`,
             t1.`done_order_num`,
             null                     as `robot_num`,
             null                     as `day_accum_create_order_num`,
             sum(t2.`done_order_num`) as `day_accum_done_order_num`
      from (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
              and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t1
               left join
           (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
              and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t2
           on t1.`order_type` = t2.`order_type` and
              date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
      where t2.`time_value` <= t1.`time_value`
      group by 1, 2, 3, 4, 5) t
group by 1, 2
;








###全量逻辑
#搬运作业单单量统计明细
select `time_value`,
       `order_type`,
       coalesce(sum(create_order_num), 0)           as `create_order_num`,
       coalesce(sum(done_order_num), 0)             as `done_order_num`,
       coalesce(sum(robot_num), 0)                  as `robot_num`,
       coalesce(sum(day_accum_create_order_num), 0) as `day_accum_create_order_num`,
       coalesce(sum(day_accum_done_order_num), 0)   as `day_accum_done_order_num`,
       '天'                                          as `time_type`
from (select date_format(create_time, '%Y-%m-%d') as `time_value`,
             order_type                           as `order_type`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`,
             count(distinct robot_code)           as `robot_num`,
             count(distinct order_id)             as `day_accum_create_order_num`,
             null                                 as `day_accum_done_order_num`
      from phoenix_rms.transport_order
--       where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `time_value`,
             order_type                           as `order_type`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`,
             null                                 as `robot_num`,
             null                                 as `day_accum_create_order_num`,
             count(distinct order_id)             as `day_accum_done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
--         and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2) t
group by 1, 2

union all

select `time_value`,
       `order_type`,
       coalesce(sum(`create_order_num`), 0)           as `create_order_num`,
       coalesce(sum(`done_order_num`), 0)             as `done_order_num`,
       coalesce(sum(`robot_num`), 0)                  as `robot_num`,
       coalesce(sum(`day_accum_create_order_num`), 0) as `day_accum_create_order_num`,
       coalesce(sum(`day_accum_done_order_num`), 0)   as `day_accum_done_order_num`,
       '小时'                                           as `time_type`
from (select t1.`time_value`,
             t1.`order_type`,
             t1.`create_order_num`,
             null                       as `done_order_num`,
             t1.`robot_num`,
             sum(t2.`create_order_num`) as `day_accum_create_order_num`,
             null                       as `day_accum_done_order_num`
      from (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct robot_code)                    as `robot_num`,
                   count(distinct order_id)                      as `create_order_num`
            from phoenix_rms.transport_order
--             where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t1
               left join
           (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `create_order_num`
            from phoenix_rms.transport_order
--             where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t2
           on t1.`order_type` = t2.`order_type` and
              date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
      where t2.`time_value` <= t1.`time_value`
      group by 1, 2, 3, 4, 5

      union all

      select t1.`time_value`,
             t1.`order_type`,
             null                     as `create_order_num`,
             t1.`done_order_num`,
             null                     as `robot_num`,
             null                     as `day_accum_create_order_num`,
             sum(t2.`done_order_num`) as `day_accum_done_order_num`
      from (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
--               and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t1
               left join
           (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                   order_type                                    as `order_type`,
                   count(distinct order_id)                      as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
--               and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t2
           on t1.`order_type` = t2.`order_type` and
              date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
      where t2.`time_value` <= t1.`time_value`
      group by 1, 2, 3, 4, 5) t
group by 1, 2
;




##############统计维度补位逻辑##############################################
insert into qt_smartreport.qt_transport_order_detail_stat(time_value, date_value, hour_value, order_type,
                                                          create_order_num, done_order_num,
                                                          robot_num, day_accum_create_order_num,
                                                          day_accum_done_order_num, time_type)

select a.time_value,
       a.date_value,
       null                                      as hour_value,
       a.order_type,
       coalesce(b.create_order_num, 0)           as create_order_num,
       coalesce(b.done_order_num, 0)             as done_order_num,
       coalesce(b.robot_num, 0)                  as robot_num,
       coalesce(b.day_accum_create_order_num, 0) as day_accum_create_order_num,
       coalesce(b.day_accum_done_order_num, 0)   as day_accum_done_order_num,
       coalesce(b.time_type, '天')                as time_type
from (select date_format(ta.date_value, '%Y-%m-%d 00:00:00') as `time_value`,
             ta.date_value,
             tb.order_type
      from (select date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d') as date_value) ta
               left join (select distinct order_type from phoenix_rms.transport_order) tb on 1) a
         left join (select `time_value`,
                           date_value,

                           `order_type`,
                           coalesce(sum(create_order_num), 0)           as `create_order_num`,
                           coalesce(sum(done_order_num), 0)             as `done_order_num`,
                           coalesce(sum(robot_num), 0)                  as `robot_num`,
                           coalesce(sum(day_accum_create_order_num), 0) as `day_accum_create_order_num`,
                           coalesce(sum(day_accum_done_order_num), 0)   as `day_accum_done_order_num`,
                           '天'                                          as `time_type`
                    from (select date_format(create_time, '%Y-%m-%d 00:00:00') as `time_value`,
                                 DATE(create_time)                             as date_value,

                                 order_type                                    as `order_type`,
                                 count(distinct order_id)                      as `create_order_num`,
                                 null                                          as `done_order_num`,
                                 count(distinct robot_code)                    as `robot_num`,
                                 count(distinct order_id)                      as `day_accum_create_order_num`,
                                 null                                          as `day_accum_done_order_num`
                          from phoenix_rms.transport_order
                          where date_format(create_time, '%Y-%m-%d') =
                                date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                          group by 1, 2, 3
                          union all
                          select date_format(update_time, '%Y-%m-%d 00:00:00') as `time_value`,
                                 DATE(update_time)                             as date_value,

                                 order_type                                    as `order_type`,
                                 null                                          as `create_order_num`,
                                 count(distinct order_id)                      as `done_order_num`,
                                 null                                          as `robot_num`,
                                 null                                          as `day_accum_create_order_num`,
                                 count(distinct order_id)                      as `day_accum_done_order_num`
                          from phoenix_rms.transport_order
                          where state = 'DONE'
                            and date_format(update_time, '%Y-%m-%d') =
                                date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                          group by 1, 2, 3) t
                    group by 1, 2, 3) b
                   on a.time_value = b.time_value and a.date_value = b.date_value and a.order_type = b.order_type

union all

select a.time_value,
       a.date_value,
       a.hour_value,
       a.order_type,
       coalesce(b.create_order_num, 0)           as create_order_num,
       coalesce(b.done_order_num, 0)             as done_order_num,
       coalesce(b.robot_num, 0)                  as robot_num,
       coalesce(b.day_accum_create_order_num, 0) as day_accum_create_order_num,
       coalesce(b.day_accum_done_order_num, 0)   as day_accum_done_order_num,
       coalesce(b.time_type, '小时')               as time_type
from (select ta.time_value, ta.date_value, ta.hour_value, tb.order_type
      from (select date_format(day_hours, '%Y-%m-%d %H:00:00') as `time_value`,
                   DATE(day_hours)                             as date_value,
                   HOUR(day_hours)                             as hour_value
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
                  FROM (SELECT a
                        FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                                 JOIN(SELECT '1'
                                      UNION
                                      SELECT '2'
                                      UNION
                                      SELECT '3'
                                      UNION
                                      SELECT '4'
                                      UNION
                                      SELECT '5'
                                      UNION
                                      SELECT '6') AS b ON 1) AS b,
                       (SELECT @u := -1) AS i) th) ta
               left join (select distinct order_type from phoenix_rms.transport_order) tb on 1) a
         left join (select `time_value`,
                           date_value,
                           hour_value,
                           `order_type`,
                           coalesce(sum(`create_order_num`), 0)           as `create_order_num`,
                           coalesce(sum(`done_order_num`), 0)             as `done_order_num`,
                           coalesce(sum(`robot_num`), 0)                  as `robot_num`,
                           coalesce(sum(`day_accum_create_order_num`), 0) as `day_accum_create_order_num`,
                           coalesce(sum(`day_accum_done_order_num`), 0)   as `day_accum_done_order_num`,
                           '小时'                                           as `time_type`
                    from (select t1.`time_value`,
                                 DATE(t1.time_value)        as date_value,
                                 HOUR(t1.time_value)        as hour_value,
                                 t1.`order_type`,
                                 t1.`create_order_num`,
                                 null                       as `done_order_num`,
                                 t1.`robot_num`,
                                 sum(t2.`create_order_num`) as `day_accum_create_order_num`,
                                 null                       as `day_accum_done_order_num`
                          from (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                                       order_type                                    as `order_type`,
                                       count(distinct robot_code)                    as `robot_num`,
                                       count(distinct order_id)                      as `create_order_num`
                                from phoenix_rms.transport_order
                                where date_format(create_time, '%Y-%m-%d') =
                                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                group by 1, 2) t1
                                   left join
                               (select date_format(create_time, '%Y-%m-%d %H:00:00') as `time_value`,
                                       order_type                                    as `order_type`,
                                       count(distinct order_id)                      as `create_order_num`
                                from phoenix_rms.transport_order
                                where date_format(create_time, '%Y-%m-%d') =
                                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                group by 1, 2) t2
                               on t1.`order_type` = t2.`order_type` and
                                  date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
                          where t2.`time_value` <= t1.`time_value`
                          group by 1, 2, 3, 4, 5, 6, 7

                          union all

                          select t1.`time_value`,
                                 DATE(t1.time_value)      as date_value,
                                 HOUR(t1.time_value)      as hour_value,
                                 t1.`order_type`,
                                 null                     as `create_order_num`,
                                 t1.`done_order_num`,
                                 null                     as `robot_num`,
                                 null                     as `day_accum_create_order_num`,
                                 sum(t2.`done_order_num`) as `day_accum_done_order_num`
                          from (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                                       order_type                                    as `order_type`,
                                       count(distinct order_id)                      as `done_order_num`
                                from phoenix_rms.transport_order
                                where state = 'DONE'
                                  and date_format(update_time, '%Y-%m-%d') =
                                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                group by 1, 2) t1
                                   left join
                               (select date_format(update_time, '%Y-%m-%d %H:00:00') as `time_value`,
                                       order_type                                    as `order_type`,
                                       count(distinct order_id)                      as `done_order_num`
                                from phoenix_rms.transport_order
                                where state = 'DONE'
                                  and date_format(update_time, '%Y-%m-%d') =
                                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                group by 1, 2) t2
                               on t1.`order_type` = t2.`order_type` and
                                  date_format(t1.`time_value`, '%Y-%m-%d') = date_format(t2.`time_value`, '%Y-%m-%d')
                          where t2.`time_value` <= t1.`time_value`
                          group by 1, 2, 3, 4, 5, 6, 7) t
                    group by 1, 2, 3, 4
) b on b.time_value = a.time_value and b.date_value = a.date_value and b.hour_value = a.hour_value and
       b.order_type = a.order_type
;		
		   

############################################################################
