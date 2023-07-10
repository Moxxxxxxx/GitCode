##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_hour
(
    `id`               int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`       date     NOT NULL COMMENT '日期',
    `hour_value`       varchar(100) NOT NULL COMMENT '小时',
    `create_order_num` varchar(100)          DEFAULT NULL COMMENT '创建作业单数',
    `done_order_num`   varchar(100)          DEFAULT NULL COMMENT '完成作业单数',
    `created_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单数（小时）';	





##step2:删除当天相关数据
DELETE
FROM qt_smartreport.qt_transport_order_hour
WHERE date_value = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  



  
##step3:插入当天相关数据
insert into qt_smartreport.qt_transport_order_hour(date_value, hour_value, create_order_num, done_order_num)
select `date_value`,
       `hour_value`,
       COALESCE(sum(create_order_num), 0) as `create_order_num`,
       COALESCE(sum(done_order_num), 0)   as `done_order_num`
from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
             HOUR(create_time)                    as `hour_value`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`
      from phoenix_rms.transport_order
      where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `date_value`,
             HOUR(update_time)                    as `hour_value`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
        and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2) t
group by 1, 2
;





##############统计维度补位逻辑##############################################
insert into qt_smartreport.qt_transport_order_hour(date_value, hour_value, create_order_num, done_order_num)
select a.date_value,
       a.hour_value,
       coalesce(b.create_order_num, 0) as create_order_num,
       coalesce(b.done_order_num, 0)   as done_order_num
from (select DATE(day_hours) as date_value,
             HOUR(day_hours) as hour_value
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'), INTERVAL
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
                 (SELECT @u := -1) AS i) th) a
         left join
     (select `date_value`,
             `hour_value`,
             COALESCE(sum(create_order_num), 0) as `create_order_num`,
             COALESCE(sum(done_order_num), 0)   as `done_order_num`
      from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
                   HOUR(create_time)                    as `hour_value`,
                   count(distinct order_id)             as `create_order_num`,
                   null                                 as `done_order_num`
            from phoenix_rms.transport_order
            where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2
            union all
            select date_format(update_time, '%Y-%m-%d') as `date_value`,
                   HOUR(update_time)                    as `hour_value`,
                   null                                 as `create_order_num`,
                   count(distinct order_id)             as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
              and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1, 2) t
      group by 1, 2) b on a.date_value = b.date_value and a.hour_value = b.hour_value
;
#################################################################################







###全量逻辑
#搬运作业单数（小时）
select `date_value`,
       `hour_value`,
       COALESCE(sum(create_order_num), 0) as `create_order_num`,
       COALESCE(sum(done_order_num), 0)   as `done_order_num`
from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
             HOUR(create_time)                    as `hour_value`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`
      from phoenix_rms.transport_order
--       where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `date_value`,
             HOUR(update_time)                    as `hour_value`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
--         and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1, 2) t
group by 1, 2
;