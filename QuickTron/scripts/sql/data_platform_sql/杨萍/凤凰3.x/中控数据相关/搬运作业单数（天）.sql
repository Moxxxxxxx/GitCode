##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_day
(
    `id`               int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`       date  NOT NULL COMMENT '日期',
    `create_order_num` varchar(100)       DEFAULT NULL COMMENT '创建作业单数',
    `done_order_num`   varchar(100)       DEFAULT NULL COMMENT '完成作业单数',
    `created_time`     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`     timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单数（天）';	




##step2:删除当天相关数据
DELETE
FROM qt_smartreport.qt_transport_order_day
WHERE date_value = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  
  
  
  
  
  
##step3:插入当天相关数据
insert into qt_smartreport.qt_transport_order_day(date_value, create_order_num, done_order_num)
select `date_value`,
       coalesce(sum(create_order_num), 0) as `create_order_num`,
       coalesce(sum(done_order_num), 0)   as `done_order_num`
from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`
      from phoenix_rms.transport_order
      where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1
      union all
      select date_format(update_time, '%Y-%m-%d') as `date_value`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
        and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1) t
group by 1
;



##############统计维度补位逻辑##############################################
insert into qt_smartreport.qt_transport_order_day(date_value, create_order_num, done_order_num)
select a.date_value,
       coalesce(b.create_order_num, 0) as create_order_num,
       coalesce(b.done_order_num, 0)   as done_order_num
from (select date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d') as date_value) a
         left join
     (select `date_value`,
             coalesce(sum(create_order_num), 0) as `create_order_num`,
             coalesce(sum(done_order_num), 0)   as `done_order_num`
      from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
                   count(distinct order_id)             as `create_order_num`,
                   null                                 as `done_order_num`
            from phoenix_rms.transport_order
            where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1
            union all
            select date_format(update_time, '%Y-%m-%d') as `date_value`,
                   null                                 as `create_order_num`,
                   count(distinct order_id)             as `done_order_num`
            from phoenix_rms.transport_order
            where state = 'DONE'
              and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
            group by 1) t
      group by 1) b on a.date_value = b.date_value
############################################################






###全量逻辑
#搬运作业单数（天）
select `date_value`,
       coalesce(sum(create_order_num), 0) as `create_order_num`,
       coalesce(sum(done_order_num), 0)   as `done_order_num`
from (select date_format(create_time, '%Y-%m-%d') as `date_value`,
             count(distinct order_id)             as `create_order_num`,
             null                                 as `done_order_num`
      from phoenix_rms.transport_order
--       where date_format(create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1
      union all
      select date_format(update_time, '%Y-%m-%d') as `date_value`,
             null                                 as `create_order_num`,
             count(distinct order_id)             as `done_order_num`
      from phoenix_rms.transport_order
      where state = 'DONE'
--         and date_format(update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
      group by 1) t
group by 1
;