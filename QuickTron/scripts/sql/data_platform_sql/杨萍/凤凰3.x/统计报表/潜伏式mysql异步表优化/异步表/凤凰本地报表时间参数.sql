-- {now_start_time}  -- 当天开始时间
-- {now_end_time}    -- 当天结束时间
-- {now_time}        --  当前时间
-- {next_start_time}    --  明天开始时间
-- {now_hour_start_time}      --  当前小时开始时间
-- {now_next_hour_start_time}  -- 下一个小时开始时间
-- {now_week_start_time}  -- 当前一周的开始时间
-- {now_next_week_start_time}  --  下一周的开始时间
-- {pre_hour_start_time}  -- 前一小时开始时间  2022-10-28 14:00:00.000000000 
-- {pre_hour_end_time}  -- 前一小时结束时间    2022-10-28 14:59:59.999999999
-- {pre_day_start_time}   -- 前一天开始时间    2022-10-27 00:00:00.000000000
-- {pre_day_end_time}   -- 前一天结束时间      2022-10-27 23:59:59.999999999
-- {pre_week_start_time}   -- 前一周开始时间   2022-10-17 00:00:00.000000000
-- {pre_week_end_time}   -- 前一周结束时间     2022-10-23 23:59:59.999999999
-- {start_time}  -- 筛选框开始时间  默认当天开始时间
-- {end_time}   --  筛选框结束时间  默认当前小时结束时间


set @now_time=sysdate();   --  当前时间
set @now_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 当天开始时间
set @now_end_time=date_format(sysdate(), '%Y-%m-%d 23:59:59.999999999');   -- 当天结束时间
set @next_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00.000000000'); --  明天开始时间
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @now_week_start_time= date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @now_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
set @pre_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');  -- 前一小时开始时间  
set @pre_hour_end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00'); -- 前一小时结束时间
set @pre_day_start_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');  -- 前一天开始时间
set @pre_day_end_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');  -- 前一天结束时间
set @pre_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +7 DAY), '%Y-%m-%d 00:00:00.000000000'); -- 前一周开始时间
set @pre_week_end_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +1 DAY), '%Y-%m-%d 23:59:59.999999999'); -- 前一周结束时间 
set @start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999');  --  筛选框结束时间  默认当前小时结束时间
select @now_time,@now_start_time,@now_end_time,@next_start_time,@now_hour_start_time,@now_next_hour_start_time,@now_week_start_time,@now_next_week_start_time,@pre_hour_start_time,@pre_hour_end_time,@pre_day_start_time,@pre_day_end_time,@pre_week_start_time,@pre_week_end_time,@start_time,@end_time;
 


-- 定时任务传入时间参与测试
-- 建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_time_test
(
    `id`                   int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `parameter_time`      varchar(255) NOT NULL COMMENT '参数时间',
    `parameter_time_meaning` varchar(255) NOT NULL COMMENT '参数时间意义',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='定时任务传入时间参与测试';
	

-- 删除数据
truncate table qt_smartreport.qt_time_test;


-- 插入数据
INSERT INTO qt_smartreport.qt_time_test(parameter_time,parameter_time_meaning)
VALUES 
({now_start_time},'当天开始时间'),
({now_end_time},'当天结束时间'),
({now_time},'当前时间'),
({next_start_time},'明天开始时间'),
({next_end_time},'明天结束时间'),
({now_hour_start_time},'当前小时开始时间'),
({now_next_hour_start_time},'下一个小时开始时间'),
({now_week_start_time},'当前一周的开始时间'),
({now_next_week_start_time},'下一周的开始时间'),
({pre_hour_start_time},'前一小时开始时间'),
({pre_hour_end_time},'前一小时结束时间'),
({pre_day_start_time},'前一天开始时间'),
({pre_day_end_time},'前一天结束时间'),
({pre_week_start_time},'上一周的开始时间'),
({pre_week_end_time},'上一周的结束时间');


