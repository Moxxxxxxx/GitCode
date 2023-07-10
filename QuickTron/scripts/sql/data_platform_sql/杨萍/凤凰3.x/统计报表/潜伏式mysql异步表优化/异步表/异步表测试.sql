-- 设置的时间戳测试
-- 建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_asynchronous_time_test
(
    `id`                   int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `dt_flag_time`      datetime(6) NOT NULL COMMENT 'dt',	
    `parameter_time`      datetime(6) NOT NULL COMMENT '参数时间',
    `parameter_time_meaning` varchar(255) NOT NULL COMMENT '参数时间意义',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='设置的时间戳测试';
	



-- 插入数据
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00'") %}  -- dt所在周的下一周的开始时间


INSERT INTO qt_smartreport.qt_asynchronous_time_test(dt_flag_time,parameter_time,parameter_time_meaning)
VALUES 
({{ dt_relative_time(dt) }},{{ now_time }},'客观当前时间'),
({{ dt_relative_time(dt) }},{{ dt_hour_start_time }},'dt所在小时的开始时间'),
({{ dt_relative_time(dt) }},{{ dt_next_hour_start_time }},'dt所在小时的下一个小时的开始时间'),
({{ dt_relative_time(dt) }},{{ dt_day_start_time }},'dt所在天的开始时间'),
({{ dt_relative_time(dt) }},{{ dt_next_day_start_time }},'dt所在天的下一天的开始时间'),
({{ dt_relative_time(dt) }},{{ dt_week_start_time }},'dt所在周的开始时间'),
({{ dt_relative_time(dt) }},{{ dt_next_week_start_time }},'dt所在周的下一周的开始时间');




------ 对应的mysql时间参数 --------------------------------------------------

set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;



