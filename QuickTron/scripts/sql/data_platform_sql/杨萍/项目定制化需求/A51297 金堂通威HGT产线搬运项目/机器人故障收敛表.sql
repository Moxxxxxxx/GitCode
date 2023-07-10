-- step1:建表（qtr_customize_day_robot_error_list_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qtc_day_robot_error_list_his
(
    `create_time`       timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`       timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `id`                bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`        DATE       NOT NULL COMMENT '日期',
    `error_id`          bigint(20) NOT NULL COMMENT '故障通知ID',
    `error_code`        varchar(255)        DEFAULT NULL COMMENT '错误码',
    `start_time`        datetime(6)         DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`          datetime(6)         DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`     varchar(255)        DEFAULT NULL COMMENT '告警分类',
    `alarm_module`     varchar(255)        DEFAULT NULL COMMENT '告警模块',
    `alarm_service`    varchar(255)        DEFAULT NULL COMMENT '告警服务',
    `alarm_type`       varchar(255)        DEFAULT NULL COMMENT '告警分类',
    `alarm_level`      int(11)             DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`     varchar(255)        DEFAULT NULL COMMENT '告警详情',
    `alarm_name`       varchar(255)        DEFAULT NULL COMMENT '告警名称',
    `param_value`      varchar(2000)       DEFAULT NULL COMMENT '参数值',
    `job_order`        varchar(255)        DEFAULT NULL COMMENT '关联作业单',
    `robot_job`        varchar(255)        DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`       varchar(255)        DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`      varchar(255)        DEFAULT NULL COMMENT '关联设备编号',
    `server_code`      varchar(255)        DEFAULT NULL COMMENT '关联服务编号',
    `transport_object` varchar(255)        DEFAULT NULL COMMENT '关联搬运对象',
    `stat_start_time`  datetime(6)         DEFAULT NULL COMMENT '周期维度内参与计算的开始时间',
    `stat_end_time`    datetime(6)         DEFAULT NULL COMMENT '周期维度内参与计算的结束时间',
    `point_location`   varchar(255)        DEFAULT NULL COMMENT '关联地图码点位置',
    `point_code`       varchar(255)        DEFAULT NULL COMMENT '关联地图码点编码',
    `x_location`       varchar(255)        DEFAULT NULL COMMENT '关联地图x坐标',
    `y_location`       varchar(255)        DEFAULT NULL COMMENT '关联地图y坐标',
	`is_self_recovery`     varchar(255)        DEFAULT NULL COMMENT '是否自恢复成功',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_date_value (`error_id`),
    key idx_date_value (`error_code`),
    key idx_date_value (`robot_code`),
    key idx_date_value (`start_time`),
    key idx_date_value (`end_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障定制收敛结果集（T）';	
	
	
	
-- step2:删除相关数据（qtr_customize_day_robot_error_list_his）
DELETE
FROM qt_smartreport.qtr_customize_day_robot_error_list_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);
	
	
	
-- step3:插入相关数据（qt_day_robot_error_detail_his）
insert into qt_smartreport.qtr_customize_day_robot_error_list_his(create_time,update_time,id,date_value,error_id,error_code,start_time,end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,alarm_name,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object,stat_start_time,stat_end_time,point_location,point_code,x_location,y_location)



