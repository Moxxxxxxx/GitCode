# step1:建表（qt_day_sys_error_list_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_sys_error_list_his
(
    `id`                   bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date         NOT NULL COMMENT '日期',
    `error_id`             bigint(20)   NOT NULL COMMENT '故障通知ID',
    `error_code`           varchar(255) NOT NULL COMMENT '故障码',
    `start_time`           datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`             datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`         varchar(255)          DEFAULT NULL COMMENT '故障分类',
    `alarm_module`         varchar(255)          DEFAULT NULL COMMENT '告警模块-外设、系统、服务、机器人',
    `alarm_service`        varchar(255)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`           varchar(255)          DEFAULT NULL COMMENT '告警对象类型',
    `alarm_level`          int(11)               DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`         varchar(255)          DEFAULT NULL COMMENT '故障详情',
    `param_value`          varchar(255)          DEFAULT NULL COMMENT '参数值',
    `job_order`            varchar(255)          DEFAULT NULL COMMENT '关联作业单',
    `robot_job`            varchar(255)          DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`           varchar(255)          DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`          varchar(255)          DEFAULT NULL COMMENT '关联设备编码',
    `server_code`          varchar(255)          DEFAULT NULL COMMENT '关联服务器',
    `transport_object`     varchar(255)          DEFAULT NULL COMMENT '关联搬运对象',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_start_time (`start_time`),
    key idx_end_time (`end_time`),
    key idx_alarm_service (`alarm_service`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统天维度故障结果集（T+1）';
			


# step2:删除相关数据（qt_day_sys_error_list_his）
DELETE
FROM qt_smartreport.qt_day_sys_error_list_his
where date_value = date_add(CURRENT_DATE(), interval -1 day);



# step3:插入相关数据（qt_day_sys_error_list_his）
insert into qt_smartreport.qt_day_sys_error_list_his(date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
distinct    -- 一定要记得对之前小时维度的故障集合去重
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object 
from qt_smartreport.qt_hour_sys_error_list_his t 
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value=date_add(current_date(), interval -1 day)


		
# step4:建表（qt_day_sys_error_mtbf_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_sys_error_mtbf_his
(
    `id`                        bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                date       NOT NULL COMMENT '日期',
    `alarm_service`        varchar(255)             NOT NULL COMMENT '告警服务',
    `theory_run_duration`       decimal(65, 30)     DEFAULT NULL COMMENT '在该天内理论运行时长（秒）',
    `error_duration`            decimal(65, 30)     DEFAULT NULL COMMENT '在该天内故障时长（秒）',
    `error_num`                 bigint(10)          DEFAULT NULL COMMENT '在该天内参与计算的故障数',
    `mtbf`                      decimal(65, 30)     DEFAULT NULL COMMENT 'mtbf（秒）',
    `accum_theory_run_duration` decimal(65, 30)     DEFAULT NULL COMMENT '累计理论运行时长（秒）',
    `accum_error_duration`      decimal(65, 30)     DEFAULT NULL COMMENT '累计故障时长（秒）',
    `accum_error_num`           bigint(10)          DEFAULT NULL COMMENT '累计参与计算的故障数',
    `accum_mtbf`                decimal(65, 30)     DEFAULT NULL COMMENT '累计mtbf（秒）',
    `created_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_alarm_service (`alarm_service`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系統天维度mtbf（T+1）';
	
	

# step5:删除相关数据（qt_day_sys_error_mtbf_his）
DELETE
FROM qt_smartreport.qt_day_sys_error_mtbf_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);
	

					
# step6:插入相关数据（qt_day_sys_error_mtbf_his）
insert into qt_smartreport.qt_day_sys_error_mtbf_his(date_value,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)	
select 
date_add(CURRENT_DATE(), interval -1 day) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/t2.error_num else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/t3.accum_error_num else null end as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统前一天理论运行时长、故障时长（时间段上去重）
left join 
(select 
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.qt_hour_sys_error_mtbf_his
where date_value=date_add(current_date(), interval -1 day)
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统前一天参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.qt_day_sys_error_list_his
where date_value=date_add(current_date(), interval -1 day)
group by alarm_service)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.qt_day_sys_error_list_his
where date_value<=date_add(current_date(), interval -1 day)
group by alarm_service)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join  
(select 
alarm_service,accum_theory_run_duration,accum_error_duration  
from qt_smartreport.qt_day_sys_error_mtbf_his
where date_value=date_add(current_date(), interval -2 day))t4 on t4.alarm_service=ts.alarm_service