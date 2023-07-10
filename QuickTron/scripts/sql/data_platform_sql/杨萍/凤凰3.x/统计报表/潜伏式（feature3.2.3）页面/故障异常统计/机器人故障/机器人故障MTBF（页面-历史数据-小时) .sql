#机器人类故障收敛规则：
#1、故障等级>=3（现场需要人工介入的机器人故障）
#2、机器人多条故障均没有结束时间or结束时间相同，取第一条


-- 小时内机器人故障收敛后集合
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < @now_start_time then @now_start_time else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= @next_start_time then @next_start_time else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_start_time and start_time < @next_start_time and
               coalesce(end_time, sysdate()) < @next_start_time) or
              (start_time >= @now_start_time and start_time < @next_start_time and
               coalesce(end_time, sysdate()) >= @next_start_time) or
              (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @now_start_time and
               coalesce(end_time, sysdate()) < @next_start_time) or
              (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @next_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @now_start_time and start_time < @next_start_time and
                              coalesce(end_time, sysdate()) < @next_start_time) or
                             (start_time >= @now_start_time and start_time < @next_start_time and
                              coalesce(end_time, sysdate()) >= @next_start_time) or
                             (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @now_start_time and
                              coalesce(end_time, sysdate()) < @next_start_time) or
                             (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @next_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id


########################################################################################################################

-- 小时内机器人理论运行时长
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;

select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration
from 
(select distinct robot_code from phoenix_basic.basic_robot)br
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration
from 
(select 
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error, 
t2.duration / 1000 as                           duration,
case when sysdate() < @next_start_time then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) - UNIX_TIMESTAMP(@now_start_time) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, @next_start_time)) - UNIX_TIMESTAMP(@now_start_time) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < @now_start_time
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= @now_start_time and create_time < @next_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all 

select 
t4.robot_code,	   
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error, 
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and sysdate() >= @next_start_time then UNIX_TIMESTAMP(@next_start_time)-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and sysdate() < @next_start_time then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= @now_start_time and create_time < @next_start_time)t4 
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= @now_start_time and create_time < @next_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code

	

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################


# step1:建表（qt_hour_robot_error_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_error_detail_his
(
    `id`                   bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date         NOT NULL COMMENT '日期',
    `hour_start_time`      datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time` datetime     NOT NULL COMMENT '下一个小时开始时间',
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
    `stat_start_time`      datetime(6)           DEFAULT NULL COMMENT '小时内参与计算的开始时间',
    `stat_end_time`        datetime(6)           DEFAULT NULL COMMENT '结束时间-小时内参与计算的结束时间',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_start_time (`start_time`),
    key idx_end_time (`end_time`),
    key idx_robot_code (`robot_code`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人小时维度故障收敛结果集（H+1）';
			


# step2:删除相关数据（qt_hour_robot_error_detail_his）
DELETE
FROM qt_smartreport.qt_hour_robot_error_detail_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	




# step3:插入相关数据（qt_hour_robot_error_detail_his）
insert into qt_smartreport.qt_hour_robot_error_detail_his(date_value,hour_start_time,next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object,stat_start_time,stat_end_time)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') then date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= date_format(sysdate(), '%Y-%m-%d %H:00:00') then date_format(sysdate(), '%Y-%m-%d %H:00:00') else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
               coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
              (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
               coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
              (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and
               coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
              (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00'))
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
                              coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
                             (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
                              coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
                             (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and
                              coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
                             (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00'))
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id

									
				
# step4:建表（qt_hour_robot_error_mtbf_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_error_mtbf_his
(
    `id`                        bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                date       NOT NULL COMMENT '日期',
    `hour_start_time`           datetime   NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime   NOT NULL COMMENT '下一个小时开始时间',
    `robot_code`                varchar(255)        DEFAULT NULL COMMENT '机器人编码',
    `theory_run_duration`       decimal(65, 10)     DEFAULT NULL COMMENT '在该小时内理论运行时长（秒）',
    `error_duration`            decimal(65, 10)     DEFAULT NULL COMMENT '在该小时内故障时长（秒）',
    `error_num`                 bigint(10)          DEFAULT NULL COMMENT '在该小时内参与计算的故障数',
    `mtbf`                      decimal(65, 10)     DEFAULT NULL COMMENT 'mtbf（秒）',
    `accum_theory_run_duration` decimal(65, 10)     DEFAULT NULL COMMENT '累计理论运行时长（秒）',
    `accum_error_duration`      decimal(65, 10)     DEFAULT NULL COMMENT '累计故障时长（秒）',
    `accum_error_num`           bigint(10)          DEFAULT NULL COMMENT '累计参与计算的故障数',
    `accum_mtbf`                decimal(65, 10)     DEFAULT NULL COMMENT '累计mtbf（秒）',
    `created_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_robot_code (`robot_code`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人mtbf（H+1）';
	
	

# step5:删除相关数据（qt_hour_robot_error_mtbf_his）
DELETE
FROM qt_smartreport.qt_hour_robot_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	


					
					
# step6:插入相关数据（qt_hour_robot_error_mtbf_his）
insert into qt_smartreport.qt_hour_robot_error_mtbf_his(date_value,hour_start_time,next_hour_start_time,robot_code,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)								
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as accum_mtbf
from(select distinct robot_code from phoenix_basic.basic_robot)br				
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration
from 
(select 
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error,
t2.duration / 1000 as                           duration,
case when sysdate() < date_format(sysdate(), '%Y-%m-%d %H:00:00') then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) - UNIX_TIMESTAMP(date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, date_format(sysdate(), '%Y-%m-%d %H:00:00'))) - UNIX_TIMESTAMP(date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and create_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by robot_code)t3 on t3.robot_code=t1.robot_code
union all 
select 
t4.robot_code,	   
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error,
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and sysdate() >= date_format(sysdate(), '%Y-%m-%d %H:00:00') then UNIX_TIMESTAMP(date_format(sysdate(), '%Y-%m-%d %H:00:00'))-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and sysdate() < date_format(sysdate(), '%Y-%m-%d %H:00:00') then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and create_time < date_format(sysdate(), '%Y-%m-%d %H:00:00'))t4 
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and create_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code
left join 
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,
count(distinct error_id) as error_num
FROM qt_smartreport.qt_hour_robot_error_detail_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by robot_code)t2 on t2.robot_code=br.robot_code		 			
left join 
(select robot_code,count(distinct error_id) as accum_error_num 
FROM qt_smartreport.qt_hour_robot_error_detail_his
where hour_start_time<=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by robot_code)t3 on t3.robot_code=br.robot_code				
left join 
(select robot_code ,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qt_hour_robot_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -2 HOUR), '%Y-%m-%d %H:00:00'))t4 on t4.robot_code=br.robot_code					