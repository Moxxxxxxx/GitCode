-- 故障集合
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_start_time then @now_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@next_start_time then @next_start_time else COALESCE(end_time,sysdate()) end as end_time,
	   UNIX_TIMESTAMP(case when COALESCE(end_time,sysdate())>=@next_start_time then @next_start_time else COALESCE(end_time,sysdate()) end)-UNIX_TIMESTAMP(case when start_time<@now_start_time then @now_start_time else start_time end) as the_hour_keep_suration
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
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
order by alarm_service,original_start_time asc




-- 各模块系统故障时间段重叠时长
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
ts.alarm_service,
3600 as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_start_time then @now_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@next_start_time then @next_start_time else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
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
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service



-- 各模块系统故障时间段重叠时长（当前小时）
set @now_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
ts.alarm_service,
unix_timestamp(date_format(DATE_ADD(sysdate(), INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(sysdate(), '%Y-%m-%d %H:00:00'))  as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_start_time then @now_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@next_start_time then @next_start_time else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
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
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################




#step1:建表（qt_hour_sys_error_duration_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_sys_error_duration_his
(
    `id`                   int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date      NOT NULL COMMENT '日期',
    `hour_start_time`      datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time` datetime  NOT NULL COMMENT '下一个小时开始时间',
    `alarm_service`        varchar(255)       DEFAULT NULL COMMENT '告警服务',
    `sys_run_duration`   decimal(30, 6)     DEFAULT NULL COMMENT '在该小时内运行时长（秒）',	
    `sys_error_duration`   decimal(30, 6)     DEFAULT NULL COMMENT '在该小时内去重故障时长（秒）',
    `sys_error_num`        decimal(30, 6)     DEFAULT NULL COMMENT '在该小时内故障次数',
    `created_time`         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_alarm_service (`alarm_service`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统故障在小时内去重持续时长（H+1）';
	
	
	
	
	
#step2:删除相关数据（qt_hour_sys_error_duration_his）
DELETE
FROM qt_smartreport.qt_hour_sys_error_duration_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	




#step3:插入相关数据(qt_hour_sys_error_duration_his)
insert into qt_smartreport.qt_hour_sys_error_duration_his(date_value,hour_start_time, next_hour_start_time,alarm_service,sys_run_duration, sys_error_duration,sys_error_num)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
ts.alarm_service,
3600 as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') then date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=date_format(sysdate(), '%Y-%m-%d %H:00:00') then date_format(sysdate(), '%Y-%m-%d %H:00:00') else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
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
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
