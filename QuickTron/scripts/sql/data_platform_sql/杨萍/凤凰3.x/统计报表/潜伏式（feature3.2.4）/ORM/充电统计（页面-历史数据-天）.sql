-- 机器人充电统计
set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');


select 
t.hour_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= @now_start_time and  coalesce(enter_charging_time,bind_robot_time) < @next_start_time
group by hour_value,charging_robot
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and recover_charger_time >= @now_start_time and recover_charger_time < @next_start_time
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by t.hour_value,t.robot_code,brt.robot_type_code,brt.robot_type_name



############################################################################

-- 充电桩充电统计
-- 充电桩充电统计
set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');


select 
t.hour_value,
t.charger_code,
bc.charger_port_type,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= @now_start_time and  coalesce(enter_charging_time,bind_robot_time) < @next_start_time
group by hour_value,charger_code
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and recover_charger_time >= @now_start_time and recover_charger_time < @next_start_time
group by hour_value,charger_code)t
left join phoenix_basic.basic_charger bc on bc.charger_code=t.charger_code
group by t.hour_value,t.charger_code,bc.charger_port_type

				   
########################################################################################################################
########################################################################################################################
########################################################################################################################



#step1:建表（qt_hour_robot_charge_stat_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_charge_stat_his
(
    `id`                   bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date       NOT NULL COMMENT '日期',
    `robot_code`           varchar(255)        DEFAULT NULL COMMENT '机器人编码',
    `robot_type_code`      varchar(255)        DEFAULT NULL COMMENT '机器人类型编码',
    `robot_type_name`      varchar(255)        DEFAULT NULL COMMENT '机器人类型',
    `hour_value`           datetime            DEFAULT NULL COMMENT '小时',
    `create_charge_num`    bigint(20)          DEFAULT NULL COMMENT '新增充电次数',
    `charge_num`           bigint(20)          DEFAULT NULL COMMENT '完成充电次数',
    `charge_duration`      decimal(65, 10)     DEFAULT NULL COMMENT '充电时长（秒）',
    `charge_power_num`     decimal(65, 10)     DEFAULT NULL COMMENT '充电电量',
    `avg_charge_power_num` decimal(65, 10)     DEFAULT NULL COMMENT '平均充电电量',
    `created_time`         timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_robot_type_code (robot_type_code),
    key idx_robot_type_name (robot_type_name),
    key idx_hour_value (`hour_value`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人小时维度充电统计（T+1）';
	
	




#step2:删除相关数据（qt_hour_robot_charge_stat_his）
DELETE
FROM qt_smartreport.qt_hour_robot_charge_stat_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);
	
	
	
#step3:插入相关数据（qt_hour_robot_charge_stat_his）
insert into qt_smartreport.qt_hour_robot_charge_stat_his(date_value,robot_code,robot_type_code,robot_type_name,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)	
select 
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
t.hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as avg_charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and  coalesce(enter_charging_time,bind_robot_time) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charging_robot
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where  enter_charging_power is not null
and recover_charger_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and recover_charger_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by date_value,t.robot_code,brt.robot_type_code,brt.robot_type_name,t.hour_value





#step4:建表（qt_hour_charger_charge_stat_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_charger_charge_stat_his
(
    `id`                   bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date       NOT NULL COMMENT '日期',
    `charger_code`         varchar(255)        DEFAULT NULL COMMENT '充电桩编码',
    `charger_port_type`    varchar(255)        DEFAULT NULL COMMENT '充电桩型号',
    `hour_value`           datetime            DEFAULT NULL COMMENT '小时',
    `create_charge_num`    bigint(20)          DEFAULT NULL COMMENT '新增充电次数',	
    `charge_num`           bigint(20)          DEFAULT NULL COMMENT '充电次数',
    `charge_duration`      decimal(65, 10)     DEFAULT NULL COMMENT '充电时长（秒）',
    `charge_power_num`     decimal(65, 10)     DEFAULT NULL COMMENT '充电电量',
    `avg_charge_power_num` decimal(65, 10)     DEFAULT NULL COMMENT '平均充电电量',
    `created_time`         timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_charger_code (`charger_code`),
    key idx_charger_port_type (charger_port_type),
    key idx_hour_value (`hour_value`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='充电桩小时维度充电统计（T+1）';
	


#step5:删除相关数据（qt_hour_charger_charge_stat_his）
DELETE
FROM qt_smartreport.qt_hour_charger_charge_stat_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);
	
	
	
#step6:插入相关数据（qt_hour_charger_charge_stat_his）
insert into qt_smartreport.qt_hour_charger_charge_stat_his(date_value,charger_code,charger_port_type,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)	
select 
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t.charger_code,
bc.charger_port_type,
t.hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as avg_charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and  coalesce(enter_charging_time,bind_robot_time) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charger_code
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and recover_charger_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and recover_charger_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charger_code)t
left join phoenix_basic.basic_charger bc on bc.charger_code=t.charger_code
group by date_value,t.charger_code,bc.charger_port_type,t.hour_value