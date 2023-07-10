##step1:建表（临时表 qt_robot_charge_detail_temp_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_charge_detail_temp_realtime
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `charge_job_sn`             varchar(100)       DEFAULT NULL COMMENT '任务ID',
    `charger_code`               varchar(100)       DEFAULT NULL COMMENT '充电桩code',
    `stat_time`          datetime           DEFAULT NULL COMMENT '充电用于统计时间',
    `add_power`                 int(100)           DEFAULT NULL COMMENT '增加电量',
    `charge_time`               int(100)           DEFAULT NULL COMMENT '充电花费时间（秒）',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人充电明细临时表（当天数据）';	



--------------------------------
##step2:删除当天相关数据（临时表 qt_robot_charge_detail_temp_realtime）
DELETE
FROM qt_smartreport.qt_robot_charge_detail_temp_realtime;  




--------------------------------
##step3:插入当天相关数据(临时表 qt_robot_charge_detail_temp_realtime)
insert into qt_smartreport.qt_robot_charge_detail_temp_realtime(robot_code, first_classification_name,
                                                                       charge_job_sn, charger_code, stat_time,
                                                                       add_power, charge_time)
select robot_code,
       first_classification_name,
       charge_job_sn,
       charger_code,
       next_time as stat_time,
       sum(add_power)    as add_power,
       sum(cost_seconds) as charge_time
from (select t.robot_code,
             case
                 when brt.first_classification = 'WORKBIN' then '料箱车'
                 when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
                 when brt.first_classification = 'CARRIER' then '潜伏式'
                 when brt.first_classification = 'ROLLER' then '辊筒'
                 when brt.first_classification = 'FORKLIFT' then '堆高全向车'
                 when brt.first_classification = 'DELIVER' then '投递车'
                 when brt.first_classification = 'SC' then '四向穿梭车'
                 else brt.first_classification end               as first_classification_name,
             t.job_sn                                            as charge_job_sn,
             t3.charger_code,
             t.first_start_time,
			 t1.create_time as pre_time,
			 t1.id as pre_id,
			 t1.next_time,
			 t1.next_id,
			 t1.power as pre_power,
             t2.power                                            as next_power,
             t2.power - t1.power                                 as add_power,
             timestampdiff(second, t1.create_time, t1.next_time) as cost_seconds
      from (select robot_code,
                   job_sn,
                   min(create_time) as first_start_time
            from phoenix_rms.robot_state_history
            where work_state = 'CHARGING'
			and job_sn like 'CHARGE_%'
              and date_format(create_time, '%Y-%m-%d') >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d')
            group by robot_code, job_sn) t 
               inner join
           (select a.*,
                   min(b.id)          as next_id,
                   min(b.create_time) as next_time
            from (select id,
                         robot_code,
                         create_time,
                         network_state,
                         online_state,
                         power,
                         cause,
                         job_sn
                  from phoenix_rms.robot_state_history
                  where work_state = 'CHARGING'
				  and job_sn like 'CHARGE_%'
                    and date_format(create_time, '%Y-%m-%d') >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d')
                 ) a
                     left join
                 (select *
                  from phoenix_rms.robot_state_history
                  where 1=1
                    and date_format(create_time, '%Y-%m-%d') >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d')) b
                 on b.robot_code = a.robot_code and b.create_time > a.create_time
            group by 1, 2, 3, 4, 5, 6, 7, 8) t1 on t1.robot_code = t.robot_code and t1.job_sn = t.job_sn  
               left join
           (select *
            from phoenix_rms.robot_state_history
            where power is not null
              and date_format(create_time, '%Y-%m-%d') >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d')) t2
           on t2.robot_code = t1.robot_code and t2.create_time = t1.next_time 
               left join
           (select robot_code,
                   job_sn,
                   create_time,
                   TRIM(BOTH '"' from json_extract(request_json, '$.CHARGE.equipmentCode')) as charger_code
            from phoenix_rms.job_history
            where job_type = 'CHARGE'
              and date_format(create_time, '%Y-%m-%d') >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d')
           ) t3 on t3.robot_code = t.robot_code and t3.job_sn = t.job_sn   
               left join phoenix_basic.basic_robot br on br.robot_code = t.robot_code
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
     ) tt
	 where date_format(tt.next_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
group by robot_code, first_classification_name, charge_job_sn, charger_code, next_time;



--------------------------------
##step4:建表(qt_robot_charge_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_charge_stat_realtime
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `charge_num`             int(100)           DEFAULT NULL COMMENT '充电次数',
    `charge_time`               int(100)           DEFAULT NULL COMMENT '充电时长（秒）',
    `add_power`                 int(100)           DEFAULT NULL COMMENT '充电电量',
    `avg_add_power`             decimal(56, 2)     DEFAULT NULL COMMENT '平均充电电量',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人充电统计表（当天数据）';	
	

--------------------------------
##step5:删除当天相关数据(qt_robot_charge_stat_realtime)
DELETE
FROM qt_smartreport.qt_robot_charge_stat_realtime;  




##step6:插入当天相关数据(qt_robot_charge_stat_realtime)
insert into qt_smartreport.qt_robot_charge_stat_realtime(time_value, date_value, hour_value, robot_code,
                                                first_classification_name, charge_num, charge_time, add_power,
                                                avg_add_power, time_type)
select date_format(stat_time, '%Y-%m-%d %H:00:00')                               as time_value,
       DATE(stat_time)                                                           as date_value,
       HOUR(stat_time)                                                           as hour_value,
       robot_code,
       first_classification_name,
       count(distinct charge_job_sn)                                                    as charge_num,
       sum(charge_time)                                                                 as charge_time,
       sum(add_power)                                                                   as add_power,
       cast(round(sum(add_power) / count(distinct charge_job_sn), 2) as decimal(56, 2)) as avg_add_power,
       '小时'                                                                             as time_type
from qt_smartreport.qt_robot_charge_detail_temp_realtime
where date_format(stat_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
group by 1, 2, 3, 4, 5
union all
select date_format(stat_time, '%Y-%m-%d 00:00:00')                               as time_value,
       DATE(stat_time)                                                           as date_value,
       null                                                                             as hour_value,
       robot_code,
       first_classification_name,
       count(distinct charge_job_sn)                                                    as charge_num,
       sum(charge_time)                                                                 as charge_time,
       sum(add_power)                                                                   as add_power,
       cast(round(sum(add_power) / count(distinct charge_job_sn), 2) as decimal(56, 2)) as avg_add_power,
       '天'                                                                              as time_type
from qt_smartreport.qt_robot_charge_detail_temp_realtime
where date_format(stat_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
group by 1, 2, 3, 4, 5
;





##step7:建表(qt_charger_charge_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_charger_charge_stat_realtime
(
    `id`            int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`    datetime  NOT NULL COMMENT '统计时间',
    `date_value`    date               DEFAULT NULL COMMENT '日期',
    `hour_value`    varchar(100)       DEFAULT NULL COMMENT '小时',
    `charger_code`   varchar(100)       DEFAULT NULL COMMENT '充电桩code',
    `charge_num` int(100)           DEFAULT NULL COMMENT '充电次数',
    `charge_time`   int(100)           DEFAULT NULL COMMENT '充电时长（秒）',
    `add_power`     int(100)           DEFAULT NULL COMMENT '充电电量',
    `avg_add_power` decimal(56, 2)     DEFAULT NULL COMMENT '平均充电电量',
    `time_type`     varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='充电桩充电统计表（当天数据）';	
	
	
	

--------------------------------
##step8:删除当天相关数据(qt_charger_charge_stat_realtime)
DELETE
FROM qt_smartreport.qt_charger_charge_stat_realtime;  




##step9:插入当天相关数据(qt_charger_charge_stat_realtime)
insert into qt_smartreport.qt_charger_charge_stat_realtime(time_value, date_value, hour_value, charger_code, charge_num,
                                                  charge_time, add_power, avg_add_power, time_type)
select date_format(stat_time, '%Y-%m-%d %H:00:00')                               as time_value,
       DATE(stat_time)                                                           as date_value,
       HOUR(stat_time)                                                           as hour_value,
       charger_code,
       count(distinct charge_job_sn)                                                    as charge_num,
       sum(charge_time)                                                                 as charge_time,
       sum(add_power)                                                                   as add_power,
       cast(round(sum(add_power) / count(distinct charge_job_sn), 2) as decimal(56, 2)) as avg_add_power,
       '小时'                                                                             as time_type
from qt_smartreport.qt_robot_charge_detail_temp_realtime
where date_format(stat_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
group by 1, 2, 3, 4
union all
select date_format(stat_time, '%Y-%m-%d 00:00:00')                               as time_value,
       DATE(stat_time)                                                           as date_value,
       null                                                                             as hour_value,
       charger_code,
       count(distinct charge_job_sn)                                                    as charge_num,
       sum(charge_time)                                                                 as charge_time,
       sum(add_power)                                                                   as add_power,
       cast(round(sum(add_power) / count(distinct charge_job_sn), 2) as decimal(56, 2)) as avg_add_power,
       '天'                                                                              as time_type
from qt_smartreport.qt_robot_charge_detail_temp_realtime
where date_format(stat_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
group by 1, 2, 3, 4
;