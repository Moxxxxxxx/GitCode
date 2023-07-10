#166122 【报表】A51297 金堂通威HGT产线搬运项目 定制报表需求
http://ones.flashhold.com:10007/project/#/team/BrU6Tdct/task/W8HkcHG6tZCnJSXT


通威现场定制报表需求：
1. 凤凰3.x的报表默认还是部署服务，但通威这边给前端提个定制需求，把导航上的统计报表菜单隐藏，系统无点击进入的入口。如果需要访问需要输入网址路由访问，这个风险是如果这个网址路由被客户知道他们访问路由也是能记录的。
2. 通威本地 部署本地BI工具，定制报表需求：过滤掉自恢复故障。仅先输出机器人故障统计报表即可。

--------------------------------------------------------------------------------------

预想的机器人故障收敛逻辑：

R1、故障等级>=3
R2、只在相同故障内部应用收敛规则，不同故障不收敛
R3、相同故障码报出之后半小时内如果有相同故障码自恢复数据，则表明对该故障进行了自恢复（自恢复成功不对客户展示但对内部研发展示）
R4、相同故障码有距离在0.1米内且开始时间有与其他的故障结束时间间隔在30秒内的全部故障都认为是一次故障，并且故障开始时间取这批故障中的最早开始时间，结束时间取这批故障中的最晚时间

注：（1）凤凰本地报表是按小时维度计算的，不对历史小时的数据进行回写，且R4计算复杂度大，纯粹的数据表查询无法处理，占用现场计算资源
（2）由于只在不同故障内部收敛，在机器人视角看，多种故障之间的时间重叠还是无法排除

--------------------------------------------------------------------------------------
qt_smartreport.qtr_customize_table_name 


phoenix_basic.basic_robot_type
phoenix_basic.basic_robot
phoenix_basic.basic_notification
phoenix_rms.robot_recovery_record




----------------------------------------------------------------------------------------------
select `method`,count(0)  
from  phoenix_rms.robot_recovery_record
group by 1

一键启动	61
下上线	456
急停恢复	169
清错	407
自恢复	1567
重定位	24




phoenix_basic.basic_robot_type
phoenix_basic.basic_robot
phoenix_basic.basic_notification
phoenix_rms.robot_recovery_record



select *
from  phoenix_rms.robot_recovery_record
where robot_code ='qilin31_46'
and start_time >='2023-01-01 00:00:00'
order by start_time asc 




select * 
from  phoenix_rms.robot_recovery_record
where `method`='自恢复'
order by start_time asc



select robot_code,count(0) num   
from  phoenix_rms.robot_recovery_record
where `method`='自恢复'
group by robot_code
order by num desc 




set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间


---------------


-- 半小时内自恢复成功的故障
set @dt_day_start_time='2023-01-01 00:00:00';
set @dt_next_day_start_time='2023-02-08 00:00:00';


select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot'
and (
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
          )		  
and alarm_level >= 3)bn 
inner join 
-- 机器人自恢复成功的故障list
(select 
robot_code,
`method`,
error_codes,
`result`,
start_time,
end_time
from phoenix_rms.robot_recovery_record 
where `method` ='自恢复'
and (
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
          )	
and `result`=1)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60




----------------------------------------------------------------------------------
-- mysql时间参数
set @now_time = sysdate(); --  当前时间
set @dt_day_start_time = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'); -- 开始时间
set @dt_next_day_start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00');
--  下一个开始时间

-- set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 开始时间
-- set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  下一个开始时间

-- set @dt_day_start_time = '2023-01-01 00:00:00'; -- 开始时间
-- set @dt_next_day_start_time = '2023-02-08 00:00:00'; --  下一个开始时间

select @now_time, @dt_day_start_time, @dt_next_day_start_time;


select @now_time                                                                            as create_time,
       @now_time                                                                            as update_time,
       date(@dt_day_start_time)                                                             as date_value,
       t1.id                                                                                as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       bei.alarm_name,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
       GREATEST(t1.start_time, @dt_day_start_time)                                          as stat_start_time,
       case
           when t1.end_time is null or t1.end_time >= LEAST(@dt_next_day_start_time, @now_time)
               then LEAST(@dt_next_day_start_time, @now_time)
           else t1.end_time end                                                             as stat_end_time,
       t1.point_location,
       case
           when t1.point_location like '%pointCode=%' then substring_index(
                   substring_index(t1.point_location, 'pointCode=', -1), ')', 1) end        as point_code,
       substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x_location,
       substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y_location,
       case when t3.error_id is not null then 1 else 0 end                                     is_self_recovery -- 是否自恢复成功	   
-- step1:计算时间段内故障
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
               coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time and
               coalesce(end_time, @now_time) < @dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_next_day_start_time)
          )) t1
         -- 注意：一定是用的inner join ,保留同一台机器人相同end_time的第一条
         -- step2:相同end_time留下第一条
         inner join (select robot_code,
                            COALESCE(end_time, 'unfinished') as end_time,
                            min(id)                          as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                              coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                             (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
                             (start_time < @dt_day_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_day_start_time and
                              coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                             (start_time < @dt_day_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_day_start_time)
                         )
                     group by robot_code, COALESCE(end_time, 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
-- step3:半小时内自恢复成功的故障			
         left join (select distinct bn.id as error_id, bn.robot_code, bn.error_code, bn.start_time
                    from (select id, robot_code, error_code, start_time
                          from phoenix_basic.basic_notification
                          where alarm_module = 'robot'
                            and (
                                  (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                   coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                  (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                   coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
                                  (start_time < @dt_day_start_time and
                                   coalesce(end_time, @now_time) >= @dt_next_day_start_time and
                                   coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                  (start_time < @dt_day_start_time and
                                   coalesce(end_time, @now_time) >= @dt_next_day_start_time)
                              )
                            and alarm_level >= 3) bn
                             inner join
                         -- 机器人自恢复成功的故障list
                             (select robot_code,
                                     `method`,
                                     error_codes,
                                     `result`,
                                     start_time,
                                     end_time
                              from phoenix_rms.robot_recovery_record
                              where `method` = '自恢复'
                                and (
                                      (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                       coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                      (start_time >= @dt_day_start_time and start_time < @dt_next_day_start_time and
                                       coalesce(end_time, @now_time) >= @dt_next_day_start_time) or
                                      (start_time < @dt_day_start_time and
                                       coalesce(end_time, @now_time) >= @dt_next_day_start_time and
                                       coalesce(end_time, @now_time) < @dt_next_day_start_time) or
                                      (start_time < @dt_day_start_time and
                                       coalesce(end_time, @now_time) >= @dt_next_day_start_time)
                                  )
                                and `result` = 1) tsh
                         on tsh.robot_code = bn.robot_code and tsh.error_codes = bn.error_code and
                            tsh.start_time >= bn.start_time and
                            UNIX_TIMESTAMP(tsh.start_time) - UNIX_TIMESTAMP(bn.start_time) <= 30 * 60) t3
                   on t3.error_id = t1.id
         left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code