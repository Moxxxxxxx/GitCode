-- 快仓全部项目list

select * from dwd.dwd_pms_share_project_base_info_df 
where d='2023-02-05'  -- 快仓全部项目
and project_name REGEXP 'PSA|一汽大众'

-- FH-B2022-B111 ：一汽大众佛山线边搬运项目
-- A51488 ：PSA  Sochaux Stellantis overall project

----------------------------------------------------------------------------
20230131会议
预想的机器人故障收敛逻辑：

R1、故障等级>=3
R2、只在相同故障内部应用收敛规则，不同故障不收敛
R3、相同故障码报出之后半小时内如果有相同故障码自恢复数据，则表明对该故障进行了自恢复（自恢复成功不对客户展示但对内部研发展示）
R4、相同故障码有距离在0.1米内且开始时间有与其他的故障结束时间间隔在30秒内的全部故障都认为是一次故障，并且故障开始时间取这批故障中的最早开始时间，结束时间取这批故障中的最晚时间

注：（1）凤凰本地报表是按小时维度计算的，不对历史小时的数据进行回写，且R4计算复杂度大，纯粹的数据表查询无法处理，占用现场计算资源
（2）由于只在不同故障内部收敛，在机器人视角看，多种故障之间的时间重叠还是无法排除
----------------------------------------------------------------------------------
select * from phoenix_basic.basic_error_info

select * 
from phoenix_basic.basic_notification bn 
where bn.alarm_module = 'robot'
and bn.alarm_level >= 3



-- 故障流水明细
select 
bn.id as error_id
,CONCAT(bn.robot_code,'~',bn.error_code) as robot_error_str
,bn.robot_code
,bn.error_code
,bn.start_time 
,bn.end_time
,date_format(bn.start_time, '%Y-%m-%d') as start_date
,date_format(bn.end_time, '%Y-%m-%d') as end_date
,unix_timestamp(bn.end_time)-unix_timestamp(bn.start_time) as error_duration 
,bei.alarm_name
,bei.solution
,bei.alarm_detail
,case when bn.point_location like '%pointCode=%' then substring_index(substring_index(bn.point_location,'pointCode=',-1),')',1) end as point_code
,substring_index(substring_index(bn.point_location, "x=", -1), ",", 1)                   as x
,substring_index(substring_index(replace(bn.point_location, ")", ""), "y=", -1), ",", 1) as y
,bn.alarm_level
,bn.alarm_service
,bn.warning_spec
,bn.alarm_module
,bn.alarm_type
,bn.alarm_detail as alarm_detail_bn
,bn.param_value
,bn.job_order
,bn.robot_job
,bn.device_code
,bn.server_code
,bn.transport_object
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code=bn.error_code
where bn.alarm_module = 'robot'
and bn.alarm_level >= 3
-- and bn.start_time>='2023-02-01 00:00:00'
order by bn.robot_code,robot_error_str,bn.start_time asc





-- 2023-02之后上报故障
select 
date_format(bn.start_time, '%Y-%m-%d') as start_date
,bei.alarm_name
,count(distinct bn.id) as error_count
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code=bn.error_code
where bn.alarm_module = 'robot'
and bn.alarm_level >= 3
and bn.start_time>='2023-02-01 00:00:00'
group by 1,2











-- 当天新抛出故障
select 
bn.id as error_id
,CONCAT(bn.robot_code,'~',bn.error_code) as robot_error_str
,bn.robot_code
,bn.error_code
,bn.start_time 
,bn.end_time
,date_format(bn.start_time, '%Y-%m-%d') as start_date
,date_format(bn.end_time, '%Y-%m-%d') as end_date
,unix_timestamp(bn.end_time)-unix_timestamp(bn.start_time) as error_duration 
,bei.alarm_name
,bei.solution
,bei.alarm_detail
,case when bn.point_location like '%pointCode=%' then substring_index(substring_index(bn.point_location,'pointCode=',-1),')',1) end as point_code
,substring_index(substring_index(bn.point_location, "x=", -1), ",", 1)                   as x
,substring_index(substring_index(replace(bn.point_location, ")", ""), "y=", -1), ",", 1) as y
,bn.alarm_level
,bn.alarm_service
,bn.warning_spec
,bn.alarm_module
,bn.alarm_type
,bn.alarm_detail as alarm_detail_bn
,bn.param_value
,bn.job_order
,bn.robot_job
,bn.device_code
,bn.server_code
,bn.transport_object
,case when t.error_code is not null then 0 else 1 end is_new 
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code=bn.error_code
-- 判断之前故障是否抛出过
left join 
(select distinct  bn.error_code
from phoenix_basic.basic_notification bn 
where bn.alarm_module = 'robot'
and bn.alarm_level >= 3
and bn.start_time>='2023-02-01 00:00:00'
and date_format(bn.start_time, '%Y-%m-%d')<CURRENT_DATE
)t on t.error_code=bn.error_code 
where bn.alarm_module = 'robot'
and bn.alarm_level >= 3
and bn.start_time>=date_format(CURRENT_DATE, '%Y-%m-%d 00:00:00')
order by bn.robot_code,robot_error_str,bn.start_time asc

