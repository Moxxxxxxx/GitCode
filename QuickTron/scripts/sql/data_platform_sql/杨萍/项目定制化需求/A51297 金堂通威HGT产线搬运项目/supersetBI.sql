-- 半小时内自恢复成功的故障
set @from_dttm ='2023-02-01 00:00:00';
set @to_dttm ='2023-02-08 00:00:00';
select @from_dttm,@to_dttm;


select 
t1.robot_code,    -- 机器人编号
t1.id as error_id,    -- 故障通知ID
t1.error_code,   -- 错误码
bei.alarm_name,  -- 故障名称
t1.start_time,   -- 故障开始时间
t1.end_time,     -- 故障结束时间
t1.warning_spec,  -- 故障分类
t1.alarm_module,  -- 故障告警模块 
t1.alarm_service,  -- 故障告警服务
t1.alarm_type,   -- 故障告警分类
t1.alarm_level,  -- 故障告警级别
t1.alarm_detail,  -- 故障告警详情
t1.param_value,   -- 故障告警参数值
t1.job_order,   -- 关联作业单
t1.robot_job,   -- 关联机器人任务  
t1.device_code,  -- 关联设备编号
t1.server_code,  -- 关联服务编号
t1.transport_object,  -- 关联搬运对象
t1.point_location,  -- 关联地图码点位置
case when t1.point_location like '%pointCode=%' then substring_index(substring_index(t1.point_location, 'pointCode=', -1), ')', 1) end        as point_code,  -- 关联地图码点编码
substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x_location,  -- 关联地图x坐标
substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y_location,  -- 关联地图y坐标
case when t3.error_id is not null then '是' else '否' end  as is_self_recovery -- 是否自恢复成功		   
from 
(select *
from phoenix_basic.basic_notification
where alarm_module = 'robot' and alarm_level >= 3
and start_time >= @from_dttm
and start_time<@to_dttm
)t1 
-- 注意：一定是用的inner join ,保留同一台机器人相同end_time的第一条
-- part2:相同end_time留下第一条
inner join 
(select
robot_code,
COALESCE(end_time, 'unfinished') as end_time,
min(id)                          as first_error_id
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
and start_time >= @from_dttm
and start_time<@to_dttm
group by robot_code, COALESCE(end_time, 'unfinished')
)t2 on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
-- part3:半小时内自恢复成功的故障	
left join 
(select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
and start_time >= @from_dttm 
and start_time<@to_dttm
)bn 
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
where `method` ='自恢复' and `result`=1
and start_time >= @from_dttm 
and start_time<=from_unixtime(UNIX_TIMESTAMP(@to_dttm)+30*60)
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t3 on t3.error_id = t1.id
left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code


---------------------------------------------------------------------------------------
-- 生成虚拟数据集-替换jinja参数

/*
{% if not from_dttm %}
{% set from_dttm ='2023-02-01 00:00:00' %}  -- 客观当前时间
{% endif %}
{% if not to_dttm %}
{% set to_dttm ='2023-02-08 00:00:00' %}  -- 客观当前时间
{% endif %}
select "{{ from_dttm }}","{{ to_dttm }}";
*/


{% if not from_dttm or not to_dttm %}
{% set from_dttm =datetime.date.today() + datetime.timedelta(days=-1) %}  -- 开始时间
{% set to_dttm = datetime.datetime.now() %}  -- 结束时间
{% endif %}
-- select "{{ from_dttm }}","{{ to_dttm }}";


select 
t1.robot_code,    -- 机器人编号
t1.id as error_id,    -- 故障通知ID
t1.error_code,   -- 故障码
bei.alarm_name,  -- 故障名称
t1.start_time,   -- 故障开始时间
t1.end_time,     -- 故障结束时间
t1.warning_spec,  -- 故障分类
t1.alarm_module,  -- 故障告警模块 
t1.alarm_service,  -- 故障告警服务
t1.alarm_type,   -- 故障告警分类
t1.alarm_level,  -- 故障告警级别
t1.alarm_detail,  -- 故障告警详情
t1.param_value,   -- 故障告警参数值
t1.job_order,   -- 关联作业单
t1.robot_job,   -- 关联机器人任务  
t1.device_code,  -- 关联设备编号
t1.server_code,  -- 关联服务编号
t1.transport_object,  -- 关联搬运对象
t1.point_location,  -- 关联地图码点位置
case when t1.point_location like '%pointCode=%' then substring_index(substring_index(t1.point_location, 'pointCode=', -1), ')', 1) end        as point_code,  -- 关联地图码点编码
substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x_location,  -- 关联地图x坐标
substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y_location,  -- 关联地图y坐标
case when t3.error_id is not null then '是' else '否' end  as is_self_recovery -- 是否自恢复故障		   
from 
(select *
from phoenix_basic.basic_notification
where alarm_module = 'robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
)t1 
-- 注意：一定是用的inner join ,保留同一台机器人相同end_time的第一条
-- part2:相同end_time留下第一条
inner join 
(select
robot_code,
COALESCE(end_time, 'unfinished') as end_time,
min(id)                          as first_error_id
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
group by robot_code, COALESCE(end_time, 'unfinished')
)t2 on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
-- part3:半小时内自恢复成功的故障	
left join 
(select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
)bn 
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
where `method` ='自恢复' and `result`=1
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
AND start_time<=from_unixtime(UNIX_TIMESTAMP('{{ to_dttm }}')+30*60)
{% endif %}
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t3 on t3.error_id = t1.id
left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code	



----------------------------------------------------
-- 故障平均持续时长 
(sum(UNIX_TIMESTAMP(case when end_time is null then now() else end_time end)-UNIX_TIMESTAMP(start_time))/count(distinct error_id))*1000


-- 故障持续时长 
(UNIX_TIMESTAMP(case when end_time is null then now() else end_time end)-UNIX_TIMESTAMP(start_time))*1000







-- 第一次正式使用

select 
t1.robot_code,    -- 机器人编号
t1.id as error_id,    -- 故障通知ID
t1.error_code,   -- 错误码
bei.alarm_name,  -- 故障名称
t1.start_time,   -- 故障开始时间
t1.end_time,     -- 故障结束时间
t1.warning_spec,  -- 故障分类
t1.alarm_module,  -- 故障告警模块 
t1.alarm_service,  -- 故障告警服务
t1.alarm_type,   -- 故障告警分类
t1.alarm_level,  -- 故障告警级别
t1.alarm_detail,  -- 故障告警详情
t1.param_value,   -- 故障告警参数值
t1.job_order,   -- 关联作业单
t1.robot_job,   -- 关联机器人任务  
t1.device_code,  -- 关联设备编号
t1.server_code,  -- 关联服务编号
t1.transport_object,  -- 关联搬运对象
t1.point_location,  -- 关联地图码点位置
case when t1.point_location like '%pointCode=%' then substring_index(substring_index(t1.point_location, 'pointCode=', -1), ')', 1) end        as point_code,  -- 关联地图码点编码
substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x_location,  -- 关联地图x坐标
substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y_location,  -- 关联地图y坐标
case when t3.error_id is not null then '是' else '否' end  as is_self_recovery -- 是否自恢复成功		   
from 
(select *
from phoenix_basic.basic_notification
where alarm_module = 'robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
)t1 
-- 注意：一定是用的inner join ,保留同一台机器人相同end_time的第一条
-- part2:相同end_time留下第一条
inner join 
(select
robot_code,
COALESCE(end_time, 'unfinished') as end_time,
min(id)                          as first_error_id
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
group by robot_code, COALESCE(end_time, 'unfinished')
)t2 on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
-- part3:半小时内自恢复成功的故障	
left join 
(select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
{% endif %}
)bn 
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
where `method` ='自恢复' and `result`=1
{% if from_dttm is not none %}
AND  start_time >= '{{ from_dttm }}'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '{{ to_dttm }}' 
AND start_time<=from_unixtime(UNIX_TIMESTAMP('{{ to_dttm }}')+30*60)
{% endif %}
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t3 on t3.error_id = t1.id
left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code	













----- 以下作废

-- 生成虚拟数据集-sql查询
select 
t1.robot_code,    -- 机器人编号
t1.id as error_id,    -- 故障通知ID
t1.error_code,   -- 错误码
bei.alarm_name,  -- 故障名称
t1.start_time,   -- 故障开始时间
t1.end_time,     -- 故障结束时间
t1.warning_spec,  -- 故障分类
t1.alarm_module,  -- 故障告警模块 
t1.alarm_service,  -- 故障告警服务
t1.alarm_type,   -- 故障告警分类
t1.alarm_level,  -- 故障告警级别
t1.alarm_detail,  -- 故障告警详情
t1.param_value,   -- 故障告警参数值
t1.job_order,   -- 关联作业单
t1.robot_job,   -- 关联机器人任务  
t1.device_code,  -- 关联设备编号
t1.server_code,  -- 关联服务编号
t1.transport_object,  -- 关联搬运对象
t1.point_location,  -- 关联地图码点位置
case when t1.point_location like '%pointCode=%' then substring_index(substring_index(t1.point_location, 'pointCode=', -1), ')', 1) end        as point_code,  -- 关联地图码点编码
substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x_location,  -- 关联地图x坐标
substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y_location,  -- 关联地图y坐标
case when t3.error_id is not null then '是' else '否' end  as is_self_recovery -- 是否自恢复成功		   
from 
(select *
from phoenix_basic.basic_notification
where alarm_module = 'robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '2023-02-01 00:00:00'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '2023-02-08 00:00:00' 
{% endif %}
)t1 
-- 注意：一定是用的inner join ,保留同一台机器人相同end_time的第一条
-- part2:相同end_time留下第一条
inner join 
(select
robot_code,
COALESCE(end_time, 'unfinished') as end_time,
min(id)                          as first_error_id
from phoenix_basic.basic_notification
where alarm_module = 'robot'
and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '2023-02-01 00:00:00'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '2023-02-08 00:00:00' 
{% endif %}
group by robot_code, COALESCE(end_time, 'unfinished')
)t2 on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
-- part3:半小时内自恢复成功的故障	
left join 
(select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
{% if from_dttm is not none %}
AND  start_time >= '2023-02-01 00:00:00'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '2023-02-08 00:00:00' 
{% endif %}
)bn 
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
where `method` ='自恢复' and `result`=1
{% if from_dttm is not none %}
AND  start_time >= '2023-02-01 00:00:00'
{% endif %}
{% if to_dttm is not none %}
AND start_time < '2023-02-08 00:00:00' 
AND start_time<=from_unixtime(UNIX_TIMESTAMP('2023-02-08 00:00:00')+30*60)
{% endif %}
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t3 on t3.error_id = t1.id
left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code	



