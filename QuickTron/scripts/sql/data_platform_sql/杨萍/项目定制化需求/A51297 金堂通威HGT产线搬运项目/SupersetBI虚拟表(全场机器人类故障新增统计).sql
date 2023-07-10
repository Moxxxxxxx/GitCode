-- Virtual data1:全场机器人类故障新增统计


{% if not from_dttm %}
{% set from_dttm =datetime.date.today() + datetime.timedelta(days=0) %}  -- 开始时间
{% endif %}
{% if not to_dttm %}
{% set to_dttm = datetime.datetime.now() %}  -- 结束时间
{% endif %}
{% set robotCodes = filter_values('robot_code') %}
{% set robotTypeNames = filter_values('robot_type_name') %}
{% set alarmName = filter_values('alarm_name') %}
{% set alarmLevel = filter_values('alarm_level') %}


select 
COALESCE(max(t.error_num),0) as error_num,  -- 机器人类故障新增次数
COALESCE(max(t.work_robot_num),0) as work_robot_num,    -- 参与作业机器人数
COALESCE(max(t.create_order_num),0) as create_order_num, -- 搬运作业单量 
COALESCE(max(t.create_job_num),0) as create_job_num		-- 机器人任务量
from 
(
-- 搬运作业单、机器人任务数量
select
null as error_num,    -- 新增机器人故障次数
count(distinct tocj.robot_code) as work_robot_num,    -- 参与作业机器人数
count(distinct tor.order_no)                      as create_order_num, -- 搬运作业单量
count(distinct tocj.job_sn)                       as create_job_num		-- 机器人任务量						
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
left join phoenix_basic.basic_robot br on br.robot_code=tocj.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code = br.robot_type_code
where 1=1
{% if from_dttm is not none %}
AND tor.create_time >= '{{ from_dttm }}' 
{% endif %}
{% if to_dttm is not none %}
AND tor.create_time <= '{{ to_dttm }}' 
{% endif %}
{% if robotCodes|length>0 %}
AND br.robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
{% endif %}
{% if robotTypeNames|length>0 %}
AND brt.robot_type_name in ( {{ "'" + "','".join(robotTypeNames) + "'" }} )
{% endif %}

UNION ALL 
-- 新增机器人故障次数
select 
count(distinct t1.id)  as error_num,    -- 新增机器人故障次数
null as work_robot_num,    -- 参与作业机器人数
null as create_order_num, -- 搬运作业单量
null as create_job_num		-- 机器人任务量	
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
{% if robotCodes|length>0 %}
AND robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
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
{% if robotCodes|length>0 %}
AND robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
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
{% if robotCodes|length>0 %}
AND robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
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
{% if robotCodes|length>0 %}
AND robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
{% endif %}
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t3 on t3.error_id = t1.id
left join phoenix_basic.basic_error_info bei on bei.error_code = t1.error_code	
left join phoenix_basic.basic_robot br on br.robot_code=t1.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code=br.robot_type_code
where 1=1
and t3.error_id is null
{% if robotCodes|length>0 %}
AND t1.robot_code in ( {{ "'" + "','".join(filter_values('robot_code')) + "'" }} )
{% endif %}
{% if robotTypeNames|length>0 %}
AND brt.robot_type_name in ( {{ "'" + "','".join(robotTypeNames) + "'" }} )
{% endif %}
{% if alarmName|length>0 %}
AND bei.alarm_name in ( {{ "'" + "','".join(alarmName) + "'" }} )
{% endif %}
{% if alarmLevel|length>0 %}
AND t1.alarm_level in ( {{ "'" + "','".join(to_str(alarmLevel)) + "'" }} )
{% endif %}

)t 


-- 故障率（搬运作业单）
case when coalesce(SUM(create_order_num),0)<>0 then coalesce(SUM(error_num),0)/coalesce(SUM(create_order_num),0) else coalesce(SUM(error_num),0)/1 end

-- 故障率（机器人任务）
case when coalesce(SUM(create_job_num),0)<>0 then coalesce(SUM(error_num),0)/coalesce(SUM(create_job_num),0) else coalesce(SUM(error_num),0)/1 end 