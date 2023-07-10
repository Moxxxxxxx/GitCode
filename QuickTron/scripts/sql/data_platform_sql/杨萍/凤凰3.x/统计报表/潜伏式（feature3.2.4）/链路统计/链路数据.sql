链路数据

phoenix_basic.basic_trace_subject
phoenix_basic.basic_trace_step
phoenix_basic.basic_trace_log
phoenix_basic.basic_trace_log_content



select 
t1.parent_no as upstream_order_no,  -- 上游作业单ID
t1.subject_no as order_no,     -- 搬运作业单ID
t2.subject_no as job_sn,     -- 机器人任务号job_sn
t3.subject_no as action_uid   -- 机器人动作消息ID action_uid 
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no and t2.subject_type='JOB' 
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no=t2.subject_no and t3.subject_type='ACTION'
where t1.subject_type='ORDER' and t1.subject_no='SIRack_166676707082000001'





select *
from phoenix_basic.basic_trace_step
where subject_type='ORDER'
order by `hierarchy` asc

group_concat(default_name) ：
接收作业单,接口平台接收下发请求,Interface接收,接口平台转发请求至RSS,Interface >> RSS,Interface >> RSS,RSS接收到接口平台请求,RSS接收,落库,RSS内部处理,分车,作业单-分车,分车结果,下发任务,作业单-下发任务,RSS >> RMS,RSS >> RMS


接收作业单
分车
下发任务
接口平台接收下发请求
Interface接收
接口平台转发请求至RSS
Interface >> RSS
Interface >> RSS
RSS接收到接口平台请求
RSS接收
落库
RSS内部处理
作业单-分车
分车结果
作业单-下发任务
RSS >> RMS
RSS >> RMS



select *
from phoenix_basic.basic_trace_step
where subject_type='JOB'
order by `hierarchy` asc

group_concat(default_name) ：
任务接收,RMS接收到任务请求,任务下发,机器人状态检查,当前是否可以作业,落库,任务保存,任务处理,选取任务,选取好的任务,任务执行,任务执行-内部处理,任务取消,任务暂停,任务恢复

任务接收
任务处理
RMS接收到任务请求
机器人状态检查
落库
选取任务
任务执行
任务下发
当前是否可以作业
任务保存
选取好的任务
任务执行-内部处理
任务取消
任务暂停
任务恢复


select *
from phoenix_basic.basic_trace_step
where subject_type='ACTION'
order by `hierarchy` asc


group_concat(default_name) ：
移动降下,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动顶升,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动降下,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动降下,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动顶升,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动顶升,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动顶升,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动顶升,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动降下,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,移动,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,退出充电,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报,充电,开始处理,RMS内部处理,请求路径规划,RMS >> RTS,RMS >> RTS,下发动作,RMS >> QSH,RMS >> QSH,执行动作,中间事件,外设交互,动作结果上报





select 
t1.id as subject_id,
t1.parent_no as upstream_order_no,  -- 上游作业单ID
t1.subject_no as order_no,     -- 搬运作业单ID
t2.subject_no as job_sn,     -- 机器人任务号job_sn
t3.subject_no as action_uid,   -- 机器人动作消息ID action_uid 
t4.step_no,
t6.default_name,
t4.content_id as log_id,
t5.content 
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no and t2.subject_type='JOB' 
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no=t2.subject_no and t3.subject_type='ACTION'
left join phoenix_basic.basic_trace_log t4 on t4.subject_id=t1.id
left join phoenix_basic.basic_trace_log_content t5 on t5.id=t4.content_id
left join phoenix_basic.basic_trace_step t6 on t6.step_no =t4.step_no
where t1.subject_type='ORDER' and t1.subject_no='SIRack_166676707082000001'



order_no='SIRack_166787676117801232' 33个json


select 
subject_type,
count(0) as num,
count(distinct subject_no)
from phoenix_basic.basic_trace_subject
group by subject_type


select count(0) from phoenix_basic.basic_trace_log_content

449781/6725=66.88






-------------------------------------------------------

phoenix_basic.basic_trace_log.time_type （目前有23458）
RECEIVE_TIME 2 -- 接收时间
REQUEST_TIME 3  -- 发出请求时间（远程调用）
RESPONSE_TIME  4  -- 收到回复时间（远程调用）
INVOKE_START_TIME  5  -- 链路步骤触发时间（非远程调用）
INVOKE_END_TIME  6   -- 链路步骤结束时间（非远程调用）
TIME_COST_DURATION   7   -- 链路步骤持续时间（比如步骤耗时30ms）
TIME_COST_RANGE   8  -- 链路步骤时间范围（从开始时间持续了多久，由此算出开始时间结束时间）


select 
t1.id as subject_id,
t1.parent_no as upstream_order_no,  -- 上游作业单ID
t1.subject_no as order_no,     -- 搬运作业单ID
t2.subject_no as job_sn,     -- 机器人任务号job_sn
t3.subject_no as action_uid,   -- 机器人动作消息ID action_uid 
t4.step_no,
t6.default_name,
t4.time_type,
t4.time_value,  
from_unixtime(t4.time_value/1000) as time_value_to_time,   -- 当time_type<>8时，则是时间
from_unixtime((t4.time_value << 23 >> 23)/1000) as start_time,  -- 当time_type=8时，表示的时间范围的开始时间
from_unixtime(((t4.time_value << 23 >> 23)+(t4.time_value >> 41))/1000) as end_time,  -- 当time_type=8时，表示的时间范围的结束时间 
t4.time_value >> 41 as duration_millisecond,        -- 当time_type=8时，表示的时间范围的持续时长（ms）
t4.content_id as log_id,
t5.create_time ,
t5.content 
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no and t2.subject_type='JOB' 
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no=t2.subject_no and t3.subject_type='ACTION'
left join phoenix_basic.basic_trace_log t4 on t4.subject_id=t1.id
left join phoenix_basic.basic_trace_log_content t5 on t5.id=t4.content_id
left join phoenix_basic.basic_trace_step t6 on t6.step_no =t4.step_no
where t1.subject_type='ORDER' and t1.subject_no='SIRack_166797921617200101'
order by start_time asc


eb5e4a89037d4533893e4048e303869a  -- upstream_order_no  -- 4407
SIRack_166797921617200101   -- order_no  -- 4408
BM_SI_166797921647200102    -- job_sn  --4410
action_LvbKjGrfl   -- action_uid  4411
action_AzNKltYIP   -- action_uid  4415

------------------------------------------------------

subject_type='ORDER'时，default_name有以下：







