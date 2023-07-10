产品语雀文档链接：https://quicktron.yuque.com/softdoc/vlhmhd/yfao47


搬运作业单（order_no）

搬运作业单耗时、对应开始时间、对应结束时间        -- （备注：从接口平台接收作业单的时间 到 接口平台接收到完成指令 的总耗时）
作业单下发至车动耗时、对应开始时间、对应结束时间  -- （备注：从接口平台接收作业单的时间 到 小车开始移动时间 的总耗时。）
分车耗时、对应开始时间、对应结束时间              --（备注：从接口平台接收作业单的时间 到 第一个下位机指令下发时间 的总耗时）
分车次数                                  -- （备注：作业单下发后，执行分车指令的次数）
机器人空车移动耗时、对应开始时间、对应结束时间   -- （备注：小车开始空车移动到小车空车移动完成时间（都使用上位机上报的时间））
机器人顶升完成至车动耗时、对应开始时间、对应结束时间     -- （备注：从顶升后确定完成时间 到 小车开始移动时间 的总耗时）
机器人带载移动耗时 、对应开始时间、对应结束时间 -- （备注：小车开始带载移动时间到小车带载移动完成时间（使用上位机上报的时间））
机器人降下完成至作业单完成、对应开始时间、对应结束时间     -- （备注：机器人降下确定完成时间 到 接口平台接收到完成指令的总时长）


接口下发至车开始运动耗时、对应开始时间、对应结束时间 
接口平台处理耗时、对应开始时间、对应结束时间 
接口平台至RSS服务的通信耗时、对应开始时间、对应结束时间 
RSS服务处理耗时、对应开始时间、对应结束时间 
RSS服务至RMS服务的通信耗时、对应开始时间、对应结束时间 
RMS服务处理耗时、对应开始时间、对应结束时间 
wifi无线网络通信耗时、对应开始时间、对应结束时间 
上位机处理耗时、对应开始时间、对应结束时间 
顶升货架完成至车动耗时、对应开始时间、对应结束时间 
上位机处理耗时、对应开始时间、对应结束时间 
wifi无线网络通信耗时、对应开始时间、对应结束时间 
RMS服务处理耗时、对应开始时间、对应结束时间 
wifi无线网络通信耗时、对应开始时间、对应结束时间 
上位机处理耗时、对应开始时间、对应结束时间 
降下货架完成至作业单上报上游完成耗时、对应开始时间、对应结束时间 
上位机处理耗时、对应开始时间、对应结束时间 
wifi无线网络通信耗时、对应开始时间、对应结束时间 
RMS服务处理耗时、对应开始时间、对应结束时间 
RMS服务至RSS服务的通信耗时、对应开始时间、对应结束时间 
RSS服务处理耗时、对应开始时间、对应结束时间 
RSS服务至接口平台服务的通信耗时、对应开始时间、对应结束时间 
接口平台处理耗时、对应开始时间、对应结束时间 




select 
t1.order_no,
t1.subject_id,
t2.parent_step_no,
t4.default_name as parent_default_name,
t1.step_no,
t2.default_name,
t1.time_type,
from_unixtime(t1.time_value/1000) as time_value_to_time,   -- 当time_type<>8时，则是时间
from_unixtime((t1.time_value << 23 >> 23)/1000) as start_time,  -- 当time_type=8时，表示的时间范围的开始时间
from_unixtime(((t1.time_value << 23 >> 23)+(t1.time_value >> 41))/1000) as end_time,  -- 当time_type=8时，表示的时间范围的结束时间 
t1.time_value >> 41 as duration_millisecond,        -- 当time_type=8时，表示的时间范围的持续时长（ms）
t1.time_value,
t1.content_id,
t1.trace_log_id,
t1.trace_log_create_time,
t3.content 
from 
(select 
distinct 
t.order_no,
btl.id as trace_log_id,
btl.create_time as trace_log_create_time, 
btl.subject_id,
btl.step_no,
btl.time_type,
btl.time_value,
btl.content_id
from 
(select 
t2.subject_no as upstream_order_no,t2.subject_type as upstream_order_no_type,t2.id as upstream_order_no_subject_id,   -- upstream_order_no,  -- 上游作业单ID
t1.subject_no as order_no,t1.subject_type as order_no_type,t1.id as order_no_subject_id,   -- order_no,     -- 搬运作业单ID
t3.subject_no as job_sn,t3.subject_type as job_sn_type,t3.id as job_sn_subject_id,    -- job_sn,     -- 机器人任务号job_sn
t4.subject_no as action_uid,t4.subject_type as action_uid_type,t4.id as action_uid_subject_id    -- action_uid   -- 机器人动作消息ID action_uid 
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.subject_no =t1.parent_no
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no =t1.subject_no
left join phoenix_basic.basic_trace_subject t4 on t4.parent_no =t3.subject_no
where 1=1
and t1.subject_type='ORDER' 
and t1.subject_no ='SIRack_166797921617200101'
)t 
left join phoenix_basic.basic_trace_log btl 
on btl.subject_id=t.upstream_order_no_subject_id or 
btl.subject_id=t.order_no_subject_id or 
btl.subject_id=t.job_sn_subject_id or 
btl.subject_id=t.action_uid_subject_id)t1 
left join phoenix_basic.basic_trace_step t2 on t2.step_no=t1.step_no
left join phoenix_basic.basic_trace_log_content t3 on t3.id=t1.content_id
left join phoenix_basic.basic_trace_step t4 on t4.step_no=t2.parent_step_no
order by t1.trace_log_create_time asc 





-- Data1:链路步骤（父子层）
select 
t2.subject_type as parent_subject_type, -- parent链路步骤主体类型
t2.default_name as parent_default_name,  -- parent链路默认名称
t2.`hierarchy` as parent_hierarchy,      -- parent链路层级
t2.step_no as parent_step_no,            -- parent链路步骤号
t1.subject_type,                         -- 链路步骤主体类型
t1.default_name as step_default_name,    -- 链路默认名称
t1.`hierarchy` as step_hierarchy,        -- 链路层级
t1.step_no
from phoenix_basic.basic_trace_step t1 
left join phoenix_basic.basic_trace_step t2 on t2.step_no =t1.parent_step_no 
order by t1.`hierarchy`,t1.subject_type asc



-- Data2:三级链路步骤关系（从第一层往第三层找）
-- 126环境共150条
-- 与（从第三层往第一层找）的数据对比来看，（从第一层往第三层找）会遗漏关系
select 
t1.subject_type as subject_type_1, -- 第一层链路步骤主体类型
t1.default_name as default_name_1,  -- 第一层链路默认名称
t1.`hierarchy` as hierarchy_1,      -- 第一层链路层级
t1.step_no as step_no_1,            -- 第一层链路步骤号
t2.subject_type as subject_type_2, -- 第二层链路步骤主体类型
t2.default_name as default_name_2,  -- 第二层链路默认名称
t2.`hierarchy` as hierarchy_2,      -- 第二层链路层级
t2.step_no as step_no_2,            -- 第二层链路步骤号
t3.subject_type as subject_type_3, -- 第三层链路步骤主体类型
t3.default_name as default_name_3,  -- 第三层链路默认名称
t3.`hierarchy` as hierarchy_3,      -- 第三层链路层级
t3.step_no as step_no_3            -- 第三层链路步骤号
from phoenix_basic.basic_trace_step t1
left join phoenix_basic.basic_trace_step t2 on t2.parent_step_no =t1.step_no and t2.`hierarchy`=2
left join phoenix_basic.basic_trace_step t3 on t3.parent_step_no =t2.step_no and t3.`hierarchy`=3 
where t1.`hierarchy`=1
order by t1.subject_type desc,t1.default_name desc,t2.default_name desc,t3.default_name desc



-- Data3:三级链路步骤关系（从第三层往第一层找）
-- 126环境共253条 
-- 出现 父子的链路层级都是2的情况
-- 172.31.236.51 环境共363条

select 
t3.subject_type as subject_type_1, -- 第一层链路步骤主体类型
t3.default_name as default_name_1,  -- 第一层链路默认名称
t3.`hierarchy` as hierarchy_1,      -- 第一层链路层级
t3.step_no as step_no_1,            -- 第一层链路步骤号
t2.subject_type as subject_type_2, -- 第二层链路步骤主体类型
t2.default_name as default_name_2,  -- 第二层链路默认名称
t2.`hierarchy` as hierarchy_2,      -- 第二层链路层级
t2.step_no as step_no_2,            -- 第二层链路步骤号
t1.subject_type as subject_type_3, -- 第三层链路步骤主体类型
t1.default_name as default_name_3,  -- 第三层链路默认名称
t1.`hierarchy` as hierarchy_3,      -- 第三层链路层级
t1.step_no as step_no_3            -- 第三层链路步骤号
from phoenix_basic.basic_trace_step t1
left join phoenix_basic.basic_trace_step t2 on t2.step_no =t1.parent_step_no 
left join phoenix_basic.basic_trace_step t3 on t3.step_no =t2.parent_step_no 
order by t1.`hierarchy` asc,t2.`hierarchy` asc



ORDER	接收作业单	1	201010000	ORDER	RSS接收到接口平台请求	2	201013000	ORDER	RSS接收	2
ORDER	接收作业单	1	201010000	ORDER	落库	2	201014000	ORDER	RSS内部处理	2
ORDER	分车	1	201020000	ORDER	作业单-分车	2	201021000	ORDER	分车结果	2
ORDER	接收作业单	1	201010000	ORDER	接口平台接收下发请求	2	201011000	ORDER	Interface接收	2
ORDER	接收作业单	1	201010000	ORDER	接口平台转发请求至RSS	2	201012000	ORDER	Interface >> RSS	2
ORDER	下发任务	1	201030000	ORDER	作业单-下发任务	2	201031000	ORDER	RSS >> RMS	2
ORDER	接收作业单	1	201010000	ORDER	接口平台转发请求至RSS	2	201012000	ORDER	Interface >> RSS	2
ORDER	下发任务	1	201030000	ORDER	作业单-下发任务	2	201031000	ORDER	RSS >> RMS	2








-- Data3:上游作业单、系统作业单、机器人任务、机器人动作消息 关系
select 
t1.parent_no as upstream_order_no,   -- 上游作业单
t4.id as upstream_order_no_subject_id,
t1.subject_no as order_no,           -- 系统作业单
t1.id as order_subject_id,
t2.subject_no as job_sn,    -- 机器人任务
t2.id as job_subject_id,
t3.subject_no as action_uid,   -- 机器人动作消息
t3.id as action_subject_id
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no=t2.subject_no
left join phoenix_basic.basic_trace_subject t4 on t4.subject_no=t1.parent_no
where t1.subject_type='ORDER' and t1.parent_no <> ''
and t1.subject_no='SIRack_166797921617200101'
order by t1.subject_no asc



-- Data3:
select 
t.upstream_order_no,  -- 上游作业单
t.order_no,  -- 系统作业单
t.subject_no,  -- 链路主体
t.subject_id,  -- 链路主体id
t.subject_type,  -- 链路主体类型
btl.id as trace_log_id,  -- 链路日志id
btl.create_time as trace_log_create_time,   -- lianlu
btl.subject_id,
bts3.step_no as step_no_1,
bts3.default_name as step_name_1,
bts3.`hierarchy` as step_hierarchy_1,
bts2.step_no as step_no_2,
bts2.default_name as step_name_2,
bts2.`hierarchy` as step_hierarchy_2,
bts.step_no as step_no_3,
bts.default_name as step_name_3,
bts.`hierarchy` as step_hierarchy_3,
btl.time_type,
btl.time_value,  -- 链路步骤日志时间值
from_unixtime(btl.time_value/1000) as time_value_to_time,   -- 当time_type<>8时，则是时间
from_unixtime((btl.time_value << 23 >> 23)/1000) as start_time,  -- 当time_type=8时，表示的时间范围的开始时间
from_unixtime(((btl.time_value << 23 >> 23)+(btl.time_value >> 41))/1000) as end_time,  -- 当time_type=8时，表示的时间范围的结束时间 
btl.time_value >> 41 as duration_millisecond,        -- 当time_type=8时，表示的时间范围的持续时长（ms）
btl.content_id,  -- 链路步骤日志内容id
btl.content_type,  -- 链路步骤日志内容类型
btlc.content  -- 链路步骤日志内容
from 
-- 找到每个作业单的所有对象id
(select 
t1.parent_no as upstream_order_no,
t1.subject_no as order_no,
t2.subject_no,
t2.id as subject_id,
'upstream_order' as subject_type
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.subject_no =t1.parent_no
where t1.subject_type='ORDER' and t1.parent_no <> ''
and t1.subject_no='SIRack_166797921617200101'
union all 
select 
t1.parent_no as upstream_order_no,
subject_no as order_no,
subject_no,
id as subject_id,
'order' as subject_type
from phoenix_basic.basic_trace_subject t1
where t1.subject_type='ORDER' and t1.parent_no <> ''
and t1.subject_no='SIRack_166797921617200101'
union all 
select 
t1.parent_no as upstream_order_no,
t1.subject_no as order_no,
t2.subject_no,
t2.id as subject_id,
'job' as subject_type
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no
where t1.subject_type='ORDER' and t1.parent_no <> ''
and t1.subject_no='SIRack_166797921617200101'
union all 
select 
t1.parent_no as upstream_order_no,
t1.subject_no as order_no,
t3.subject_no,
t3.id as subject_id,
'action' as subject_type
from phoenix_basic.basic_trace_subject t1
left join phoenix_basic.basic_trace_subject t2 on t2.parent_no=t1.subject_no
left join phoenix_basic.basic_trace_subject t3 on t3.parent_no=t2.subject_no
where t1.subject_type='ORDER' and t1.parent_no <> ''
and t1.subject_no='SIRack_166797921617200101'
)t 
left join phoenix_basic.basic_trace_log btl on btl.subject_id =t.subject_id 
left join phoenix_basic.basic_trace_log_content btlc on btlc.id=btl.content_id
left join phoenix_basic.basic_trace_step bts on bts.step_no=btl.step_no
left join phoenix_basic.basic_trace_step bts2 on bts2.step_no=bts.parent_step_no
left join phoenix_basic.basic_trace_step bts3 on bts3.step_no =bts2.parent_step_no
order by t.order_no,btl.id asc 
