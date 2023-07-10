availability_ration -- 可用性
usage_ratio  -- 利用率   A divided by B : A/B

-- 可用性 
-- 单台机器人可用性: (24小时-当天发生的故障总时长)/24小时 
-- 整个项目可用性:(24小时*机器人总数-所有机器人当天发生的故障总时长)/(24小时*机器人总数)
-- 备注:1 假设最完美的情况下是24小时均可用 ; 2 PSA项目机器人总数按109计算


-- PSA : pt ='A51488'
-- 小米: A53018
-- 中南智能: A51186



select 
agv_code,
date_value,
total_error_duration,  -- 故障时长(秒)
error_num,  -- 故障次数
total_error_duration/error_num as avg_error_duration,  -- 平均故障时长(秒)
case when agv_code is null then (109*3600*24-total_error_duration)/(109*3600*24) when agv_code is not null then (3600*24-total_error_duration)/(3600*24) end as availability_ration -- 可用性
from 
(select 
agv_code,
to_date(start_time) as date_value,
sum(error_duration) as total_error_duration,
count(distinct message_id) as error_num
from 
(select 
t1.*,
lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as next_start_time, 
lead(notify_close_time,1) over(partition by agv_code order by notify_start_time asc) as next_close_time,
unix_timestamp(notify_close_time)-unix_timestamp(notify_start_time) as error_duration,
unix_timestamp(lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc))-unix_timestamp(notify_close_time) as to_next_start_duration,
lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as pre_start_time,
unix_timestamp(notify_start_time)-unix_timestamp(lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc)) as pre_start_duration
from 
(select t.*,
notify_start_time as start_time, 
notify_close_time as close_time,
lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc) as pre_close_time,
case when notify_close_time = lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc,notify_start_time asc) then 1 else 0 end flag

from dwd.dwd_notification_message_info_di t 
where pt ='A51488'
--and d='2023-01-09'
--and notify_level=2 
and warning_type='ROBOT'
and message_title in ('RCS_RbtErr_NotOnCode','RCS_RbtErr_UNKONW')
--and agv_code in 
--('CARRIER_192168055123',
--'CARRIER_192168055085',
--'CARRIER_192168055092',
--'CARRIER_192168055108',
--'CARRIER_192168055103'
--)
order by agv_code,notify_start_time
)t1 
-- where t1.flag =0
)t2
where to_date(t2.notify_close_time) !='1900-01-01'
and pre_start_duration > 60
-- order by agv_code,notify_start_time asc
group by agv_code,to_date(start_time)
GROUPING SETS(
(agv_code,to_date(start_time)),
(to_date(start_time))
)
)t3

---- 对应故障流水

select 
t2.*
from
(select 
t1.*,
lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as next_start_time, 
lead(notify_close_time,1) over(partition by agv_code order by notify_start_time asc) as next_close_time,
unix_timestamp(notify_close_time)-unix_timestamp(notify_start_time) as error_duration,
unix_timestamp(lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc))-unix_timestamp(notify_close_time) as to_next_start_duration,
lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as pre_start_time,
unix_timestamp(notify_start_time)-unix_timestamp(lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc)) as pre_start_duration
from 
(select t.*,
notify_start_time as start_time, 
notify_close_time as close_time,
lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc) as pre_close_time,
case when notify_close_time = lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc,notify_start_time asc) then 1 else 0 end flag

from dwd.dwd_notification_message_info_di t 
where pt ='A53018'
--and d='2023-01-09'
--and notify_level=2 
and warning_type='ROBOT'
and message_title in ('RCS_RbtErr_NotOnCode','RCS_RbtErr_UNKONW')
--and agv_code in 
--('CARRIER_192168055123',
--'CARRIER_192168055085',
--'CARRIER_192168055092',
--'CARRIER_192168055108',
--'CARRIER_192168055103'
--)
order by agv_code,notify_start_time
)t1 
-- where t1.flag =0
)t2
where to_date(t2.notify_close_time) !='1900-01-01'
and pre_start_duration > 60
order by agv_code,notify_start_time asc



-------


select t1.*,
COALESCE (t2.error_cname,'未维护') as error_cname
--coalesce(coalesce(t1.error_display_name,t2.error_cname),t1.error_code) as error_display_name
from dwd.dwd_agv_breakdown_detail_incre_dt t1
left join dim.dim_dsp_error_dict t2 on t2.error_code =t1.error_code 
where t1.pt in ('A51488','A53018','A51186')
and t1.error_code_position=0
--and t1.breakdown_id ='message__1492549924451576'
order by t1.pt,t1.breakdown_id 

-------
select 
t2.pt,
t2.id,
t2.message_id,
t2.message_body,
t2.agv_code,
t2.notify_start_time,
to_date(t2.notify_start_time) as date_value,
t3.first_error_code,
t3.error_code_list,
t3.error_code,
t3.error_cname
from
(select 
t1.*,
lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as next_start_time, 
lead(notify_close_time,1) over(partition by agv_code order by notify_start_time asc) as next_close_time,
unix_timestamp(notify_close_time)-unix_timestamp(notify_start_time) as error_duration,
unix_timestamp(lead(notify_start_time,1) over(partition by agv_code order by notify_start_time asc))-unix_timestamp(notify_close_time) as to_next_start_duration,
lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc) as pre_start_time,
unix_timestamp(notify_start_time)-unix_timestamp(lag(notify_start_time,1) over(partition by agv_code order by notify_start_time asc)) as pre_start_duration
from 
(select t.*,
notify_start_time as start_time, 
notify_close_time as close_time,
lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc) as pre_close_time,
case when notify_close_time = lag(notify_close_time,1) over(partition by agv_code order by notify_close_time asc,notify_start_time asc) then 1 else 0 end flag

from dwd.dwd_notification_message_info_di t 
where pt in ('A51488','A53018','A51186')
--and d='2023-01-09'
--and notify_level=2 
and warning_type='ROBOT'
and message_title in ('RCS_RbtErr_NotOnCode','RCS_RbtErr_UNKONW')
--and agv_code in 
--('CARRIER_192168055123',
--'CARRIER_192168055085',
--'CARRIER_192168055092',
--'CARRIER_192168055108',
--'CARRIER_192168055103'
--)
order by agv_code,notify_start_time
)t1 
-- where t1.flag =0
)t2
left join 
(select 
t1.pt,
t1.agv_code,
t1.first_error_code,
t1.error_code_list,
t1.message_body,
t1.message_id,
t2.error_code,
--case when t2.error_cname is not null and  t2.error_cname!='' then t2.error_cname else '未维护' end error_cname
COALESCE (t2.error_cname,'未维护') as error_cname
from  
(select 
t.*,
regexp_replace(t1.error_code,'[\\[\\]]','') as first_error_code,
t1.pos as error_code_position,
if(regexp_replace(t1.error_code,'[\\[\\]]','')='0',t1.pos,null) as error_code_0_position_list
from 
(select 
t.*,
regexp_replace(regexp_replace(regexp_extract(message_body,'\\[.*\\]',0),'\\[ ','['),'\\s+',',') as error_code_list
from dwd.dwd_notification_message_info_di t 
where pt in ('A51488','A53018','A51186')
and message_title in ('RCS_RbtErr_NotOnCode','RCS_RbtErr_UNKONW')
)t 
lateral view posexplode(split(t.error_code_list,',')) t1 as pos,error_code)t1 
left join dim.dim_dsp_error_dict t2 on t2.error_code =t1.first_error_code
where t1.error_code_position=0
)t3 on t3.pt=t2.pt and t3.message_id=t2.message_id 
where to_date(t2.notify_close_time) !='1900-01-01'
and t2.pre_start_duration > 60
order by t2.agv_code,t2.notify_start_time asc




------------------------------------------------------------------------------------------------------------
-- 从PSA、小米、中南智能这三个项目发现的未维护的故障
select 
t1.error_code,
count(distinct t1.message_id) as message_num
from  
(select 
t.*,
regexp_replace(t1.error_code,'[\\[\\]]','') as error_code,
t1.pos as error_code_position,
if(regexp_replace(t1.error_code,'[\\[\\]]','')='0',t1.pos,null) as error_code_0_position_list
from 
(select 
t.*,
regexp_replace(regexp_replace(regexp_extract(message_body,'\\[.*\\]',0),'\\[ ','['),'\\s+',',') as error_code_list
from dwd.dwd_notification_message_info_di t 
where pt in ('A51488','A53018','A51186')
and message_title in ('RCS_RbtErr_NotOnCode','RCS_RbtErr_UNKONW')
)t 
lateral view posexplode(split(t.error_code_list,',')) t1 as pos,error_code)t1 
left join dim.dim_dsp_error_dict t2 on t2.error_code =t1.error_code 
where t2.error_code is null 
group by t1.error_code






------------------------------------------------------------------------------------------------------------
-- PSA : pt ='A51488'
-- 小米: A53018
-- 中南智能: A51186


-- 利用率 
-- 单台机器人利用率: 使用时长/24小时 
-- 使用时长:机器人开始执行任务到任务执行完成的时长
-- 备注:1 假设最完美的情况下是24小时均可用 


select 
t1.d,
t1.agv_code,
COALESCE(t2.duration,0) as usage_duration, 
COALESCE(t2.duration,0)/(3600*24) as usage_ratio  -- 利用率 
from 
(select  d,agv_code 
from dwd.dwd_rcs_agv_base_info_df
where pt='A51488' and d>='2023-01-03' and d<='2023-01-10')t1 
left join 
(select 
agv_code,
to_date(job_execute_time) as date_value, 
sum(execute_finish_duration) as duration
from 
(select 
agv_code,
job_id,
dest_point_code,
job_created_time,
job_state,
job_type,
job_accept_time,
job_execute_time,
job_finish_time,
unix_timestamp(job_execute_time)-unix_timestamp(job_accept_time) as accept_execute_duration,
unix_timestamp(job_finish_time)-unix_timestamp(job_execute_time) as execute_finish_duration,
unix_timestamp(job_finish_time)-unix_timestamp(job_accept_time) as accept_finish_duration
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
and to_date(job_finish_time) = to_date(job_execute_time)
)t 
group by agv_code,agv_code,to_date(job_execute_time))t2 on t2.date_value=t1.d and t2.agv_code=t1.agv_code
order by 
t1.d
,t2.duration


------------------------------------------------

select 
job_type,
count(distinct job_id) as job_num,
avg(execute_finish_duration) as avg_duration 
from
(select 
agv_code,
job_id,
dest_point_code,
job_created_time,
job_state,
job_type,
job_accept_time,
job_execute_time,
job_finish_time,
unix_timestamp(job_execute_time)-unix_timestamp(job_accept_time) as accept_execute_duration,
unix_timestamp(job_finish_time)-unix_timestamp(job_execute_time) as execute_finish_duration,
unix_timestamp(job_finish_time)-unix_timestamp(job_accept_time) as accept_finish_duration
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
and to_date(job_finish_time) = to_date(job_execute_time)
)t
group by job_type