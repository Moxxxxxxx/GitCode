set @stat_start_time='2022-11-01 00:00:00'; 
set @stat_next_start_time='2023-03-01 00:00:00'; 


-- 机器人故障分组明细
-- 这一层内是分组明细
select 
concat(robot_error_flag,'~',case when ur_rank =1 and group_first_error_id =0 then error_id else group_error_flag end) as group_flag,
t1.*
from 
(select 
case when @vr!=t.robot_error_flag then @cum:=group_first_error_id when @vr=t.robot_error_flag  then @cum:=@cum+group_first_error_id end as group_error_flag,
@vr:=t.robot_error_flag as robot_error_flag_vr,
t.*
from 
(select 
if(@ur!=t.robot_error_flag,t.error_id,0) as group_first_error_id,
if(@ur=t.robot_error_flag,@urk:=@urk+1,@urk:=1) as ur_rank, 
@ur:=t.robot_error_flag as robot_error_flag_ur, 
t.*
from 
(select 
DATE_FORMAT(bn.start_time, '%Y-%m-%d') as start_date,
bn.robot_code,
right(br.ip,2) as ip_tail,
br.ip,
right(bn.robot_code,2) as robot_code_tail,
bn.id as error_id,
bn.start_time,
bn.end_time,
case when bn.end_time is not null then UNIX_TIMESTAMP(bn.end_time)-UNIX_TIMESTAMP(bn.start_time) end as error_durations,
DATE_FORMAT(bn.start_time, '%Y-%m-%d %H:%i:%s') as start_second,
DATE_FORMAT(bn.end_time, '%Y-%m-%d %H:%i:%s') as end_second,
bn.error_code,
case when bn.error_code regexp '^[A-Z]{3}0{6}'  then coalesce(case when bn.param_value regexp '^[A-Z]{3}' and length(bn.param_value)=9 then bn.param_value end,bn.error_code) else bn.error_code end error_code_etl,
bei.alarm_name,
bei.alarm_detail as alarm_name_detail_info,
bei.solution,
bn.alarm_detail,
bn.alarm_level ,
bn.alarm_module ,
bn.alarm_service ,
bn.alarm_type,
bn.param_value,
bn.job_order,
bn.robot_job,
substring_index(substring_index(bn.point_location, "x=", -1), ",", 1) as x,
substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1) as y,
concat(substring_index(substring_index(bn.point_location, "x=", -1), ",", 1),'-',substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1)) as xy,
case when bn.point_location like "%pointCode=%" then replace(substring_index(bn.point_location, "pointCode=", -1), ")", "") end as point_code, 
bn.point_location,
bn.warning_spec,
coalesce (bn.robot_job,case when bn.point_location like "%pointCode=%" then replace(substring_index(bn.point_location, "pointCode=", -1), ")", "") end,concat(substring_index(substring_index(bn.point_location, "x=", -1), ",", 1),'-',substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1)),case when bn.error_code regexp '^[A-Z]{3}0{6}'  then coalesce(case when bn.param_value regexp '^[A-Z]{3}' and length(bn.param_value)=9 then bn.param_value end,bn.error_code) else bn.error_code end) as error_flag_info,
concat(bn.robot_code,'~',coalesce (bn.robot_job,case when bn.point_location like "%pointCode=%" then replace(substring_index(bn.point_location, "pointCode=", -1), ")", "") end,concat(substring_index(substring_index(bn.point_location, "x=", -1), ",", 1),'-',substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1)),case when bn.error_code regexp '^[A-Z]{3}0{6}'  then coalesce(case when bn.param_value regexp '^[A-Z]{3}' and length(bn.param_value)=9 then bn.param_value end,bn.error_code) else bn.error_code end)) as robot_error_flag

from phoenix_basic.basic_notification bn
left join phoenix_basic.basic_error_info bei 
on bei.error_code =(case when bn.error_code regexp '^[A-Z]{3}0{6}'  then coalesce(case when bn.param_value regexp '^[A-Z]{3}' and length(bn.param_value)=9 then bn.param_value end,bn.error_code) else bn.error_code end )
left join phoenix_basic.basic_robot br on br.robot_code=bn.robot_code
where bn.alarm_module ='robot' and bei.level >=3
and bn.start_time >=@stat_start_time and bn.start_time <@stat_next_start_time
-- and bn.robot_code='H150A-018'
order by bn.robot_code,bn.start_time asc)t,(select @ur:=null,@urk:=0) r
order by t.robot_code asc,t.start_time asc)t,(select @vr:=0,@cum:=0) r)t1
















--- job执行平均时长
select 
job_type,
count(distinct job_sn) as job_count,
avg(d2) as avg_job_exec_fin_duration,
avg(d1) as avg_job_acc_fin_duration,
avg(duration)/1000 as avg_duration
from 
(select 
UNIX_TIMESTAMP(jh.finish_time)-UNIX_TIMESTAMP(jh.accept_time) as d1,
UNIX_TIMESTAMP(jh.execute_time)-UNIX_TIMESTAMP(jh.accept_time) as d2,
jh.*
from phoenix_rms.job_history jh)t  
group by job_type


CHARGE	12758	37.7557504	652.6019396	652.60193965
CUSTOMIZE	82788	4.5903463	367.2629357	367.26293572
DEADLOCK	13327	1.0738882	189.0238536	189.02385361
EXIT_CHARGE	8663	79.5292433	444.3647798	444.36477975
HOMING	11	0.0651818	211.1853636	211.18536364
IDLE_PARKING	63299	6.7502774	231.0034998	231.00349976
MOVE	4835	328.4848865	1248.1049127	1248.10491272



--------------------------------------------



select 
count(distinct id) as error_count,
count(distinct case when robot_job is not null then id end) as have_job_error_count,
count(distinct case when robot_job is null then id end) as null_job_error_count
from phoenix_basic.basic_notification bn
where bn.alarm_module ='robot' and bn.alarm_level >=3

25167	21816	3351

21816/25167=86.7%  -- 机器人类故障近九成故障都带有job编号


19571/20141=97.2%	 -- 用 job_sn 或 pointCode 做故障批次标识信息，能使97.2%的故障打上标记

19743/20141=98%	    -- 用 job_sn 或 pointCode 或 xy坐标 做故障批次标识信息，能使98%的故障打上标记