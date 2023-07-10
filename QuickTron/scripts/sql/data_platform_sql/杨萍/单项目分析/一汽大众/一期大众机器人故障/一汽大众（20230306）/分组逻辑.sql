set @stat_start_time='2023-02-27 00:00:00'; 
set @stat_next_start_time='2023-03-07 00:00:00'; 


select 
t.group_flag,  
t.robot_code,
t.ip_tail,
DATE_FORMAT(min(t.error_min_start_time), '%Y-%m-%d') as group_start_date,
max(case when t.rn=1 then t.error_code_etl end) as key_error_code,
max(case when t.rn=1 then t.min_error_id end) as key_error_id,
max(case when t.rn=1 then t.alarm_name end) as key_error_name,
min(t.error_min_start_time) as group_start_time,
max(t.error_max_end_time) as group_end_time,
unix_timestamp(max(t.error_max_end_time)) - unix_timestamp(min(t.error_min_start_time)) as group_error_duration, 
sum(t.err_num) as alarm_count,
group_concat(t.error_code_etl) as error_code_list,
group_concat(concat(t.error_code_etl,'(',t.alarm_name,')')) as error_code_name_list

from 
(select 
if(@rnf=t.group_flag,@rn:=@rn+1,@rn:=1) as rn, 
@rnf:=t.group_flag as group_flag_rnf, 
t.*
from 
(select 
t2.group_flag,
t2.robot_code,
t2.ip_tail,
t2.error_code_etl,
t2.alarm_name,
-- min(t2.ur_rank) as error_group_min_rk,
-- max(t2.ur_rank) as error_group_max_rk,
min(t2.error_id) as min_error_id,
count(distinct t2.error_id) as err_num,
sum(t2.error_durations) as error_code_durations,
min(t2.start_time) as error_min_start_time,
max(coalesce (t2.end_time,now())) as error_max_end_time
from 
(
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
order by t.robot_code asc,t.start_time asc)t,(select @vr:=0,@cum:=0) r)t1)t2
group by t2.group_flag,t2.robot_code,t2.ip_tail,t2.error_code_etl,t2.alarm_name
order by t2.robot_code asc,t2.group_flag asc,error_code_durations desc)t,(select @rnf:=null, @rn:=0) r)t
group by t.group_flag,t.robot_code,t.ip_tail
having group_error_duration>30  -- 组故障时长做个限制
order by group_start_date asc,t.ip_tail asc,t.robot_code asc,group_start_time asc