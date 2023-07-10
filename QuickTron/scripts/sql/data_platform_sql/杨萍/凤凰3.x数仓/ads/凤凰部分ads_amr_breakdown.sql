
-- 所有符合执行范围内的机器人故障明细
with phx_robot_error_detail as (
select 
te.project_code,
te.id,
te.robot_code,
te.robot_type_code,
te.robot_type_name,
te.first_classification,
te.error_code,
tde.error_name,
te.error_start_time,
te.error_end_time,
unix_timestamp(te.error_end_time)-unix_timestamp(te.error_start_time) as error_duration,
te.error_level,
te.error_detail, 
te.error_module,
to_date(te.error_start_time) as error_start_date
from ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di te
left join ${dim_dbname}.dim_phx_basic_error_info_ful tde on tde.error_code =te.error_code
where te.d >= '${pre1_date}' and te.d<= DATE_ADD(current_date(),-1)
and te.error_module='robot' and te.error_level>=3
and (
(te.error_start_time >= '${pre1_date}' and te.error_start_time < current_date() and coalesce(te.error_end_time, current_timestamp) < current_date()) 
or(te.error_start_time >= '${pre1_date}' and te.error_start_time < current_date() and coalesce(te.error_end_time, current_timestamp) >= current_date())
or(te.error_start_time < '${pre1_date}' and coalesce(te.error_end_time,current_timestamp) >= current_date() and coalesce(te.error_end_time, current_timestamp) < current_date()) 
or(te.error_start_time < '${pre1_date}' and coalesce(te.error_end_time, current_timestamp) >= current_date())
)
),
-- 搬运作业单机器人任务明细
phx_create_order_job_detail as (
select 
t1.project_code,
t1.order_create_time,
t1.order_no,
t2.job_create_time,
t2.job_sn,
t2.robot_code,
tr.robot_type_code,
tr.robot_type_name 
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t1 
left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di t2 on t2.pt=t1.pt and t2.order_id =t1.id and t2.d>='${pre1_date}'
left join  ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr on tr.pt=t2.pt and tr.d=t2.d and tr.robot_code=t2.robot_code and tr.d>='${pre1_date}'
where t1.d>='${pre1_date}'
),
-- 机器人小时列表
phx_hour_robot_list_detail  as (
select 
tt.cur_week, -- 统计星期
tt.cur_date, -- 统计日期
tt.cur_hour, -- 统计小时
tt.project_code, -- 项目编码
trl.robot_code,  -- 机器人编码
trl.robot_type_code,  -- 机器人类型编码
trl.robot_type_name    -- 机器人类型名称
-- 时间维度信息
from 
(select 
t1.cur_week, -- 统计星期
t1.cur_date, -- 统计日期
t1.cur_hour, -- 统计小时
thp.project_code -- 项目编码
from 
(SELECT 
WEEKOFYEAR(t1.days) as cur_week,
t1.days as cur_date,
date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
FROM ${dim_dbname}.dim_day_date t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
WHERE t1.days >= '${pre1_date}' AND t1.days <= DATE_ADD(current_date(),-1))t1 
left join 
(SELECT project_code
FROM ${dim_dbname}.dim_collection_project_record_ful
where project_version like '3%')thp)tt  
-- 机器人信息
join 
(select 
tr.d as cur_date,
tr.project_code,
tr.robot_code,
tr.robot_type_code,
tr.robot_type_name 
from ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr 
inner join 
(SELECT project_code
FROM ${dim_dbname}.dim_collection_project_record_ful
where project_version like '3%')thp on thp.project_code =tr.project_code 
where tr.d>='${pre1_date}' and tr.d<= DATE_ADD(current_date(),-1)
and tr.robot_usage_state ='using')trl on trl.project_code=tt.project_code and trl.cur_date=tt.cur_date
),
-- 机器人小时内理论运行时长（秒）
phx_robot_hour_theory_run_duration as (
select 
ts.project_code,
ts.cur_date,
ts.cur_hour,
ts.robot_code,
ts.robot_type_code,  -- 机器人类型编码
ts.robot_type_name,    -- 机器人类型名称
COALESCE(sum(ts.state_keep_duration),0)+COALESCE(sum(ts.before_state_keep_duration),0) as theory_run_duration  -- 理论运行时长（秒）
from 
(select 
t.project_code,
t.cur_date,
t.cur_hour,
t.robot_code,
t.robot_type_code,  -- 机器人类型编码
t.robot_type_name,    -- 机器人类型名称
t.id           as                           state_id,
t.create_time     as                           state_create_time,
t.next_state_create_time,
t.network_state,
t.online_state,
t.work_state,
t.job_sn,
t.is_error,
t.duration / 1000 as                           duration,
case when t.desc_rk=1 then (unix_timestamp(t.cur_hour)+3600)-unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss')) 
else unix_timestamp(from_unixtime(unix_timestamp(t.next_state_create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss'))  end as state_keep_duration,  
t.asc_rk,
t.desc_rk,
tf.online_state as before_online_state,
tf.work_state as before_work_state,
tf.is_error as before_is_error,
case when t.asc_rk=1 and  t.id is null then 3600 when t.asc_rk=1 and t.id is not null then unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(t.cur_hour) end as before_state_keep_duration  -- 小时前最后一条状态在本小时内持续时长
from 
(select 
hrl.project_code,
hrl.cur_date,
hrl.cur_hour,
hrl.robot_code,
hrl.robot_type_code,  -- 机器人类型编码
hrl.robot_type_name,    -- 机器人类型名称
trs.id,
trs.create_time,
trs.network_state,
trs.online_state,
trs.work_state,
trs.job_sn,
trs.is_error,
trs.duration,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time asc) as asc_rk,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time desc) as desc_rk,
lead(trs.create_time,1) over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time asc) as next_state_create_time
from phx_hour_robot_list_detail hrl 
left join ${dwd_dbname}.dwd_phx_rms_robot_state_info_di trs on trs.d>=DATE_ADD('${pre1_date}',-1) and trs.pt=hrl.project_code and trs.robot_code=hrl.robot_code and date_format(trs.create_time,'yyyy-MM-dd HH:00:00')=hrl.cur_hour
-- where hrl.robot_code='qilin31_51'
)t
-- 找到每个小时之前的第一条数据
left join 
(select 
hrl.project_code,
hrl.cur_date,
hrl.cur_hour,
hrl.robot_code,
trs.id,
trs.create_time,
trs.network_state,
trs.online_state,
trs.work_state,
trs.job_sn,
trs.is_error,
trs.duration,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time desc) as rk 
from phx_hour_robot_list_detail hrl 
left join ${dwd_dbname}.dwd_phx_rms_robot_state_info_di trs on trs.d>=DATE_ADD('${pre1_date}',-30) and trs.pt=hrl.project_code and trs.robot_code=hrl.robot_code and trs.create_time<hrl.cur_hour
-- where hrl.robot_code='qilin31_51'
)tf on tf.rk=1 and tf.project_code=t.project_code and tf.robot_code=t.robot_code and tf.cur_date=t.cur_date and tf.cur_hour=t.cur_hour
)ts
where (ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1)or(ts.before_online_state = 'REGISTERED' or ts.before_work_state = 'ERROR' or ts.before_is_error = 1)
group by ts.project_code,ts.cur_date,ts.cur_hour,ts.robot_code,ts.robot_type_code,ts.robot_type_name
),
-- 机器人每小时故障时长
phx_hour_robot_error_duration as (
select 
t.cur_week,
t.cur_date,
t.cur_hour,
t.cur_next_hour,
t.project_code,
t.robot_code,
t.robot_type_code,
t.robot_type_name,
concat_ws(',' , collect_set(cast(t.id as string))) as error_id_list, -- 故障编码
count(distinct t.id) as error_num,
sum(unix_timestamp(from_unixtime(unix_timestamp(t.stat_end_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(t.stat_start_time),'yyyy-MM-dd HH:mm:ss'))) as error_duration  -- 该小时故障时长
from 
(select 
t1.cur_week, -- 统计星期
t1.cur_date, -- 统计日期
t1.cur_hour, -- 统计小时
from_unixtime(unix_timestamp(t1.cur_hour)+3600) as cur_next_hour,  -- 统计小时的下一个小时
te.project_code,
te.id,
te.robot_code,
te.robot_type_code,
te.robot_type_name,
te.error_start_time,
te.error_end_time,
case when te.error_start_time<t1.cur_hour then t1.cur_hour else te.error_start_time end as stat_start_time,
case when te.error_end_time is null then from_unixtime(unix_timestamp(t1.cur_hour)+3600) when te.error_end_time>=from_unixtime(unix_timestamp(t1.cur_hour)+3600) then from_unixtime(unix_timestamp(t1.cur_hour)+3600) else te.error_end_time end as stat_end_time
from 
(SELECT 
WEEKOFYEAR(t1.days) as cur_week,
t1.days as cur_date,
date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
FROM ${dim_dbname}.dim_day_date t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
WHERE t1.days >= '${pre1_date}' AND t1.days <= DATE_ADD(current_date(),-1))t1 
inner join phx_robot_error_detail te on 
((te.error_start_time >= t1.cur_hour and te.error_start_time < from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) < from_unixtime(unix_timestamp(t1.cur_hour)+3600)) 
or(te.error_start_time >= t1.cur_hour and te.error_start_time < from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600))
or(te.error_start_time < t1.cur_hour and coalesce(te.error_end_time,current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) < from_unixtime(unix_timestamp(t1.cur_hour)+3600)) 
or(te.error_start_time < t1.cur_hour and coalesce(te.error_end_time, current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600))
)
)t
group by t.cur_week,t.cur_date,t.cur_hour,t.cur_next_hour,t.project_code,t.robot_code,t.robot_type_code,t.robot_type_name
)




----------------------------------------------------------------------------------
-- INSERT overwrite table ${ads_dbname}.ads_amr_breakdown partition(d,pt)

-- 分时分小车
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'single' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
rl.robot_code as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt


from phx_hour_robot_list_detail rl 
left join 
(select 
project_code,
robot_code,
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by project_code,robot_code,to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00')
)tor on tor.project_code=rl.project_code and tor.robot_code=rl.robot_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour
left join phx_hour_robot_error_duration ted  on ted.project_code=rl.project_code and ted.robot_code=rl.robot_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour
left join phx_robot_hour_theory_run_duration trd on trd.project_code=rl.project_code and trd.robot_code=rl.robot_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour
left join 
(select 
project_code,
robot_code,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,robot_code,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee on tee.project_code=rl.project_code and tee.robot_code=rl.robot_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour
left join 
(select 
project_code,
robot_code,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,robot_code,to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce on tce.project_code=rl.project_code and tce.robot_code=rl.robot_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour
left join 
(select 
t.project_code,
t.robot_code,
t.robot_type_code,	   
t.robot_type_name,
t.cur_hour,
add_theory_time,
add_error_duration,
add_error_num,
d,
pt
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}')tadd on tadd.project_code=rl.project_code and tadd.robot_code=rl.robot_code and tadd.cur_hour=rl.cur_hour


UNION ALL 
-- 分时分类型
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'part' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select  
cur_date,
cur_hour,
project_code,
robot_type_code,
robot_type_name 
from phx_hour_robot_list_detail
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name) rl 
left join 
(select 
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
project_code,
robot_type_code,
robot_type_name, 
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00'),project_code,robot_type_code,robot_type_name
)tor on tor.project_code=rl.project_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour and tor.robot_type_code=rl.robot_type_code and tor.robot_type_name=rl.robot_type_name
left join 
(select cur_date,cur_hour,project_code,robot_type_code,robot_type_name,
concat_ws(',' , collect_set(cast(error_id_list as string))) as error_id_list,
sum(error_duration) as error_duration
from phx_hour_robot_error_duration
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name) ted  
on ted.project_code=rl.project_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour and ted.robot_type_code=rl.robot_type_code and ted.robot_type_name=rl.robot_type_name
left join 
(select cur_date,cur_hour,project_code,robot_type_code,robot_type_name,
sum(theory_run_duration) as theory_run_duration
from phx_robot_hour_theory_run_duration
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name 
)trd on trd.project_code=rl.project_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour and trd.robot_type_code=rl.robot_type_code and trd.robot_type_name=rl.robot_type_name
left join 
(select 
project_code,
robot_type_code,
robot_type_name,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,robot_type_code,robot_type_name,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee 
on tee.project_code=rl.project_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour and tee.robot_type_code=rl.robot_type_code and tee.robot_type_name=rl.robot_type_name
left join 
(select 
project_code,
robot_type_code,
robot_type_name,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,robot_type_code,robot_type_name,
to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce 
on tce.project_code=rl.project_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour and tce.robot_type_code=rl.robot_type_code and tce.robot_type_name=rl.robot_type_name
left join 
(select 
t.project_code,
t.cur_hour,
t.robot_type_code,	   
t.robot_type_name,
sum(add_theory_time) as add_theory_time,
sum(add_error_duration) as add_error_duration,
sum(add_error_num) as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
group by t.project_code,t.cur_hour,t.robot_type_code,t.robot_type_name
)tadd on tadd.project_code=rl.project_code and tadd.cur_hour=rl.cur_hour and tadd.robot_type_code=rl.robot_type_code and tadd.robot_type_name=rl.robot_type_name 


UNION ALL 
-- 分时分项目
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'all' as type_class, -- 数据类型
null as amr_type,  -- 机器人类型编码
null as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select  
cur_date,
cur_hour,
project_code
from phx_hour_robot_list_detail
group by cur_date,cur_hour,project_code) rl 
left join 
(select 
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
project_code,
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00'),project_code
)tor on tor.project_code=rl.project_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour 
left join 
(select cur_date,cur_hour,project_code,
concat_ws(',' , collect_set(cast(error_id_list as string))) as error_id_list,
sum(error_duration) as error_duration
from phx_hour_robot_error_duration
group by cur_date,cur_hour,project_code) ted  
on ted.project_code=rl.project_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour 
left join 
(select cur_date,cur_hour,project_code,
sum(theory_run_duration) as theory_run_duration
from phx_robot_hour_theory_run_duration
group by cur_date,cur_hour,project_code
)trd on trd.project_code=rl.project_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour 
left join 
(select 
project_code,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee 
on tee.project_code=rl.project_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour 
left join 
(select 
project_code,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce 
on tce.project_code=rl.project_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour 
left join 
(select 
t.project_code,
t.cur_hour,
sum(add_theory_time) as add_theory_time,
sum(add_error_duration) as add_error_duration,
sum(add_error_num) as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
group by t.project_code,t.cur_hour
)tadd on tadd.project_code=rl.project_code and tadd.cur_hour=rl.cur_hour
