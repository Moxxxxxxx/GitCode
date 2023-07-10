dature.flashhold.com 账号密码:yangping/YP@QuickTron
-------------------------------------------------
key points:
1 整体系统生产可用性:以95%开始,每周增长1%直至99%
公式:(总运行时长-总停止时长)/总运行时长 

2 月度单个机器人使用率:月度单个机器人的运行时长/月度生产时长 


----------------------------

代超超在群里说,1月2号之前不生产,只统计次数不够详细,具体有哪些错误,哪些错误次数比较多,哪些小车报错多,现场好针对性的进行改善


----------------------------------------------------------------------------------
project_name='PSA  Sochaux Stellantis overall project'
project_code ='A51488'

select * from collection_offline.collection_project_record_info where project_code ='A51488';

select * from evo_basic.basic_bucket where project_code ='A51488';
select * from evo_basic.basic_bucket_type where project_code ='A51488';
select * from evo_basic.basic_slot where project_code ='A51488';
select * from evo_basic.basic_slot_type where project_code ='A51488';
select * from evo_basic.basic_station where project_code ='A51488';
select * from evo_basic.basic_station_point where project_code ='A51488';
select * from evo_basic.notification_message where project_code ='A51488';

select * from evo_rcs.agv_charger_bind  where project_code ='A51488';
select * from evo_rcs.agv_job  where project_code ='A51488';   -- 热数据
select * from evo_rcs.agv_job_event_notification  where project_code ='A51488';   -- job_type:BUCKET_MOVE_JOB CANCEL_JOB CHARGE_JOB IDLE_PARKING_MOVE MOVE_JOB NO_VALID_BUCKET_MOVE_JOB SHUTDOWN_JOB
select * from evo_rcs.agv_job_history  where project_code ='A51488';
select * from evo_rcs.agv_job_sub  where project_code ='A51488';
select * from evo_rcs.agv_loading_history  where project_code ='A51488';     -- cause:CLEAR LOAD TOP_FACE_CHANGED
select * from evo_rcs.agv_loading_state  where project_code ='A51488';  -- 热数据
select * from evo_rcs.agv_pd_status  where project_code ='A51488';   -- N
select * from evo_rcs.base_charger  where project_code ='A51488';
select * from evo_rcs.basic_agv  where project_code ='A51488';
select * from evo_rcs.basic_agv_appearance  where project_code ='A51488';
select * from evo_rcs.basic_agv_part  where project_code ='A51488';
select * from evo_rcs.basic_agv_type  where project_code ='A51488';
select * from evo_rcs.basic_roller_part where project_code ='A51488';
select * from evo_rcs.dsp_system_config  where project_code ='A51488';
select * from evo_rcs.rcs_dsp_error_dict  where project_code ='A51488';  -- N
select * from evo_rcs.rcs_agv_path_plan  where project_code ='A51488';   -- N
select * from evo_rcs.rcs_scan_code_record  where project_code ='A51488';   -- N

select * from notification.notification_message  where project_code ='A51488';    -- N

select * from evo_wcs_g2p.basic_agv_point where project_code ='A51488';  -- N
select * from evo_wcs_g2p.bucket_convey_detail where project_code ='A51488';  -- N
select * from evo_wcs_g2p.bucket_convey_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.bucket_convey_work where project_code ='A51488';   -- N
select * from evo_wcs_g2p.bucket_move_action where project_code ='A51488';    -- N
select * from evo_wcs_g2p.bucket_move_job where project_code ='A51488';   -- 货架移动
select * from evo_wcs_g2p.bucket_point  where project_code ='A51488';   -- N
select * from evo_wcs_g2p.bucket_robot_job where project_code ='A51488';   -- 标准搬运任务
select * from evo_wcs_g2p.bucket_runtime where project_code ='A51488';  -- N
select * from evo_wcs_g2p.container_move_job_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.container_take_down_job_v2  where project_code ='A51488';  -- N
select * from evo_wcs_g2p.container_transfer_job_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.countcheck_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.countcheck_work_detail where project_code ='A51488'; -- N
select * from evo_wcs_g2p.fork_move_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.guided_put_away_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.guided_putaway_work where project_code ='A51488'; -- N
select * from evo_wcs_g2p.guided_putaway_work_detail where project_code ='A51488'; -- N
select * from evo_wcs_g2p.job_state_change where project_code ='A51488';  -- 任务状态变更记录
select * from evo_wcs_g2p.order_group where project_code ='A51488'; -- N
select * from evo_wcs_g2p.picking_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.picking_work where project_code ='A51488'; -- N
select * from evo_wcs_g2p.picking_work_detail where project_code ='A51488'; -- N
select * from evo_wcs_g2p.putaway_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.putaway_work where project_code ='A51488'; -- N
select * from evo_wcs_g2p.putaway_work_detail where project_code ='A51488'; -- N
select * from evo_wcs_g2p.reprint_move_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.robot_job_detail where project_code ='A51488';    --  job的state变化
select * from evo_wcs_g2p.roller_assign_record where project_code ='A51488'; -- N
select * from evo_wcs_g2p.roller_job_extends where project_code ='A51488'; -- N
select * from evo_wcs_g2p.roller_move_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.roller_sub_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.si_qp_extend where project_code ='A51488';  -- N
select * from evo_wcs_g2p.si_qp_move_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.si_qp_transfer_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.smallparcel_move_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.station_task_group where project_code ='A51488'; -- N
select * from evo_wcs_g2p.tally_picking_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.tally_putaway_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.tally_work where project_code ='A51488';  -- N
select * from evo_wcs_g2p.tally_work_detail where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_job_v2 where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_work where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_work_detail where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_work_detail_v2 where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_countcheck_work_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.w2p_guided_put_away_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_guided_putaway_work_detail where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_picking_job_v2 where project_code ='A51488';  -- N
select * from evo_wcs_g2p.w2p_picking_work_detail_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.w2p_picking_work_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.w2p_putaway_job_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.w2p_putaway_work_v2 where project_code ='A51488'; -- N
select * from evo_wcs_g2p.work_binding_station where project_code ='A51488';  -- N
select * from evo_wcs_g2p.workbin_assign_record where project_code ='A51488';  -- N
select * from evo_wcs_g2p.workbin_bucket where project_code ='A51488';  -- N
select * from evo_wcs_g2p.workbin_bucket_move_job where project_code ='A51488';  -- N
select * from evo_wcs_g2p.workbin_job_extends where project_code ='A51488';  -- N
select * from evo_wcs_g2p.workbin_move_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.workbin_put_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.workbin_standard_move_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.workbin_take_down_job where project_code ='A51488'; -- N
select * from evo_wcs_g2p.workbin_take_job where project_code ='A51488';  -- N

-----------------------------------------------------------------------------------------

select * from dim.dim_collection_project_record_ful where project_code ='A51488';
-----------------------------------------------------------------------------------------
select d,count(0) 
from ods.ods_agv_breakdown_detail_dt
where project_code ='A51488'
group by d
order by d 

2022-12-14	980
2022-12-16	980
2022-12-29	39
2023-01-03	1658
2023-01-04	2431
2023-01-05	2821


-----------------------------------------------------------------------------------------
SELECT date(happen_at) as date_value,count(0)
FROM evo_basic.notification_message m
WHERE project_code ='A51488'
group by date(happen_at)
order by date(happen_at)

2023-01-03	3379
2023-01-04	3859
2023-01-05	3596
2023-01-06	3285

--------

select warning_type,title ,count(0) 
FROM evo_basic.notification_message m
WHERE project_code ='A51488'
group by warning_type,title 

ROBOT	RCS_OnlineErr_RbtErrNotFoundPointUseLast	5
ROBOT	RCS_RbtErr_BucketMismatch	4
ROBOT	RCS_RbtErr_chargeFailed_BatteryNotIncrement	1
ROBOT	RCS_RbtErr_chargeFailed_Charge_Station_Exception	1
ROBOT	RCS_RbtErr_chargeFailed_DSP_ERROR	34
ROBOT	RCS_RbtErr_chargeFailed_JOB_FAILED	68
ROBOT	RCS_RbtErr_NotOnCode	2753
ROBOT	RCS_RbtErr_UNKONW	3111
ROBOT	RCS_RbtErr_Warning_DetectBarrier	3767
ROBOT	RCS_RbtErr_Warning_LowPower	282

SYSTEM	RCS_JobErr_Job_ExecuteFail	2793
SYSTEM	RCS_OnlineErr_LockpointRequestFailed	122
SYSTEM	RCS_TrafficErr_BlockedByErrorBucket	37
SYSTEM	RCS_TrafficErr_BlockedByErrorLockpoint	1004
SYSTEM	RCS_TrafficErr_BlockedByHumanLockpoint	4
SYSTEM	RCS_TrafficErr_PathPlanFailed	133


-----------------------------------------------------------------------------------------

select d,count(0) as num
from dwd.dwd_notification_message_info_di
where pt ='A51488'
group by d
order by d

2023-01-02	450
2023-01-03	3379
2023-01-04	3859
2023-01-05	3596
2023-01-06	3285
2023-01-09	2617


select d,warning_type,count(0) as num
from dwd.dwd_notification_message_info_di
where pt ='A51488'
group by d,warning_type
order by warning_type,d

2023-01-02	ROBOT	298
2023-01-03	ROBOT	2344
2023-01-04	ROBOT	2697
2023-01-05	ROBOT	2575
2023-01-06	ROBOT	2410
2023-01-02	SYSTEM	152
2023-01-03	SYSTEM	1035
2023-01-04	SYSTEM	1162
2023-01-05	SYSTEM	1021
2023-01-06	SYSTEM	875



select warning_type,message_title,notify_level,count(0)  
from dwd.dwd_notification_message_info_di
where pt ='A51488'
group by warning_type,message_title,notify_level
order by warning_type,notify_level  



ROBOT	RCS_RbtErr_Warning_DetectBarrier	1	4793
ROBOT	RCS_RbtErr_Warning_LowPower	1	416

ROBOT	RCS_RbtErr_chargeFailed_JOB_FAILED	2	87
ROBOT	RCS_RbtErr_BucketMismatch	2	4
ROBOT	RCS_OnlineErr_RbtErrNotFoundPointUseLast	2	6
ROBOT	RCS_RbtErr_chargeFailed_BatteryNotIncrement	2	1
ROBOT	RCS_RbtErr_chargeFailed_Charge_Station_Exception	2	1
ROBOT	RCS_RbtErr_NotOnCode	2	3237
ROBOT	RCS_RbtErr_UNKONW	2	3757
ROBOT	RCS_RbtErr_chargeFailed_DSP_ERROR	2	36

SYSTEM	RCS_TrafficErr_BlockedByHumanLockpoint	1	4
SYSTEM	RCS_TrafficErr_BlockedByErrorBucket	1	48
SYSTEM	RCS_TrafficErr_BlockedByErrorLockpoint	1	1161

SYSTEM	RCS_JobErr_Job_ExecuteFail	2	3326
SYSTEM	RCS_TrafficErr_PathPlanFailed	2	163
SYSTEM	RCS_OnlineErr_LockpointRequestFailed	2	146



-----------------------------------------------------------------------------------------
select d,count(0) 
from dwd.dwd_agv_breakdown_detail_incre_dt 
where pt='A51488'
group by d 
order by d

2022-12-14	980
2022-12-27	39
2023-01-01	246
2023-01-02	773
2023-01-03	2821
2023-01-04	5339
2023-01-05	4984
2023-01-06	4265


-----------------------------------------------------------------------------------------
select error_level,count(0)
from dwd.dwd_agv_breakdown_detail_incre_dt 
where pt='A51488'
and d>='2023-01-03'
group by error_level


1	761
	15619
4	34
3	995


-----------------------------------------------------------------------------------------
select 
d,count(0)
from dwd.dwd_agv_breakdown_astringe_v4_di 
where pt ='A51488'
group by d
order by d

2022-12-14	50
2023-01-03	180
2023-01-04	188
2023-01-05	187
2023-01-06	178



-----------------------------------------------------------

-- 从job的开始执行时间看是24小时都有在工作
select 
date(job_execute_time) as date_value,
DATE_FORMAT(job_execute_time, '%Y-%m-%d %H:00:00') as hour_value,
count(0) as num,
min(job_execute_time) as first_job_execute_time,
max(job_execute_time) as last_job_execute_time
from evo_rcs.agv_job_history  where project_code ='A51488'
group by 1,2
order by hour_value 
-------------------------------------------------------------------


--秒粒度切割
select 
tmp.*,
from_unixtime(unix_timestamp(start_time) + pos) as mid_second
from
    (
    select
        '1' as uid,
        '2020-01-01 10:00:00' as start_time,
        '2020-01-01 10:00:12' as end_time
    ) tmp lateral view posexplode(split(space(cast(unix_timestamp(end_time) - unix_timestamp(start_time) as int)),''))t as pos,val 
	
---------------------------------------------------




select d,count(0) 
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
group by d

2023-01-02	1979
2023-01-03	10476
2023-01-04	13749
2023-01-05	14762
2023-01-06	14400
2023-01-07	6
2023-01-09	11655
2023-01-10	5


select job_type,count(0) 
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
group by job_type

NO_VALID_BUCKET_MOVE_JOB	6231
BUCKET_MOVE_JOB	37196
MOVE_JOB	12667
IDLE_PARKING_MOVE	5694
CHARGE_JOB	1672
SHUTDOWN_JOB	1
DEAD_LOCK_MOVE_JOB	3571



select 
d,count(0)
from dwd.dwd_rcs_agv_job_event_notification_info_di
where pt='A51488'
group by d 
order by d

2023-01-01	1824
2023-01-02	3970
2023-01-03	48291
2023-01-07	8199
2023-01-08	3030
2023-01-09	52853
2023-01-10	1444


select 
job_type,count(0)
from dwd.dwd_rcs_agv_job_event_notification_info_di
where pt='A51488'
group by job_type

CANCEL_JOB	1
NO_VALID_BUCKET_MOVE_JOB	15434
BUCKET_MOVE_JOB	89687
MOVE_JOB	9787
IDLE_PARKING_MOVE	3199
CHARGE_JOB	1503	

----------------------------------------------------------------------
select 
job_type,
min(duration1) as min_duration1,
max(duration1) as max_duration1,
min(duration2) as min_duration2,
max(duration2) as max_duration2
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
unix_timestamp(job_execute_time)-unix_timestamp(job_accept_time) as duration1,
unix_timestamp(job_finish_time)-unix_timestamp(job_execute_time) as duration2 
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
--and to_date(job_finish_time) != to_date(job_execute_time)
order by duration2 desc)t
group by job_type


NO_VALID_BUCKET_MOVE_JOB	0	82	0	3980
BUCKET_MOVE_JOB	0	355	0	4410
MOVE_JOB	0	16	0	4021
IDLE_PARKING_MOVE	0	689	0	28405
CHARGE_JOB	0	1	0	28345
SHUTDOWN_JOB	0	0	1	1
DEAD_LOCK_MOVE_JOB	0	1	0	13765


--------------------
 SELECT WEEKOFYEAR(t1.days) as cur_week,
           t1.days as cur_date,
           date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
    FROM ${dim_dbname}.dim_day_date t1
    LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
    WHERE t1.days = '${pre1_date}' --t1.days >= '${pre1_date}'
    
    
    
select  t1.days as cur_date
from dim.dim_day_date t1 
where t1.days>='2023-01-03' and t1.days <='2023-01-10'


select  d,agv_code 
from dwd.dwd_rcs_agv_base_info_df
where pt='A51488' and d>='2023-01-03' and d<='2023-01-10'



select  d,count(distinct agv_code) 
from dwd.dwd_rcs_agv_base_info_df
where pt='A51488' and d>='2023-01-03' and d<='2023-01-10'
group by d 


-----------------

select 
job_type,
min(duration1) as min_duration1,
max(duration1) as max_duration1,
min(duration2) as min_duration2,
max(duration2) as max_duration2,
min(duration3) as min_duration3,
max(duration3) as max_duration3
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
unix_timestamp(job_execute_time)-unix_timestamp(job_accept_time) as duration1,
unix_timestamp(job_finish_time)-unix_timestamp(job_execute_time) as duration2,
unix_timestamp(job_finish_time)-unix_timestamp(job_accept_time) as duration3
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
--and to_date(job_finish_time) != to_date(job_accept_time)
-- order by duration3 desc
)t
group by job_type

SHUTDOWN_JOB	0	0	1	1	1	1
NO_VALID_BUCKET_MOVE_JOB	0	82	0	3980	12	3980
MOVE_JOB	0	16	0	4021	0	4021
BUCKET_MOVE_JOB	0	355	0	4410	1	4410
DEAD_LOCK_MOVE_JOB	0	1	0	13765	0	13765
CHARGE_JOB	0	1	0	28345	0	28345
IDLE_PARKING_MOVE	0	689	0	28405	0	28405


----------------------------------------------------------

select 
agv_code,
to_date(job_accept_time) as date_value, 
sum(duration3) as usage_duration
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
unix_timestamp(job_execute_time)-unix_timestamp(job_accept_time) as duration1,
unix_timestamp(job_finish_time)-unix_timestamp(job_execute_time) as duration2,
unix_timestamp(job_finish_time)-unix_timestamp(job_accept_time) as duration3
from dwd.dwd_rcs_agv_job_history_info_di 
where pt='A51488'
and to_date(job_finish_time) = to_date(job_accept_time)
)t 
group by agv_code,agv_code,to_date(job_accept_time)



------------------------
select 
t1.pt,
t1.agv_code,
t1.error_code,
t1.error_code_list,
t1.message_body,
t1.message_id,
COALESCE (t2.error_cname,'未维护') as error_cname
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
where t1.error_code_position=0