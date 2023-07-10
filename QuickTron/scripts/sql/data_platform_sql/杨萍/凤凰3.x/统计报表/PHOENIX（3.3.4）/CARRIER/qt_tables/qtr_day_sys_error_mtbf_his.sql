-- part1：mysql逻辑



-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/t2.error_num else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/t3.accum_error_num else null end as decimal(20,10)) as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统天内理论运行时长、故障时长（时间段上去重）
left join 
(select 
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where hour_start_time>= @dt_day_start_time and hour_start_time <  @dt_next_day_start_time
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统天内参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value >= @dt_day_start_time and date_value <  @dt_next_day_start_time
group by alarm_service
WITH rollup)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value < @dt_next_day_start_time
group by alarm_service
WITH rollup)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join  
(select alarm_service,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该天之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该天之前累计故障时长
from qt_smartreport.qtr_day_sys_error_mtbf_his
where date_value < @dt_day_start_time
group by alarm_service)t4 on t4.alarm_service=ts.alarm_service	




-- part2：sqlserver逻辑

-- sqlserver时间参数
declare @now_time as datetime=sysdatetime() 
declare @dt_hour_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd HH:00:00')
declare @dt_next_hour_start_time as datetime=FORMAT(DATEADD(hh,1,sysdatetime()),'yyyy-MM-dd HH:00:00')
declare @dt_day_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd 00:00:00')
declare @dt_next_day_start_time as datetime=FORMAT(DATEADD(dd,1,sysdatetime()),'yyyy-MM-dd 00:00:00')
declare @dt_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),0),'yyyy-MM-dd 00:00:00')
declare @dt_next_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),7),'yyyy-MM-dd 00:00:00')

select 
@now_time as create_time,
@now_time as update_time,
FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd') as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/cast(t2.error_num as decimal) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/cast(t3.accum_error_num as decimal) else null end as decimal(20,10)) as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.dbo.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统天内理论运行时长、故障时长（时间段上去重）
left join 
(select 
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.dbo.qtr_hour_sys_error_mtbf_his
where hour_start_time>= @dt_day_start_time and hour_start_time <  @dt_next_day_start_time
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统天内参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.dbo.qtr_day_sys_error_list_his
where date_value >= @dt_day_start_time and date_value <  @dt_next_day_start_time
group by alarm_service
WITH rollup)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.dbo.qtr_day_sys_error_list_his
where date_value < @dt_next_day_start_time
group by alarm_service
WITH rollup)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join  
(select alarm_service,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该天之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该天之前累计故障时长
from qt_smartreport.dbo.qtr_day_sys_error_mtbf_his
where date_value < @dt_day_start_time
group by alarm_service)t4 on t4.alarm_service=ts.alarm_service	




-- part3：异步表兼容逻辑

-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的下一天的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_day_start_time }}) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/t2.error_num else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/t3.accum_error_num else null end as decimal(20,10)) as accum_mtbf
from
-- 参与计算的所有系统
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统天内理论运行时长、故障时长（时间段上去重）
left join
(select
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where hour_start_time>= {{ dt_day_start_time }} and hour_start_time <  {{ dt_next_day_start_time }}
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统天内参与计算的故障数
left join
(select
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value >= {{ dt_day_start_time }} and date_value <  {{ dt_next_day_start_time }}
group by alarm_service
WITH rollup)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join
(select
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value < {{ dt_next_day_start_time }}
group by alarm_service
WITH rollup)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join
(select alarm_service,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该天之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该天之前累计故障时长
from qt_smartreport.qtr_day_sys_error_mtbf_his
where date_value < {{ dt_day_start_time }}
group by alarm_service)t4 on t4.alarm_service=ts.alarm_service
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
cast(case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/cast(t2.error_num as decimal) else null end as decimal(20,10)) as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
cast(case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/cast(t3.accum_error_num as decimal) else null end as decimal(20,10)) as accum_mtbf
from
-- 参与计算的所有系统
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统天内理论运行时长、故障时长（时间段上去重）
left join
(select
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where hour_start_time>= {{ dt_day_start_time }} and hour_start_time <  {{ dt_next_day_start_time }}
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统天内参与计算的故障数
left join
(select
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value >= {{ dt_day_start_time }} and date_value <  {{ dt_next_day_start_time }}
group by alarm_service
WITH rollup)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join
(select
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value < {{ dt_next_day_start_time }}
group by alarm_service
WITH rollup)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join
(select alarm_service,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该天之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该天之前累计故障时长
from qt_smartreport.qtr_day_sys_error_mtbf_his
where date_value < {{ dt_day_start_time }}
group by alarm_service)t4 on t4.alarm_service=ts.alarm_service
{% endif %}