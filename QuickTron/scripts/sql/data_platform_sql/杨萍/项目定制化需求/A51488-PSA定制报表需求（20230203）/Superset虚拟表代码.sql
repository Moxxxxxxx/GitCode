select 
date_format(date_add(happen_time,interval -5 hour), '%Y-%m-%d') as work_date_value
,case when HOUR(date_add(happen_time,interval -5 hour)) in (0,1,2,3,4,5,6,7) then '早班' when HOUR(date_add(happen_time,interval -5 hour)) in (8,9,10,11,12,13,14,15) then '中班' when HOUR(date_add(happen_time,interval -5 hour)) in (16,17,18,19,20,21,22,23) then '晚班' else null end as work_shifts
,min(date_format(happen_time, '%Y-%m-%d %H:00:00')) as work_shifts_time_frame_start
,date_add(min(date_format(happen_time, '%Y-%m-%d %H:59:59')),interval 7 hour) as work_shifts_time_frame_end 
,concat(min(date_format(happen_time, '%Y-%m-%d %H:00')),'~',date_format(date_add(min(date_format(happen_time, '%Y-%m-%d %H:00:00')),interval 7 hour), '%Y-%m-%d %H:59')) as work_shifts_time_frame
,coalesce(count(distinct breakdown_id_single),0) as breakdown_num
,coalesce(sum(case when breakdown_id_sort=1 then carry_order_num end),0) as order_num
,coalesce(sum(case when breakdown_id_sort=1 then carry_task_num end),0) as task_num
,coalesce(coalesce(count(distinct breakdown_id_single),0)/coalesce(sum(case when breakdown_id_sort=1 then carry_order_num end),1),0) as order_breakdown_rate
,coalesce(coalesce(count(distinct breakdown_id_single),0)/coalesce(sum(case when breakdown_id_sort=1 then carry_task_num end),1),0) as task_breakdown_rate
,coalesce((coalesce(sum(case when breakdown_id_sort=1 then theory_time end),0) - coalesce(sum(case when breakdown_id_sort=1 then error_duration end),0))/coalesce(sum(case when breakdown_id_sort=1 then theory_time end),0),0) as oee 
,(coalesce(sum(case when breakdown_id_sort=1 then theory_time end),0) - coalesce(sum(case when breakdown_id_sort=1 then error_duration end),0))/count(distinct breakdown_id_single) as mtbf
,coalesce(sum(case when breakdown_id_sort=1 then mttr_error_duration end),0)/coalesce(sum(case when breakdown_id_sort=1 then mttr_error_num end),0) as mttr 
,coalesce(sum(case when breakdown_id_sort=1 then theory_time end),0) as theory_duration
,coalesce(sum(case when breakdown_id_sort=1 then error_duration end),0) as error_duration
,coalesce(sum(case when breakdown_id_sort=1 then mttr_error_duration end),0) as mttr_error_duration
,coalesce(sum(case when breakdown_id_sort=1 then mttr_error_num end),0) as mttr_error_num
,max(add_mtbf) as add_mtbf
from evo_wds_base.ads_amr_breakdown
where project_code ='A51488'
and type_class='all'
and timestampdiff(month,date_format(date_add(happen_time,interval -5 hour), '%Y-%m-%d'),current_date())<3 
group by work_date_value,work_shifts