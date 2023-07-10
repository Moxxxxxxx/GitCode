-- 快仓全部项目list

select * from dwd.dwd_pms_share_project_base_info_df 
where d='2023-02-05'  -- 快仓全部项目
and project_name REGEXP 'PSA|一汽大众'

-- FH-B2022-B111 ：一汽大众佛山线边搬运项目
-- A51488 ：PSA  Sochaux Stellantis overall project




select 
date_format(date_add(happen_time,interval -5 hour), '%Y-%m-%d') as work_date_value
,case when HOUR(date_add(happen_time,interval -5 hour)) in (0,1,2,3,4,5,6,7) then '早班' when HOUR(date_add(happen_time,interval -5 hour)) in (8,9,10,11,12,13,14,15) then '中班' when HOUR(date_add(happen_time,interval -5 hour)) in (16,17,18,19,20,21,22,23) then '晚班' else null end as work_shifts
,min(date_format(happen_time, '%Y-%m-%d %H:00:00')) as work_shifts_time_frame_start
,date_add(min(date_format(happen_time, '%Y-%m-%d %H:00:00')),interval 8 hour) as work_shifts_time_frame_end 
,concat(min(date_format(happen_time, '%Y-%m-%d %H:00')),'~',date_format(date_add(min(date_format(happen_time, '%Y-%m-%d %H:00:00')),interval 8 hour), '%Y-%m-%d %H:00')) as work_shifts_time_frame
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



------------------------------------


select data_time,
       project_code,
       happen_time,
       type_class,
       amr_type,
       amr_type_des,
       amr_code,
       breakdown_id,
       a.breakdown_id_single,
       ROW_NUMBER() over (PARTITION BY project_code,happen_time,type_class,amr_type,amr_type_des,amr_code,breakdown_id ORDER BY a.breakdown_id_single ASC) AS breakdown_id_sort,
       carry_order_num,
       carry_task_num,
       theory_time,
       error_duration,
       mttr_error_duration,
       mttr_error_num,
       add_mtbf,
       d,
       pt
from (select data_time,
             project_code,
             happen_time,
             type_class,
             amr_type,
             amr_type_des,
             amr_code,
             breakdown_id,
             carry_order_num,
             carry_task_num,
             theory_time,
             error_duration,
             mttr_error_duration,
             mttr_error_num,
             add_mtbf,
             d,
             pt,
             split(breakdown_id, ',') breakdown_id_single
      from ads.ads_amr_breakdown
      where pt = 'A51488'
        and d >= '${start_date}'
        and d <= '${end_date}'
		) tmp
         lateral view explode(breakdown_id_single) a as breakdown_id_single
union all
select data_time,
       project_code,
       happen_time,
       type_class,
       amr_type,
       amr_type_des,
       amr_code,
       breakdown_id,
       NULL as breakdown_id_single,
       1    as breakdown_id_sort,
       carry_order_num,
       carry_task_num,
       theory_time,
       error_duration,
       mttr_error_duration,
       mttr_error_num,
       add_mtbf,
       d,
       pt
from ads.ads_amr_breakdown
where pt = 'A51488'
  and d >= '${start_date}'
  and d <= '${end_date}'
  and breakdown_id is null
  
  
  
---------------------------------------------

select 
type_class,
count(0)
from ads.ads_amr_breakdown
where pt = 'A51488'
and d>='2023-02-01'
group by type_class


part	240
single	15480
all	120



#166122 【报表】A51297 金堂通威HGT产线搬运项目 定制报表需求

http://ones.flashhold.com:10007/project/#/team/BrU6Tdct/task/W8HkcHG6tZCnJSXT