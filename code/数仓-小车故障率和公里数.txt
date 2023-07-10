select d.dt as cur_date,d.id as agv_code,tt2.agv_type_code as agv_type,IF(tt1.num is null,0,tt1.num) as agv_breakdown_num,d.extra_data["fenzi"] as agv_breakdown_time,d.value as agv_breakdown_rate,NULL as agv_mileage,d.project_code
from pre.pre_project_agv_profile_d d
LEFT JOIN 
(
select id as agv_code,SUM(value) as num,dt
from pre.pre_project_agv_profile_d
where kpi_code='theDayHourProjectAgvBreakdownGreaterThanOrEqual3Num' AND project_code ='C35052' AND  dt >= '2021-10-01'
group BY id,dt
)tt1
ON d.dt = tt1.dt AND d.id = tt1.agv_code
LEFT JOIN 
(
select t1.agv_code,t2.agv_type_code,t1.d
from dwd.dwd_rcs_basic_agv_info t1
left join dwd.dwd_rcs_basic_agv_type_info t2
on t2.project_code = t1.project_code and t2.d = t1.d and t2.id = t1.agv_type_id
where t1.project_code ='C35052' AND  t1.d >= '2021-10-01'
)tt2
ON d.dt = tt2.d AND d.id = tt2.agv_code
where d.kpi_code='theDayProjectAgvBreakdownNolessthan3Rate' AND d.project_code ='C35052' AND  d.dt >= '2021-10-01'