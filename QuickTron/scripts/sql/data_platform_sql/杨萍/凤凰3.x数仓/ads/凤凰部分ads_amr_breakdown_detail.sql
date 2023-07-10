-- 凤凰3.X CARRIER逻辑
-- union all 

SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       bd.project_code, -- 项目编码
       bd.error_start_time as happen_time, -- 故障触发时间
       bd.first_classification as carr_type_des, -- 机器人大类
       bd.robot_type_code as amr_type, -- 机器人类型编码
       bd.robot_type_name as amr_type_des, -- 机器人类型名称
       bd.robot_code as amr_code, -- 机器人编码
       bd.error_level, -- 故障等级
       bd.error_name as error_des, -- 故障描述
       bd.error_code as error_code, -- 故障编码
       bd.error_module as error_module, -- 故障模块
       bd.error_end_time as end_time, -- 故障结束时间
       bd.error_duration as error_duration, -- 故障时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(bd.error_start_time,1,10) as d,
       bd.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  where project_version LIKE '3.%'
)pt 
join 
(select 
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
where te.d >= '${pre1_date}'
and te.error_module='robot' and te.error_level>=3)bd