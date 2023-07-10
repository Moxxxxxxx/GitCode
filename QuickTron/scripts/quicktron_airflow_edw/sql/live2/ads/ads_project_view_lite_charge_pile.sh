#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre11_date=`date -d "-8 day" +%F`

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
#if [ -n "$1" ] ;then
#    pre11_date=$1
#else
#    pre11_date=`date -d "-10 day" +%F`
#fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- 项目概览简易版本充电桩状态表 ads_project_view_lite_charge_pile 

INSERT overwrite table ${ads_dbname}.ads_project_view_lite_charge_pile partition(d,pt)
-- 最近充电桩运行状态
SELECT '' AS id, -- 主键
       c.project_code, -- 项目编码
       bc.charger_port_type AS charge_type, -- 充电桩类型
       bc.charger_code AS charge_code, -- 充电桩编码
       COUNT(DISTINCT bc.charger_code) AS charge_total_num, -- 充电桩总数
       NULL AS charge_online_num, -- 充电桩启用数量
       NULL AS charge_offline_num, -- 充电未启用数量
       COUNT(DISTINCT h.charger_code) AS charge_execute_num, -- 充电桩使用数量
       COUNT(DISTINCT bc.charger_code) - COUNT(DISTINCT h.charger_code) AS charge_off_execute_num, -- 充电桩未使用数量
       0 AS is_version, -- 是否3X版本
       SUM(nvl(h.charger_times,0)) AS use_times, -- 充电桩使用次数
       SUM(nvl(h.charger_duration,0)) AS use_duration, -- 充电桩使用时长/s
       bc.d AS count_date, -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       bc.d,
       c.project_code AS pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  WHERE c.project_version like '2.%'
)c
LEFT JOIN 
(
  SELECT *
  FROM ${dwd_dbname}.dwd_rcs_basic_charger_info_df bc
  WHERE bc.d >= '${pre11_date}'
)bc
ON c.project_code = bc.project_code
LEFT JOIN 
(
  SELECT h.project_code,
         h.d,
         get_json_object(h.job_context,'$.chargerCode') AS charger_code,
         COUNT(DISTINCT h.job_id) AS charger_times,
         SUM(unix_timestamp(h.job_finish_time) - unix_timestamp(h.job_accept_time)) AS charger_duration
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
  WHERE h.d >= '${pre11_date}' AND h.job_type = 'CHARGE_JOB'
  GROUP BY h.project_code,h.d,get_json_object(h.job_context,'$.chargerCode')
)h
ON bc.d = h.d AND bc.project_code = h.project_code AND bc.charger_code = h.charger_code
WHERE bc.charger_port_type IS NOT NULL -- 剔除无充电桩类型数据
GROUP BY c.project_code,bc.d,bc.charger_port_type,bc.charger_code


-- 凤凰
union all 
SELECT ''                                                               AS id,                     -- 主键
       c.project_code,                                                                             -- 项目编码
       bc.charger_port_type                                             AS charge_type,            -- 充电桩类型
       bc.charger_code                                                  AS charge_code,            -- 充电桩编码
       COUNT(DISTINCT bc.charger_code)                                  AS charge_total_num,       -- 充电桩总数
       NULL                                                             AS charge_online_num,      -- 充电桩启用数量
       NULL                                                             AS charge_offline_num,     -- 充电未启用数量
       COUNT(DISTINCT h.charger_code)                                   AS charge_execute_num,     -- 充电桩使用数量
       COUNT(DISTINCT bc.charger_code) - COUNT(DISTINCT h.charger_code) AS charge_off_execute_num, -- 充电桩未使用数量
       1                                                                AS is_version,             -- 是否3X版本
       SUM(nvl(h.charge_num, 0))                                        AS use_times,              -- 充电桩使用次数
       cast(SUM(nvl(h.charge_duration, 0)) as decimal(10,3))            AS use_duration,           -- 充电桩使用时长/s
       bc.d                                                             AS count_date,             -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')          AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')          AS update_time,
       bc.d,
       c.project_code                                                   AS pt
FROM (SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful c
      WHERE c.project_version like '3.%') c
         join
     (select bc.*
      from ${dwd_dbname}.dwd_phx_basic_charger_info_df bc
               inner join ${dwd_dbname}.dwd_phx_basic_map_info_df bm
                          on bm.map_code = bc.map_code and bm.map_state = 'release' and bm.d = bc.d and
                             bm.d >= '${pre11_date}'
      where bc.d >= '${pre11_date}') bc ON c.project_code = bc.project_code
         left join
     (select t.project_code,
             t.curr_date,
             t.charger_code,
             COALESCE(sum(create_charge_num), 0)    as create_charge_num,
             COALESCE(sum(end_charge_num), 0)       as charge_num,
             COALESCE(sum(end_charge_duration), 0)  as charge_duration,
             COALESCE(sum(end_charge_power_num), 0) as charge_power_num
      from (select project_code,
                   to_date(coalesce(enter_charging_time, bind_robot_time)) as curr_date,
                   charger_code,
                   count(distinct id)                                      as create_charge_num,
                   null                                                    as end_charge_num,
                   null                                                    as end_charge_duration,
                   null                                                    as end_charge_power_num
            from ${dwd_dbname}.dwd_phx_rms_robot_charging_info_di
            where d >= '${pre11_date}'
              and enter_charging_power is not null
              and coalesce(enter_charging_time, bind_robot_time) is not null
            group by project_code, to_date(coalesce(enter_charging_time, bind_robot_time)), charger_code
            union all
            select project_code,
                   to_date(recover_charger_time)                                                 as curr_date,
                   charger_code,
                   null                                                                          as create_charge_num,
                   count(distinct id)                                                            as end_charge_num,
-- sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
-- sum((nvl(unix_timestamp(from_unixtime(unix_timestamp(recover_charger_time), 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(from_unixtime(unix_timestamp(coalesce(enter_charging_time, bind_robot_time)),'yyyy-MM-dd HH:mm:ss')), 0) * 1000 +nvl(cast(SUBSTRING(recover_charger_time, 21, 3) as int), 0) - nvl(cast(SUBSTRING(coalesce(enter_charging_time, bind_robot_time), 21, 3) as int), 0)) /1000)  as end_charge_duration,			   
           sum((nvl(unix_timestamp(from_unixtime(unix_timestamp(recover_charger_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(coalesce(enter_charging_time, bind_robot_time)),'yyyy-MM-dd HH:mm:ss')),0)*1000+nvl(cast(SUBSTRING(rpad(recover_charger_time,23,'0'),21,3) as int),0)-nvl(cast(SUBSTRING(rpad(coalesce(enter_charging_time, bind_robot_time),23,'0'),21,3) as int),0))/1000) 	as end_charge_duration,			   
                   sum(recover_charger_power - coalesce(enter_charging_power, bind_robot_power)) as end_charge_power_num
            from ${dwd_dbname}.dwd_phx_rms_robot_charging_info_di
            where d >= DATE_ADD('${pre11_date}', -10)
              and enter_charging_power is not null
              and to_Date(recover_charger_time) >= '${pre11_date}'
            group by project_code, to_date(recover_charger_time), charger_code) t
      group by t.project_code, t.curr_date, t.charger_code) h
     on h.project_code = bc.project_code and h.charger_code = bc.charger_code and h.curr_date = bc.d
GROUP BY c.project_code, bc.d, bc.charger_port_type, bc.charger_code

;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"