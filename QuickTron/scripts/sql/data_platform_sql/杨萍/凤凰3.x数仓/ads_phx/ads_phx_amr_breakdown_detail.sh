#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre1_date=`date -d "-10 day" +%F`

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
#if [ -n "$1" ] ;then
#    pre1_date=$1
#else
#    pre1_date=`date -d "-10 day" +%F`
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
-- 凤凰机器人故障明细 ads_phx_amr_breakdown_detail 

INSERT overwrite table ${ads_dbname}.ads_phx_amr_breakdown_detail partition(d,pt)
SELECT bd.id                                                   as error_id,             -- 故障id
       date_format( bd.error_start_time,'yyyy-MM-dd 00:00:00') as data_time,      -- 统计小时
       bd.project_code,                                                           -- 项目编码
       bd.error_start_time                                     as happen_time,    -- 故障触发时间
       bd.first_classification                                 as carr_type_des,  -- 机器人大类
       bd.robot_type_code                                      as amr_type,       -- 机器人类型编码
       bd.robot_type_name                                      as amr_type_des,   -- 机器人类型名称
       bd.robot_code                                           as amr_code,       -- 机器人编码
       bd.error_level,                                                            -- 故障等级
       bd.error_name                                           as error_des,      -- 故障描述
       bd.error_code                                           as error_code,     -- 故障编码
       bd.error_module                                         as error_module,   -- 故障模块
       bd.error_end_time                                       as end_time,       -- 故障结束时间
       bd.error_duration                                       as error_duration, -- 故障时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(bd.error_start_time, 1, 10)                      as d,
       bd.project_code                                         as pt
FROM (SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      where project_version LIKE '3.%') c
         inner join
     (select te.project_code,
             te.id,
             te.robot_code,
             te.robot_type_code,
             te.robot_type_name,
             te.first_classification,
             te.error_code,
             tde.error_name,
             te.error_start_time,
             te.error_end_time,
			 --  case when te.error_end_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(te.error_end_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(te.error_start_time),'yyyy-MM-dd HH:mm:ss')),0)*1000+nvl(cast(SUBSTRING(te.error_end_time,21,3) as int),0)-nvl(cast(SUBSTRING(te.error_start_time,21,3) as int),0))/1000 end as error_duration,	 
	         case when te.error_end_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(te.error_end_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(te.error_start_time),'yyyy-MM-dd HH:mm:ss')),0)*1000+nvl(cast(SUBSTRING(rpad(te.error_end_time,23,'0'),21,3) as int),0)-nvl(cast(SUBSTRING(rpad(te.error_start_time,23,'0'),21,3) as int),0))/1000 end as error_duration,	 	 
             te.error_level,
             te.error_detail,
             te.error_module,
             to_date(te.error_start_time)                                            as error_start_date
      from ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di te
               left join ${dim_dbname}.dim_phx_basic_error_info_ful tde on tde.project_code=te.project_code and tde.error_code = te.error_code
      where te.d >= '${pre1_date}'
        and te.error_module = 'robot'
        and te.error_level >= 3) bd on bd.project_code = c.project_code
;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"