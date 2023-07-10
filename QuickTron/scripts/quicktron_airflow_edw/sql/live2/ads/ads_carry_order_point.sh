#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- DIC项目码点 ads_carry_order_point 

INSERT overwrite table ${ads_dbname}.ads_carry_order_point

SELECT '' as id, -- 主键
       t.point_code,
       ba.project_code,
       'S' as point_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_rcs_basic_area_info_df ba
lateral view explode(split(ba.point_code,';')) t as point_code
WHERE ba.d = '${pre1_date}' AND LENGTH(t.point_code) != 0
GROUP BY t.point_code,ba.project_code

UNION ALL 

SELECT '' as id, -- 主键
       t.point_code,
       ba.project_code,
       'T' as point_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_rcs_basic_area_info_df ba
lateral view explode(split(ba.point_code,';')) t as point_code
WHERE ba.d = '${pre1_date}' AND LENGTH(t.point_code) != 0
GROUP BY t.point_code,ba.project_code

-- 凤凰
union all 
SELECT '' as id, -- 主键
start_point_code as point_code,
project_code,
'S' as point_type,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where start_point_code <> '' and start_point_code is not null
group by start_point_code,project_code
union all 
SELECT '' as id, -- 主键
target_point_code as point_code,
project_code,
'T' as point_type,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where start_point_code <> '' and start_point_code is not null
group by target_point_code,project_code

-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"