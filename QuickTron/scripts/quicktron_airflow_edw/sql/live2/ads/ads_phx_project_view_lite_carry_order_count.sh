#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre11_date=`date -d "-10 day" +%F`

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
-- 凤凰项目概览简易搬运作业单表 ads_phx_project_view_lite_carry_order_count 
-- 凤凰3.X CARRIER逻辑

INSERT overwrite table ${ads_dbname}.ads_phx_project_view_lite_carry_order_count partition(d,pt)
SELECT '' AS id, -- 主键
       t.project_code, -- 项目编码
       COALESCE (max(t.create_order_num),0)  AS send_num, -- 下发单量
       COALESCE (max(t.canceled_order_num),0) AS cancel_num, -- 取消单量
       COALESCE (max(t.abnormal_order_num),0) AS exc_num, -- 异常单量
       t.cur_hour AS count_date, -- 统计小时
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       t.cur_date as d,
       t.project_code AS pt 
from 
(SELECT * FROM ${dim_dbname}.dim_collection_project_record_ful WHERE project_version like '3.%') c
inner join 
(select 
project_code,substr(order_create_time,1,10) as cur_date,
DATE_FORMAT(order_create_time,'yyyy-MM-dd HH:00:00') AS cur_hour,
count(distinct order_no)        as create_order_num,
null as abnormal_order_num,
null as canceled_order_num
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where d >= '${pre11_date}'
group by project_code,substr(order_create_time,1,10),DATE_FORMAT(order_create_time,'yyyy-MM-dd HH:00:00') 
union all 
select 
project_code,substr(order_update_time,1,10) as cur_date,
DATE_FORMAT(order_update_time,'yyyy-MM-dd HH:00:00') AS cur_hour,
null as create_order_num,
count(distinct case when order_state in ('ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED', 'PENDING') then order_no end)  as abnormal_order_num,
count(distinct case when order_state in ('CANCELED') then order_no end) as canceled_order_num
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where d >= '${pre11_date}'
group by project_code,substr(order_update_time,1,10),DATE_FORMAT(order_update_time,'yyyy-MM-dd HH:00:00')
)t on t.project_code =c.project_code 
group by t.project_code,t.cur_date,t.cur_hour
;
-----------------------------------------------------------------------------------------------------------------------------00

"


$hive -e "$sql"