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
--携程商旅明细表 ads_project_ctrip_travel_detail

INSERT overwrite table ${ads_dbname}.ads_project_ctrip_travel_detail
SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       nvl(pvd.project_sale_code,pvd.project_code) as project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       c.dept_name, -- 一级部门
       c.team_org_name, -- 二级部门
       c.passenger_name as travel_user, -- 商旅人员
       '用车' as travel_type, -- 商旅类型
       date_format(c.start_time, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(c.end_time, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       '国内' as travel_scope, -- 商旅范围
       CONCAT(c.start_address,'-',c.end_address) as travel_path, -- 商旅路径
       NULL as travel_detail, -- 商旅详细信息
       c.real_amount_haspost as amount, -- 费用金额
       IF(c.real_amount_haspost >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_car_account_check_info_di c
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON c.d = td.days
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df pvd
ON pvd.d = '${pre1_date}' AND (c.project_code = pvd.project_code OR c.project_code = pvd.project_sale_code)
WHERE (td.is_month_end = 1 OR td.days = '${pre1_date}') AND pvd.project_code is not null

UNION ALL 

SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       nvl(pvd.project_sale_code,pvd.project_code) as project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       f.dept_name, -- 一级部门
       f.team_org_name, -- 二级部门
       f.passenger_name as travel_user, -- 商旅人员
       '飞机' as travel_type, -- 商旅类型
       date_format(f.takeoff_time, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(f.arrival_time, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       f.flight_class as travel_scope, -- 商旅范围
       CONCAT(f.departure_city,'-',f.purpose_city) as travel_path, -- 商旅路径
       f.flight_no as travel_detail, -- 商旅详细信息
       f.real_amount as amount, -- 费用金额
       IF(f.real_amount >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di f
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON f.d = td.days
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df pvd
ON pvd.d = '${pre1_date}' AND (f.project_code = pvd.project_code OR f.project_code = pvd.project_sale_code)
WHERE (td.is_month_end = 1 OR td.days = '${pre1_date}') AND pvd.project_code is not null


UNION ALL 

SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       nvl(pvd.project_sale_code,pvd.project_code) as project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       h.dept_name, -- 一级部门
       h.team_org_name, -- 二级部门
       h.check_in_name as travel_user, -- 商旅人员
       '酒店' as travel_type, -- 商旅类型
       date_format(h.check_in_date, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(h.out_date, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       h.hotel_class as travel_scope, -- 商旅范围
       h.hotel_city as travel_path, -- 商旅路径
       h.hotel_name as travel_detail, -- 商旅详细信息
       h.amount as amount, -- 费用金额
       IF(h.amount >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di h
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON h.d = td.days
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df pvd
ON pvd.d = '${pre1_date}' AND (h.project_code = pvd.project_code OR h.project_code = pvd.project_sale_code)
WHERE (td.is_month_end = 1 OR td.days = '${pre1_date}') AND pvd.project_code is not null;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"