#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 携程机票账单信息表
#-- 注意 ：  每日增量分区
#-- 输入表 : ods.ods_qkt_ctrip_flight_account_check_di
#-- 输出表 ：dwd.dwd_ctrip_flight_account_check_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-23 CREATE 
#-- 2 wangziming 2022-06-28 modify 更新项目编码和项目名称逻辑
#-- 3 wangziming 2022-07-11 modify 增加飞机落地时间
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dwd_dbname=dwd
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di partition(d)
select 
orderid as order_id,
ctripcardno as ctrip_card_no,
name as card_holder_name,
passengername as passenger_name,
orderdate as order_time,
takeofftime as takeoff_time,
flightclass as flight_class,
split(orderdesc,'-')[0] as departure_city,
split(orderdesc,'-')[1] as purpose_city,
flight as flight_no,
class as booking_class,
if(nvl(pricerate,'')<>'',regexp_replace(pricerate,'%','')/100,null) as price_rate,
price,
tax,
oilfee,
rebookqueryfee as rebook_query_fee,
sendticketfee as send_ticket_fee,
insurancefee as insurance_fee,
refund as refund_fee,
coupon,
servicefee as service_fee,
rebookingservicefee as rebooking_service_fee,
refundservicefee as refund_service_fee,
servicepackageprice as service_package_price,
itineraryservicefee as itinerary_service_fee,
realamount as real_amount,
replace(costcenter,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_class,
REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0) as project_code,
replace(costcenter3,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_name,
dept1 as dept_name,
dept2 as team_org_name,
reasonendesc as low_reason_desc,
reason as low_reason,
codebrief as code_brief,
lowdtimefc as low_dtime_fc,
remark,
corpid as corp_id,
accountid as account_id,
batchno as batch_no,
subbatchno as sub_batch_no,
concat(substr(startdate,1,4),'-',substr(startdate,5,2),'-',substr(startdate,7,2)) as start_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_date,
protectsubclass as protect_sub_class,
batchstatus as batch_status,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as d
from
${ods_dbname}.ods_qkt_ctrip_flight_account_check_di
where d='2022-06-23'
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with tmp_ctrip_flight_order_info_str1 as (
select 
orderid,
takeofftime,
arrivaltime,
flight
from 
(
select 
orderid,
takeofftime,
arrivaltime,
flight,
row_number() over(partition by orderid,flight,takeofftime order by updated_date desc) as rn 
from
${ods_dbname}.ods_qkt_ctrip_flight_order_info_di
where substr(takeofftime,1,10)>=trunc(add_months('${pre1_date}',-1),'MM')
) t
where rn=1
)
insert overwrite table ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di partition(d)
select 
order_id,
ctrip_card_no,
card_holder_name,
passenger_name,
order_time,
takeoff_time,
flight_class,
departure_city,
purpose_city,
trim(flight_no) as flight_no,
booking_class,
price_rate,
price,
tax,
oilfee,
rebook_query_fee,
send_ticket_fee,
insurance_fee,
refund_fee,
coupon,
service_fee,
rebooking_service_fee,
refund_service_fee,
service_package_price,
itinerary_service_fee,
real_amount,
project_class,
project_code,
project_name,
dept_name,
team_org_name,
low_reason_desc,
low_reason,
code_brief,
low_dtime_fc,
remark,
corp_id,
account_id,
batch_no,
sub_batch_no,
start_date,
end_date,
protect_sub_class,
batch_status,
substr(t1.arrivaltime,1,16) as arrival_time,
if(end_dates=last_day('${pre1_date}'),'${pre1_date}',end_dates) as d
from 
(
select 
orderid as order_id,
ctripcardno as ctrip_card_no,
name as card_holder_name,
passengername as passenger_name,
orderdate as order_time,
takeofftime as takeoff_time,
flightclass as flight_class,
split(orderdesc,'-')[0] as departure_city,
split(orderdesc,'-')[1] as purpose_city,
flight as flight_no,
class as booking_class,
if(nvl(pricerate,'')<>'',regexp_replace(pricerate,'%','')/100,null) as price_rate,
price,
tax,
oilfee,
rebookqueryfee as rebook_query_fee,
sendticketfee as send_ticket_fee,
insurancefee as insurance_fee,
refund as refund_fee,
coupon,
servicefee as service_fee,
rebookingservicefee as rebooking_service_fee,
refundservicefee as refund_service_fee,
servicepackageprice as service_package_price,
itineraryservicefee as itinerary_service_fee,
realamount as real_amount,
replace(costcenter,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_class,
REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0) as project_code,
replace(costcenter3,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_name,
dept1 as dept_name,
dept2 as team_org_name,
reasonendesc as low_reason_desc,
reason as low_reason,
codebrief as code_brief,
lowdtimefc as low_dtime_fc,
remark,
corpid as corp_id,
accountid as account_id,
batchno as batch_no,
subbatchno as sub_batch_no,
concat(substr(startdate,1,4),'-',substr(startdate,5,2),'-',substr(startdate,7,2)) as start_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_date,
protectsubclass as protect_sub_class,
batchstatus as batch_status,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_dates
from
${ods_dbname}.ods_qkt_ctrip_flight_account_check_di
where d='${pre1_date}'
) t
left join tmp_ctrip_flight_order_info_str1  t1 on t.order_id =t1.orderid and trim(t.flight_no)=t1.flight
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

