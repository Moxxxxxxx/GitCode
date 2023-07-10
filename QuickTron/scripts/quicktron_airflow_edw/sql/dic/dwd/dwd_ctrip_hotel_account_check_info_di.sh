#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 携程酒店信息
#-- 注意 ：  每日t-1分区
#-- 输入表 : ods.ods_qkt_ctrip_hotel_account_check_di
#-- 输出表 ：dwd.dwd_ctrip_hotel_account_check_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-23 CREATE 
#-- 2 wangziming 2022-06-28 modify 增加  check_in_date 入店日期，并清洗project_code 的脏数据
#-- 3 wangziming 2022-07-23 modify 少字段补充
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


insert overwrite table ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di partition(d)
select 
orderid as order_id,
status as order_status,
uid as ctrip_card_no,
name as card_holder_name,
clients as check_in_name,
workcity as work_city,
orderdate as order_date,
etd as out_date,
city as hotel_city,
hotelname as hotel_name,
hotelename as hotel_ename,
roomname as room_name,
roomcount as room_count,
star as hotel_star,
quantity,
price,
amount,
remarks,
corp_paytype as corp_pay_type,
iscu as is_cu,
replace(costcenter,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_class,
REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0) as project_code,
replace(costcenter3,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_name,
dept1 as dept_name,
dept2 as team_org_name,
hotelrelatedjourneyno as hotel_relatedjourney_no,
htlclass as hotel_class,
rebate as rebate,
vatdesc as vat_type,
emptycolumn as empty_column,
corpid as corp_id,
accountid as account_id,
batchno as batch_no,
subbatchno as sub_batch_no,
concat(substr(startdate,1,4),'-',substr(startdate,5,2),'-',substr(startdate,7,2)) as start_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_date,
batchstatus as batch_status,
eta as check_in_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as d
from
${ods_dbname}.ods_qkt_ctrip_hotel_account_check_di
where d='2022-06-27'
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di partition(d)
select 
order_id,
order_status,
ctrip_card_no,
card_holder_name,
check_in_name,
work_city,
order_date,
out_date,
hotel_city,
hotel_name,
hotel_ename,
room_name,
room_count,
hotel_star,
quantity,
price,
amount,
remarks,
corp_pay_type,
is_cu,
project_class,
project_code,
project_name,
dept_name,
team_org_name,
hotel_relatedjourney_no,
hotel_class,
rebate,
vat_type,
empty_column,
corp_id,
account_id,
batch_no,
sub_batch_no,
start_date,
end_date,
batch_status,
check_in_date,
if(end_dates=last_day('${pre1_date}'),'${pre1_date}',end_dates) as d
from 
(
select 
orderid as order_id,
status as order_status,
uid as ctrip_card_no,
name as card_holder_name,
clients as check_in_name,
workcity as work_city,
orderdate as order_date,
etd as out_date,
city as hotel_city,
hotelname as hotel_name,
hotelename as hotel_ename,
roomname as room_name,
roomcount as room_count,
star as hotel_star,
quantity,
price,
amount,
remarks,
corp_paytype as corp_pay_type,
iscu as is_cu,
replace(costcenter,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_class,
REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0) as project_code,
replace(costcenter3,replace(upper(costcenter2),REGEXP_EXTRACT(upper(costcenter2),'^[A-Z].*[0-9]',0),''),'') as project_name,
dept1 as dept_name,
dept2 as team_org_name,
hotelrelatedjourneyno as hotel_relatedjourney_no,
htlclass as hotel_class,
rebate as rebate,
vatdesc as vat_type,
emptycolumn as empty_column,
corpid as corp_id,
accountid as account_id,
batchno as batch_no,
subbatchno as sub_batch_no,
concat(substr(startdate,1,4),'-',substr(startdate,5,2),'-',substr(startdate,7,2)) as start_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_date,
batchstatus as batch_status,
eta as check_in_date,
concat(substr(enddate,1,4),'-',substr(enddate,5,2),'-',substr(enddate,7,2)) as end_dates
from
${ods_dbname}.ods_qkt_ctrip_hotel_account_check_di
where d='${pre1_date}' 
) t 
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

