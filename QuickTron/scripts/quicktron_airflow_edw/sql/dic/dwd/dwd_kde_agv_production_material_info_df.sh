#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 金蝶agv生产领料，用料信息表
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods_qkt_kde_prd_pick_mtrl_df,ods_qkt_kde_prd_pick_mtrl_data_df,dwd_kde_bd_material_info_df,ods_qkt_kde_prd_ppbom_entry_df,ods_qkt_kde_prd_ppbom_entry_qty_df
#-- 输出表 : dwd.dwd_kde_agv_production_material_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-03-06 CREATE 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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




sql="
set hive.execution.engine=spark;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;
-- set hive.vectorized.execution.enabled=false;
-- set hive.vectorized.execution.reduce.enabled=false;
-- set hive.auto.convert.join=false;


with pick_material_str1 as (
select 
*
from 
(
select
*,
row_number() over(partition by material_id,order_bill_no,order_entry_id order by pick_bill_date asc) as rn,
sum(pick_quantity) over(partition by material_id,order_bill_no,order_entry_id order by pick_bill_date desc) as sum_pick_quantity,
sum(pick_amount) over(partition by material_id,order_bill_no,order_entry_id order by pick_bill_date desc) as sum_pick_amount
from 
(
select 
a.fbillno as pick_bill_no,
substr(a.fdate,1,10) as pick_bill_date,
upper(a.fdocumentstatus) as document_status,
case when upper(fcancelstatus)='A' then '0' 
	 when upper(fcancelstatus)='B' then '1' 
	 else '-1' end as is_cancel,
b.fmaterialid as material_id,
b.fmobillno as order_bill_no,
b.fmoentryid as order_entry_id,
b.factualqty as pick_quantity,
b.fprice as pick_price,
b.famount as pick_amount,
c.material_number,
c.material_name,
c.material_spec_model,
if(c.paez_checkbox='1','1','0') as is_agv_material
from 
${ods_dbname}.ods_qkt_kde_prd_pick_mtrl_df a
left join ${ods_dbname}.ods_qkt_kde_prd_pick_mtrl_data_df b on a.fid=b.fid and b.d='${pre1_date}'
left join ${dwd_dbname}.dwd_kde_bd_material_info_df c on b.fmaterialid = c.material_id and c.d='${pre1_date}'
) t
) rt
where rt.rn=1

)

insert overwrite table ${dwd_dbname}.dwd_kde_agv_production_material_info_df partition(d='${pre1_date}')
select 
a.pick_bill_no,
a.pick_bill_date,
a.document_status,
a.is_cancel,
a.material_id,
a.material_number,
a.material_name,
a.material_spec_model,
a.order_bill_no,
a.order_entry_id,
a.sum_pick_quantity as pick_quantity,
a.sum_pick_amount / a.sum_pick_quantity as pick_price,
e.fid as consume_id,
substr(e.fneeddate,1,10) as consume_need_date,
f.fconsumeqty as consume_quantity,
is_agv_material
from 
pick_material_str1 a
left join ${ods_dbname}.ods_qkt_kde_prd_ppbom_entry_df e on e.fmobillno=a.order_bill_no and e.fmoentryid=a.order_entry_id and e.fmaterialid=a.material_id and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_kde_prd_ppbom_entry_qty_df f on f.fid=e.fid and f.fentryid=e.fentryid and f.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

