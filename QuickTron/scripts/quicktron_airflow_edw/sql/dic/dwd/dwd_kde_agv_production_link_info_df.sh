#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 金蝶agv生产订单，入库链路信息表
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods_qkt_kde_prd_mo_df,ods_qkt_kde_prd_mo_entry_df,dwd_kde_bd_material_info_df,ods_qkt_kde_prd_instock_entry_df,ods_qkt_kde_prd_instock_df,ods_qkt_kde_prd_instock_mtrl_serial_df
#-- 输出表 : dwd.dwd_kde_agv_production_link_info_df
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

with agv_production_str1 as (
select
a.fbillno as order_bill_no,
substr(a.fdate,1,10) as order_bill_date,
upper(a.fdocumentstatus) as document_status,
case when upper(a.fcancelstatus)='A' then '0' 
	 when upper(a.fcancelstatus)='B' then '1' 
	 else '-1' end as is_cancel,
b.fentryid as order_entry_id,
b.fmaterialid as material_id,
b.fqty as order_quantity
from 
${ods_dbname}.ods_qkt_kde_prd_mo_df a
left join ${ods_dbname}.ods_qkt_kde_prd_mo_entry_df b on a.fid=b.fid and b.d='${pre1_date}'
where a.d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_kde_agv_production_link_info_df partition (d='${pre1_date}')
select 
a.order_bill_no,
a.order_bill_date,
a.document_status,
a.is_cancel,
a.order_entry_id,
a.material_id,
c.material_number,
c.material_name,
c.material_spec_model,
if(c.paez_checkbox='1',1,a.order_quantity) as order_quantity,
substr(f.fdate,1,10) as order_instock_date,
if(c.paez_checkbox='1' and e.fid is not null,1,e.frealqty) as order_instock_quantity,
e.fprice as order_instock_price,
regexp_replace(g.fserialno,'\\\\s+','') as frame_no,
if(c.paez_checkbox='1','1','0') as is_agv_material
from 
agv_production_str1 a
left join ${dwd_dbname}.dwd_kde_bd_material_info_df c on a.material_id=c.material_id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_kde_prd_instock_entry_df e on e.fmobillno=a.order_bill_no and e.fmaterialid = a.material_id and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_kde_prd_instock_df f on e.fid=f.fid and f.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_kde_prd_instock_mtrl_serial_df g on g.fentryid = e.fentryid and g.d='${pre1_date}'
;

"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

