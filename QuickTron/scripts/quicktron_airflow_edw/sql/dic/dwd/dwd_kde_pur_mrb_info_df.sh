#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 采购订单表头-存储采购订单主信息
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_pur_mrb_df,dim.dim_kde_bd_project_info
#-- 输出表 : dwd.dwd_kde_pur_mrb_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-30 CREATE 
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
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_kde_pur_mrb_info_df partition(d='${pre1_date}')
select 
fid as id,
fobjecttypeid as object_type_id,
fbilltypeid as bill_type_id,
fbillno as bill_no,
substr(fdate,1,10) as bill_date,
fsupplierid as supplier_id,
facceptorid as acceptor_id,
fsettleid as settle_id,
fchargeid as charge_id,
if(nvl(regexp_replace(fdeliveryno,' ',''),'')='',null,fdeliveryno) as delivery_no,
if(nvl(regexp_replace(ftakedeliveryno,' ',''),'')='',null,ftakedeliveryno) as take_delivery_no,
fcarrierid as carrier_id,
if(nvl(regexp_replace(fcarryno,' ',''),'')='',null,fcarryno) as carry_no,
fstockorgid as stock_org_id,
fmrdeptid as mr_dept_id,
fstockergroupid as stocker_group_id,
fstockerid as stocker_id,
fmrtype as mr_type,
fmrmode as mr_mode,
freplenishmode as replenish_mode,
if(nvl(regexp_replace(fmrreason,' ',''),'')='',null,fmrreason) as mr_reason,
fpurchaseorgid as purchase_org_id,
fpurchasergroupid as purchaser_group_id,
fpurchasedeptid as purchase_dept_id,
fpurchaserid as purchaser_id,
fdescription as description,
fdocumentstatus as document_status,
case when upper(fcancelstatus) ='A' then '1'
	 when upper(fcancelstatus)='B' then '0'
	else '-1' end  as is_cancel,
fcreatorid as creator_id,
fcreatedate as create_time,
fmodifierid as modifier_id,
fmodifydate as modify_time,
fapproverid as approver_id,
fapprovedate as approve_time,
fcancellerid as canceller_id,
fcanceldate as cancel_date,
fisconvert as is_convert,
fbusinesstype as business_type,
fownertypeid as owner_type_id,
fownerid as owner_id,
facceptoraddress as acceptor_address,
case when upper(fapstatus)='Y'  then '1'
	when upper(fapstatus)='N' then '0'
else '-1' end as is_ap_status,
frequireorgid as require_org_id,
ftransferbiztype as transfer_biz_type,
fcorrespondorgid as correspond_org_id,
fisinterlegalperson as is_interlegal_person,
fconfirmstatus as confirm_status,
fconfirmerid as confirmer_id,
fconfirmdate as confirm_date,
facceptorcontactid as acceptor_contact_id,
fbilltypeidvm as bill_type_idvm,
fproject as project_no,
b.project_code as project_code,
fscanpoint as scan_point,
fpushfromretailbill as push_fromretail_bill,
if(nvl(regexp_replace(forderno,' ',''),'')='',null,forderno) as order_no
from 
${ods_dbname}.ods_qkt_kde_pur_mrb_df a
left join (select id,project_code from ${dim_dbname}.dim_kde_bd_project_info where is_forbid='1') b on a.fproject=b.id 
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

