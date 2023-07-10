#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 销售退货单表头 -存储销售退货单据主表信息
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_sal_returnstock_df,dim.dim_kde_bd_project_info
#-- 输出表 : dwd.dwd_kde_sal_returnstock_info_df
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


insert overwrite table ${dwd_dbname}.dwd_kde_sal_returnstock_info_df partition(d='${pre1_date}')
select
fid as id,
fbilltypeid as bill_type_id,
fbillno as bill_no,
substr(fdate,1,10) as bill_date,
fretcustid as ret_cust_id,
freceivecustid as receive_cust_id,
fsettlecustid as settle_cust_id,
fpaycustid as pay_cust_id,
if(nvl(regexp_replace(fdeliveryno,' ',''),'')='',null,fdeliveryno) as delivery_no,
if(nvl(regexp_replace(ftakedeliveryno,' ',''),'')='',null,ftakedeliveryno) as take_delivery_no,
fstockorgid as stock_org_id,
fstockergroupid as stocker_group_id,
fstockdeptid as stock_dept_id,
fstockerid as stocker_id,
if(nvl(regexp_replace(freturnreason,' ',''),'')='',null,freturnreason) as return_reason,
fsaleorgid as sale_org_id,
fsaledeptid as sale_dept_id,
fsalegroupid as sale_group_id,
fsalesmanid as salesman_id,
fdocumentstatus as document_status,
if(nvl(regexp_replace(fnote,' ',''),'')='',null,fnote) as note,
fcreatorid as creator_id,
fcreatedate as create_time,
fmodifierid as modifier_id,
fmodifydate as modify_time,
fapproverid as approver_id,
fapprovedate as approve_time,
case when upper(fcancelstatus)='A' then '1'
	 when upper(fcancelstatus)='B' then '0'
	else '-1' end as is_cancel,
fcancellerid as canceller_id,
fcanceldate as cancel_date,
fownertypeid as owner_type_id,
fownerid as owner_id,
fbusinesstype as business_type,
fheadlocid as headloc_id,
if(nvl(regexp_replace(fheadlocaddress,' ',''),'')='',null,fheadlocaddress) as headloc_address,
if(nvl(regexp_replace(freceiveaddress,' ',''),'')='',null,freceiveaddress) as receive_address,
fcreditcheckresult as credit_check_status,
fobjecttypeid as object_type_id,
ftransferbiztype as transfer_biz_type,
fcorrespondorgid as correspond_org_id,
freccontactid as reccontact_id,
fisinterlegalperson as is_interlegal_person,
fproject as project_no,
b.project_code as project_code,
fistotalserviceorcost as is_totalservice_or_cost,
if(nvl(regexp_replace(fshopnumber,' ',''),'')='',null,fshopnumber)  as shop_number,
substr(fgydate,1,10) as gy_date,
if(nvl(regexp_replace(fsalechannel,' ',''),'')='',null,fsalechannel) as sale_channel,
if(nvl(regexp_replace(fgyexpressno,' ',''),'')='',null,fgyexpressno) as gy_express_no,
fgenfrompos_cmk as genfrompos_cmk,
if(nvl(regexp_replace(fbranchid,' ',''),'')='',null,fbranchid) as branch_id,
fisunaudit as is_unaudit
from 
${ods_dbname}.ods_qkt_kde_sal_returnstock_df a
left join (select id,project_code from ${dim_dbname}.dim_kde_bd_project_info where is_forbid='1') b on a.fproject=b.id
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
