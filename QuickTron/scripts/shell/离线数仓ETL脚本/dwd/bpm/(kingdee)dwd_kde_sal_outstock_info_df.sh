#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 销售出库单表头（存储销售出库单据主表信息）
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_sal_outstock_df,dim.dim_kde_bd_project_info
#-- 输出表 : dwd.dwd_kde_sal_outstock_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-29 CREATE 
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


insert overwrite table ${dwd_dbname}.dwd_kde_sal_outstock_info_df partition(d='${pre1_date}')
select 
fid as id ,
fbilltypeid as bill_type_id ,
fbillno as bill_no ,
substr(fdate,1,10) as bill_date ,
fcustomerid as customer_id ,
fstockorgid as stock_org_id ,
fdeliverydeptid as delivery_dept_id ,
fstockergroupid as stocker_group_id ,
fstockerid as stocker_id ,
freceiverid as receiver_id ,
fsettleid as settle_id ,
fpayerid as payer_id ,
fsaleorgid as sale_org_id ,
fsaledeptid as sale_dept_id ,
fsalesgroupid as sales_group_id ,
fsalesmanid as salesman_id ,
if(nvl(regexp_replace(fdeliverybill,' ',''),'')='',null,fdeliverybill)as delivery_bill ,
if(nvl(regexp_replace(ftakedeliverybill,' ',''),'')='',null,ftakedeliverybill) as take_delivery_bill ,
fcarrierid as carrier_id ,
if(nvl(regexp_replace(fcarriageno,' ',''),'')='',null,fcarriageno) as carriage_no ,
upper(fdocumentstatus) as document_status ,
if(nvl(regexp_replace(fnote,' ',''),'')='',null,fnote) as note ,
fcreatorid as creator_id ,
fcreatedate as create_time ,
fmodifierid as modifier_id ,
fmodifydate as modify_time ,
fapproverid as approver_id ,
fapprovedate as approve_time ,
case when upper(fcancelstatus)='A' then '1' 
     when upper(fcancelstatus)='B' then '0'
     else '-1' end as is_cancel ,
fcancellerid as canceller_id ,
fcanceldate as cancel_time ,
fownertypeid as owner_type_id ,
fownerid as owner_id ,
if(nvl(regexp_replace(fheadlocaddress,' ',''),'')='',null,fheadlocaddress) as headlocation_address ,
if(nvl(regexp_replace(fheadlocationid,' ',''),'')='',null,fheadlocationid) as headlocation_id ,
fbusinesstype as business_type ,
if(nvl(regexp_replace(freceiveaddress,' ',''),'')='',null,freceiveaddress) as receive_address ,
fcreditcheckresult as credit_check_status ,
fobjecttypeid as object_type_id ,
ftransferbiztype as transfer_biz_type ,
fcorrespondorgid as correspond_org_id ,
freccontactid as reccontact_id ,
fisinterlegalperson as is_interlegal_person ,
if(nvl(regexp_replace(fplanrecaddress,' ',''),'')='',null,fplanrecaddress) as plan_recaddress ,
fistotalserviceorcost as is_totalservice_or_cost ,
if(nvl(regexp_replace(fsrcfid,' ',''),'')='',null,fsrcfid) as src_fid ,
fdisassemblyflag as disassemblyflag ,
fproject as project_no ,
b.project_code as project_code ,
f_paez_text as paez_text ,
f_paez_text1 as paez_text1 ,
f_paez_printtimes as paez_printtimes ,
if(nvl(regexp_replace(fshopnumber,' ',''),'')='',null,fshopnumber) as shop_number ,
substr(fgydate,1,10) as gy_date ,
if(nvl(regexp_replace(fsalechannel,' ',''),'')='',null,fsalechannel) as sale_channel ,
if(nvl(regexp_replace(fgenfrompos_cmk,' ',''),'')='',null,fgenfrompos_cmk) as genfrompos_cmk ,
if(nvl(regexp_replace(flinkman,' ',''),'')='',null,flinkman) as link_man ,
if(nvl(regexp_replace(flinkphone,' ',''),'')='',null,flinkphone) as link_phone ,
fbranchid as branch_id,
fisunaudit as is_unaudit
from 
${ods_dbname}.ods_qkt_kde_sal_outstock_df a
left join (select id,project_code from ${dim_dbname}.dim_kde_bd_project_info where is_forbid='1') b on a.fproject=b.id
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
