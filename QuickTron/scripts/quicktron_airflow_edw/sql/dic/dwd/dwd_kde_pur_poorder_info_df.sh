#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 采购订单表头-存储采购订单主信息
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_pur_poorder_df,dim.dim_kde_bd_project_info
#-- 输出表 : dwd.dwd_kde_pur_poorder_info_df
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


insert overwrite table ${dwd_dbname}.dwd_kde_pur_poorder_info_df partition(d='${pre1_date}')
select 
fid as id,
fbilltypeid as bill_type_id,
fbillno as bill_no,
substr(fdate,1,10) as bill_date,
fsupplierid as supplier_id,
fpurchaseorgid as purchase_org_id,
fpurchasergroupid as purchaser_group_id,
fpurchasedeptid as purchase_dept_id,
fpurchaserid as purchaser_id,
fcreatorid as creator_id,
fcreatedate as create_time,
fmodifierid as modifier_id,
fmodifydate as modify_time,
fdocumentstatus as document_status,
fapproverid as approver_id,
fapprovedate as approve_time,
case when upper(fclosestatus)='A' then '0'
	 when upper(fclosestatus)='B' then '1'
	else '-1' end as is_closed,
fcloserid as closer_id,
fclosedate as close_time,
case when upper(fcancelstatus)='A' then '1'
	  when upper(fcancelstatus)='B' then '0'
	else '-1' end as is_cancel,
fcancellerid as canceller_id,
fcanceldate as cancel_date,
fproviderid as provider_id,
fsettleid as settle_id,
fchargeid as charge_id,
fversionno as version_no,
if(nvl(regexp_replace(fchangereason,' ',''),'')='',null,fchangereason) as change_reason,
fchangedate as change_date,
fchangerid as changer_id,
fisconvert as is_convert,
fpurcatalogid as pur_catalog_id,
fbusinesstype as business_type,
if(nvl(regexp_replace(fprovideraddress,' ',''),'')='',null,fprovideraddress) as provider_address,
fobjecttypeid as object_type_id,
fassignsupplierid as assign_supplier_id,
fcorrespondorgid as correspond_org_id,
if(nvl(regexp_replace(fprovidercontact,' ',''),'')='',null,fprovidercontact) as provider_contact,
if(nvl(regexp_replace(fnetorderbillno,' ',''),'')='',null,fnetorderbillno) as net_order_bill_no,
fnetorderbillid as net_order_bill_id,
fconfirmstatus as confirm_status,
fconfirmerid as confirmer_id,
fconfirmdate as confirm_date,
fprovidercontactid as provider_contact_id,
fbilltypeidvm as bill_type_idvm,
fproject as project_no,
b.project_code as project_code,
f_paez_base as paez_base,
f_paez_text as paez_text,
if(nvl(regexp_replace(f_consignor,' ',''),'')='',null,f_consignor) as consignor,
if(nvl(regexp_replace(f_note,' ',''),'')='',null,f_note) as note,
fchangestatus as change_status,
if(nvl(regexp_replace(facctype,' ',''),'')='',null,facctype)  as acc_type,
frelreqstatus as relreq_status,
if(nvl(regexp_replace(fsourcebillno,' ',''),'')='',null,fsourcebillno) as source_bill_no,
f_abc_checkbox as abc_checkbox,
if(nvl(regexp_replace(f_abc_text,' ',''),'')='',null,f_abc_text) as abc_text,
if(nvl(regexp_replace(f_abc_text1,' ',''),'')='',null,f_abc_text1) as abc_text1,
if(nvl(regexp_replace(f_abc_text2,' ',''),'')='',null,f_abc_text2) as abc_text2,
if(nvl(regexp_replace(f_abc_combo,' ',''),'')='',null,f_abc_combo) as abc_combo
from 
${ods_dbname}.ods_qkt_kde_pur_poorder_df a
left join (select id,project_code from ${dim_dbname}.dim_kde_bd_project_info where is_forbid='1') b on a.fproject=b.id
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

