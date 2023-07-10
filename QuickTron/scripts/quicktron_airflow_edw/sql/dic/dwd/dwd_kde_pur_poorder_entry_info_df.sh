#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 采购订单表头-存储采购订单主信息
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_pur_poorder_entry_df,dwd.dwd_kde_pur_poorder_info_df、ods.ods_qkt_kde_pur_poorderentry_finance_df
#-- 输出表 : dwd.dwd_kde_pur_poorder_entry_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-30 CREATE 
#-- 2 wangziming 2022-05-07 modify 增加表关联
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


insert overwrite table ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df partition(d='${pre1_date}')
select 
a.fentryid as entry_id,
a.fid as id,
fseq as seq,
fmaterialid as material_id,
fauxpropid as aux_prop_id,
fbomid as bom_id,
funitid as unit_id,
fqty as quantity,
fbaseunitid as base_unit_id,
fbaseunitqty as base_unit_qty,
if(nvl(regexp_replace(fnote,' ',''),'')='',null,fnote) as note,
case when upper(fmrpfreezestatus)='A' then '1'
	 when upper(fmrpfreezestatus)='B' then '0'
	else '-1' end  as is_mrp_freeze,
ffreezedate as freeze_date,
ffreezerid as freezer_id,
case when upper(fmrpterminatestatus)='A' then '1'
	 when upper(fmrpterminatestatus)='B' then '0'
	else '-1' end  as is_mrp_terminate,
fterminaterid as terminater_id,
if(nvl(regexp_replace(fterminatestatus,' ',''),'')='',null,fterminatestatus) as terminate_status,
fterminatedate as terminate_date,
case when upper(fmrpclosestatus)='A' then '1'
	 when upper(fmrpclosestatus)='B' then '0'
	else '-1' end as is_mrp_close,
flot as lot,
if(nvl(regexp_replace(flot_text,' ',''),'')='',null,flot_text) as lot_text,
if(nvl(regexp_replace(fsupplierlot,' ',''),'')='',null,fsupplierlot) as supplier_lot,
fplanconfirm as plan_confirm,
fchangeflag as change_flag,
fproducttype as product_type,
if(nvl(regexp_replace(frowid,' ',''),'')='',null,frowid)  as row_id,
if(nvl(regexp_replace(fparentrowid,' ',''),'')='',null,fparentrowid)  as parent_row_id,
fcopyentryid as copy_entry_id,
fgroup as pur_group,
if(nvl(regexp_replace(fbflowid,' ',''),'')='',null,fbflowid) as flow_id,
if(nvl(regexp_replace(fsupmatid,' ',''),'')='',null,fsupmatid) as supmat_id,
if(nvl(regexp_replace(fsupmatname,' ',''),'')='',null,fsupmatname) as supmat_name,
fgiveaway as is_give_away,
freceivedeptid as receive_dept_id,
fisstock as is_stock,
fconsumesumqty as consume_sum_qty,
fbaseconsumesumqty as base_consume_sum_qty,
fnetorderentryid as net_order_entry_id,
fsalunitid as sal_unit_id,
fsalqty as sal_qty,
fsalbaseqty as sal_base_qty,
fstockunitid as stock_unit_id,
fstockqty as stock_qty,
fstockbaseqty as stock_base_qty,
fprojectdetail as project_detail,
b.project_no,
e.project_code as project_code,
f_delivery_date as delivery_date,
f_tax_rate as tax_rate,
f_dnk_jskc as dnk_jskc,
if(nvl(regexp_replace(frowtype,' ',''),'')='',null,frowtype) as row_type,
fparentmatid as parent_mat_id,
fparentbomid as parent_bom_id,
if(nvl(regexp_replace(fbarcode,' ',''),'')='',null,fbarcode)  as bar_code,
f_abc_checkbox1 as abc_checkbox1,
if(nvl(regexp_replace(f_abc_text,' ',''),'')='',null,f_abc_text) as abc_text,
fstockbaseapjoinqty as stock_base_ap_join_qty,
fapjoinamount as ap_join_amount,
f_abc_jskc as abc_jskc,
f_ydie_jskc as ydie_jskc,
f_abc_decimal as abc_decimal,
fpurchase_applicant as purchase_applicant,
f_abc_integer as abc_integer,
c.fpricecoefficient as finance_price_coefficient,
c.fprice as finance_price,
c.ftaxrate as finance_tax_rate,
c.ftaxprice as finance_tax_price,
c.fpriceunitid as finance_price_unitid,
c.fpriceunitqty as finance_price_unit_qty,
c.fdiscountrate as finance_discount_rate,
c.ftaxnetprice as finance_tax_net_price,
c.famount as finance_amount,
c.famount_lc as finance_amount_lc,
c.fallamount as finance_all_amount,
c.fallamount_lc as finance_all_amount_lc,
c.fdiscount as finance_discount,
c.fpayorgid as finance_pay_org_id,
c.fsettleorgid as finance_settle_org_id,
c.frowcost as finance_row_cost,
c.ftaxcombination as finance_tax_combination,
c.ftaxamount as finance_tax_amount,
c.ftaxamount_lc as finance_tax_amount_lc,
c.fpayconditionid as finance_pay_conditionid,
c.fsysprice as finance_sys_price,
c.fupprice as finance_up_price,
c.fdownprice as finance_down_price,
c.fbilldisapportion as finance_bill_dis_apportion,
c.fentrydiscountallot as finance_entry_discount_allot,
c.fbefbilldisallamt as finance_bef_bill_disallamt,
c.fbefbilldisamt as finance_bef_bill_disamt,
c.fbefdisamt as finance_bef_disamt,
c.fbefdisallamt as finance_bef_disallamt,
c.fsettletypeid as finance_settle_type_id,
c.fmaxprice as finance_max_price,
c.fminprice as finance_min_price,
c.fdefaultsettleorgid as finance_default_settle_org_id,
c.fchargeprojectid as finance_charge_project_id,
c.fcentsettleorgid as finance_centsettle_org_id,
c.fdispsettleorgid as finance_dispsettle_org_id,
c.fpricebaseqty as finance_price_base_qty,
c.fsetpriceunitid as finance_setprice_unitid,
c.fpricelistentry as finance_price_list_entry,
c.fpricediscount as finance_price_discount,
c.fprilstentryid as finance_prilst_entry_id
from 
${ods_dbname}.ods_qkt_kde_pur_poorder_entry_df a
left join (select id,project_code,project_no from ${dwd_dbname}.dwd_kde_pur_poorder_info_df where d='${pre1_date}') b on a.fid=b.id
left join ${ods_dbname}.ods_qkt_kde_pur_poorderentry_finance_df c on a.fentryid=c.fentryid and c.d='${pre1_date}'
left join ${dim_dbname}.dim_kde_bd_project_info e on a.fprojectdetail=e.id
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

