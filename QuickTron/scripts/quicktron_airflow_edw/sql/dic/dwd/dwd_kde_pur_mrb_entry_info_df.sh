#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 采购订单表头-存储采购订单主信息
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_pur_mrb_entry_df,dwd.dwd_kde_pur_mrb_info_df、ods.ods_qkt_kde_pur_mrb_entry_finance_df
#-- 输出表 : dwd.dwd_kde_pur_mrb_entry_info_df
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


insert overwrite table ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df partition(d='${pre1_date}')
select 
a.fentryid as entry_id,
a.fid as id,
fseq as seq,
fmaterialid as material_id,
funitid as unit_id,
frmmustqty as rm_must_qty,
frmrealqty as rm_real_qty,
freplenishqty as replenish_qty,
fkeapamtqty as keapamt_qty,
fstockid as stock_id,
fstocklocid as stockloc_id,
fstockstatusid as stock_status_id,
fauxpropid as aux_prop_id,
fbomid as bom_id,
flot as lot,
flot_text as lot_text,
if(nvl(regexp_replace(fsupplierlot,' ',''),'')='',null,fsupplierlot) as supplier_lot,
fproducedate as produce_date,
fexpperiod as exp_period,
if(nvl(regexp_replace(fexpunit,' ',''),'')='',null,fexpunit) as exp_unit,
fexpirydate as expiry_date,
fbaseunitid as base_unit_id,
fbasereplayqty as base_replay_qty,
fbaseunitqty as base_unit_qty,
fauxunitid as aux_unit_id,
fauxunitqty as aux_unit_qty,
fbasejoinqty as base_join_qty,
fshelflife as shelf_life,
fsrcbilltypeid as src_bill_type_id,
fsrcfid as src_fid,
fsrcbillno as src_bill_no,
fsrcrowid as src_row_id,
fsrcseq as src_seq,
if(nvl(regexp_replace(fcontractno,' ',''),'')='',null,fcontractno) as contract_no,
if(nvl(regexp_replace(forderno,' ',''),'')='',null,forderno) as order_no,
if(nvl(regexp_replace(freqtraceno,' ',''),'')='',null,freqtraceno) as reqtrace_no,
fnote as note,
fstockflag as stock_flag,
fownertypeid as owner_type_id,
fownerid as owner_id,
fkeepertypeid as keeper_type_id,
fkeeperid as fkeeper_id,
fjoinqty as join_qty,
fbasepoqty as base_po_qty,
fbaseapjoinqty as base_ap_join_qty,
if(nvl(regexp_replace(fbflowid,' ',''),'')='',null,fbflowid) as flow_id,
fbasekeapamtqty as base_keapamt_qty,
freceivestockid as receive_stock_id,
freceivestocklocid as receive_stockloc_id,
freceivestockstatusid as receive_stock_status_id,
freceivelot as receive_lot,
if(nvl(regexp_replace(freceivelot_text,' ',''),'')='',null,freceivelot_text) as receive_lot_text,
freceivestockflag as receive_stock_flag,
fporequireorgid as porequire_org_id,
freceiveauxpropid as receive_aux_prop_id,
fbasejoinsalreturnqty as base_join_sal_return_qty,
fjoinsalreturnqty as join_sal_return_qty,
fextauxunitid as ext_aux_unit_id,
fextauxunitqty as ext_aux_unit_qty,
fisreceiveupdatestock as is_receive_update_stock,
fgiveaway as is_give_away,
fpoorderentryid as poorder_entry_id,
fprojectdetail as project_detail,
b.project_no as project_no,
e.project_code as project_code,
fprojectdetail as bar_code,
c.fpriceunitid as finance_price_unitid,
c.fpriceunitqty as finance_price_unit_qty,
c.fprice as finance_price,
c.ftaxprice as finance_tax_price,
c.fpricecoefficient as finance_price_coefficient,
c.fsysprice as finance_sys_price,
c.fupprice as finance_up_price,
c.fdownprice as finance_down_price,
c.fdiscountrate as finance_discount_rate,
c.fdiscount as finance_discount,
c.ftaxnetprice as finance_tax_net_price,
c.famount as finance_amount,
c.famount_lc as finance_amount_lc,
c.ftaxrate as finance_tax_rate,
c.ftaxamount as finance_tax_amount,
c.ftaxamount_lc as finance_tax_amount_lc,
c.fallamount as finance_all_amount,
c.fallamount_lc as finance_all_amount_lc,
c.fbilldisapportion as finance_bill_disapportion,
c.fbefbilldisamt as finance_bef_bill_disamt,
c.fbefbilldisallamt as fbefbilldisallamt,
c.fbefdisamt as finance_bef_disamt,
c.fbefdisallamt as finance_bef_disallamt,
c.freplenishincltaxamt as finance_replenish_incl_tax_amt,
c.freplenishexcltaxamt as finance_replenish_excl_taxamt,
c.fkeapincltaxamt as finance_keap_incl_tax_amt,
c.fkeapexcltaxamt as finance_keap_excl_tax_amt,
c.fjoinedqty as finance_joined_qty,
c.funjoinqty as finance_unjoin_qty,
c.fjoinedamount as finance_joined_amount,
c.funjoinamount as finance_unjoin_amount,
c.ffullyjoined as finance_is_fully_joined,
c.fjoinstatus as finance_joins_tatus,
c.fbaseunitprice as finance_base_unit_price,
c.fpurcost as finance_pur_cost,
c.finvoicedqty as finance_invoiced_qty,
c.fprocessfee as finance_process_fee,
c.fmaterialcosts as finance_material_costs,
c.ftaxcombination as finance_tax_combination,
c.fcostprice as finance_cost_price,
c.fcostamount as finance_cost_amount,
c.fcostamount_lc as finance_cost_amount_lc,
c.fchargeprojectid as finance_charge_project_id,
c.finvoicedstatus as finance_invoiced_status,
c.finvoicedjoinqty as finance_invoiced_join_qty,
c.fbillingclose as finance_is_billing_close,
c.fcostprice_lc as finance_cost_price_lc,
c.fpricebaseqty as finance_price_base_qty,
c.fsetpriceunitid as finance_setprice_unitid,
c.fcarryunitid as finance_carry_unitid,
c.fcarryqty as finance_carry_qty,
c.fcarrybaseqty as finance_carry_base_qty,
c.fapnotjoinqty as finance_apnot_join_qty,
c.fapjoinamount as finance_apjoin_amount,
c.fpricelistentry as finance_price_list_entry,
c.fprocessfee_lc as finance_process_fee_lc,
c.fmaterialcosts_lc as finance_material_costs_lc
from 
${ods_dbname}.ods_qkt_kde_pur_mrb_entry_df a
left join (select id,project_code,project_no from ${dwd_dbname}.dwd_kde_pur_mrb_info_df where d='${pre1_date}') b on a.fid=b.id 
left join ${ods_dbname}.ods_qkt_kde_pur_mrb_entry_finance_df c on a.fentryid=c.fentryid and c.d='${pre1_date}'
left join ${dim_dbname}.dim_kde_bd_project_info e on a.fprojectdetail=e.id
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

