#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 销售出库单表体-出库单明细（存储出库单明细物料信息，与出库单1:n对应存储）
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_sal_outstock_entry_df,dwd.dwd_kde_sal_outstock_info_df,ods.ods_qkt_kde_sal_outstock_entry_finance_df
#-- 输出表 : dwd.dwd_kde_sal_outstock_entry_info_df
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


insert overwrite table ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df partition(d='${pre1_date}')
select 
a.fentryid as entry_id,
a.fid as id,
if(nvl(regexp_replace(fcustmatname,' ',''),'')='',null,fcustmatname) as custmat_name,
if(nvl(regexp_replace(fcustmatid,' ',''),'')='',null,fcustmatid) as custmat_id,
fseq as seq,
fmaterialid as material_id,
funitid as unit_id,
fauxpropid as aux_prop_id,
fmustqty as must_qty,
frealqty as real_qty,
fstockid as stock_id,
fstocklocid as stockloc_id,
fstockstatusid as stock_status_id,
flot as lot,
flot_text as lot_text,
fgrossweight as gross_weight,
fnetweight as net_weight,
fbaseunitid as base_unit_id,
fbaseunitqty as base_unit_qty,
fauxunitid as aux_unit_id,
fauxunitqty as aux_unit_qty,
fbomid as bom_id,
fnote as note,
fstockflag as stock_flag,
fownertypeid as owner_type_id,
fownerid as owner_id,
fkeepertypeid as keeper_type_id,
fkeeperid as keeper_id,
if(nvl(regexp_replace(fproducedate,' ',''),'')='',null,fproducedate) as produce_date,
if(nvl(regexp_replace(fexpirydate,' ',''),'')='',null,fexpirydate) as expiry_date,
fbasemustqty as base_must_qty,
case when upper(farrivalstatus)='Y' then '1'
	 when upper(farrivalstatus)='N' then '0'
	else '-1' end as is_arrival,
if(nvl(farrivaldate,'')='',null,farrivaldate) as arrival_date,
farrivalconfirmor as arrival_confirmor,
if(nvl(fbflowid,'')='',null,fbflowid) as flow_id,
frepairqty as repair_qty,
frefuseqty as refuse_qty,
fwantretqty as want_ret_qty,
factqty as act_qty,
fisrepair as is_repair,
if(nvl(regexp_replace(frecnote,' ',''),'')='',null,frecnote) as rec_note,
if(nvl(regexp_replace(freturnnote,' ',''),'')='',null,freturnnote) as return_note,
fsnunitid as sn_unit_id,
fsnqty as sn_qty,
foutcontrol as out_control,
fextauxunitid as ext_aux_unit_id,
fextauxunitqty as ext_aux_unit_qty,
fsrcentryid as src_entry_id,
fprojectdetail as project_detail,
b.project_no,
b.project_code as project_code,
c.fprice as  finance_price,
c.ftaxprice as finance_tax_price,
c.fpricecoefficient as finance_price_coefficient,
c.fsysprice as finance_sys_price,
c.flimitdownprice as finance_limit_down_price,
c.fupprice as finance_up_price,
c.fdownprice as finance_down_price,
c.fpriceunitid as finance_price_unitid,
c.fpriceunitqty as finance_price_unit_qty,
c.ftaxrate as finance_tax_rate,
c.ftaxamount as finance_tax_amount,
c.ftaxamount_lc as finance_tax_amount_lc,
c.ftaxnetprice as finance_tax_net_price,
c.fdiscountrate as finance_discount_rate,
c.fdiscount as finance_discount,
c.fbilldisapportion as finance_bill_disapportion,
c.fbillcostapportion as finance_bill_costapportion,
c.fbefbilldisamt as finance_bef_bill_disamt,
c.fbefbilldisallamt as finance_bef_bill_disallamt,
c.fbefdisamt as finance_bef_disamt,
c.fbefdisallamt as finance_bef_disallamt,
c.famount as finance_amount,
c.famount_lc as finance_amount_lc,
c.fallamount as finance_all_amount,
c.fallamount_lc as finance_all_amount_lc,
c.ftaxcombination as finance_tax_combination,
c.fsalcostprice as finance_sal_cost_price,
c.fcostprice as finance_cost_price,
c.fcostamount as finance_cost_amount,
c.fcostamount_lc as finance_cost_amount_lc,
if(nvl(regexp_replace(c.fisfree,' ',''),'')='',null,c.fisfree)  as finance_is_free,
if(nvl(regexp_replace(c.fisconsumesum,' ',''),'')='',null,c.fisconsumesum)  as finance_is_consume_sum,
if(nvl(regexp_replace(c.fisoverlegalorg,' ',''),'')='',null,c.fisoverlegalorg)  as finance_is_over_egalorg,
c.fsalunitid as finance_sal_unitid,
c.fsalbaseqty as finance_sal_base_qty,
c.fsalunitqty as finance_sal_unit_qty,
c.fpricebaseqty as finance_price_base_qty,
if(nvl(regexp_replace(c.fqualifytype,' ',''),'')='',null,c.fqualifytype) as finance_qualify_type,
c.fsalbasenum as finance_sal_base_num,
c.fstockbaseden as finance_stock_base_den,
c.fsrcbizunitid as finance_src_biz_unitid,
if(nvl(regexp_replace(c.fiscreateprodoc,' ',''),'')='',null,c.fiscreateprodoc) as finance_is_create_pro_doc,
c.fpricelistentry as finance_price_list_entry,
if(nvl(regexp_replace(c.frowtype,' ',''),'')='',null,c.frowtype)  as finance_row_type,
if(nvl(regexp_replace(c.frowid,' ',''),'')='',null,c.frowid) as finance_row_id,
if(nvl(regexp_replace(c.fparentrowid,' ',''),'')='',null,c.fparentrowid) as finance_parent_rowid,
c.fparentmatid as finance_parent_matid,
c.fpricediscount as finance_price_discount,
if(nvl(regexp_replace(c.ftaildiffflag,' ',''),'')='',null,c.ftaildiffflag) as finance_tail_diff_flag,
c.fproprice as finance_pro_price,
c.fproamount as finance_pro_amount
from 
${ods_dbname}.ods_qkt_kde_sal_outstock_entry_df a
left join (select id,project_code,project_no from ${dwd_dbname}.dwd_kde_sal_outstock_info_df where d='${pre1_date}') b on a.fid=b.id
left join ${ods_dbname}.ods_qkt_kde_sal_outstock_entry_finance_df c on a.fentryid=c.fentryid and c.d='${pre1_date}' 
where a.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

