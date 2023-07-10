#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp 物料基础信息表
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_kde_bd_material_di,ods_qkt_kde_bd_material_lang_df
#-- 输出表 : dwd.dwd_kde_bd_material_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-30 CREATE 
#-- 2 wangziming 2023-02-22 modify 增加物料信息
#-- 3 wangziming 2023-03-02 modify 增加物料规格型号
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


insert overwrite table ${dwd_dbname}.dwd_kde_bd_material_info_df partition(d='${pre1_date}')
select 
t.material_id,
t.material_number,
t.material_old_number,
t.mnemonic_code,
t.master_id,
t.material_group,
t.create_org_id,
t.use_org_id,
t.creator_id,
t.create_time,
t.modifier_id,
t.modify_time,
t.document_status,
t.is_forbid,
t.approver_id,
t.approve_time,
t.forbidder_id,
t.fforbid_time,
t.image,
t.plm_material_id,
t.material_src,
t.image_file_server,
t.img_storage_type,
t.is_sales_by_net,
t.paez_text,
t.remark,
t.mass_production,
t.paez_text1,
t.paez_checkbox,
t.paez_integer,
t.paez_checkbox1,
t.refstatus,
t.is_autoallocate,
t.spu_id,
t.pinyin,
t.abc_text,
t1.material_name,
case when t1.specification in('-',' ','/') then null
	 when nvl(t1.specification,'')='' then null
	 else regexp_replace(t1.specification,'\t|\r|\n','') end as material_spec_model
from 
(
select 
*
from 
(
select 
*,
row_number() over(partition by material_id order by modify_time desc ) as rn
from 
(
select 
fmaterialid as material_id,
fnumber as material_number,
if(nvl(regexp_replace(foldnumber,' ',''),'')='',null,foldnumber) as material_old_number,
if(nvl(regexp_replace(fmnemoniccode,' ',''),'')='',null,fmnemoniccode) as mnemonic_code,
fmasterid as master_id,
fmaterialgroup as material_group,
fcreateorgid as create_org_id,
fuseorgid as use_org_id,
fcreatorid as creator_id,
fcreatedate as create_time,
fmodifierid as modifier_id,
fmodifydate as modify_time,
fdocumentstatus as document_status,
case when upper(fforbidstatus)='A' then '1'
	 when upper(fforbidstatus)='B' then '0'
	else '-1' end as is_forbid,
fapproverid as approver_id,
fapprovedate as approve_time,
fforbidderid as forbidder_id,
fforbiddate as fforbid_time,
fimage as image,
fplmmaterialid as plm_material_id,
fmaterialsrc as material_src,
if(nvl(regexp_replace(fimagefileserver,' ',''),'')='',null,fimagefileserver) as image_file_server,
fimgstoragetype as img_storage_type,
fissalesbynet as is_sales_by_net,
if(nvl(regexp_replace(f_paez_text,' ',''),'')='',null,f_paez_text) as paez_text,
if(nvl(regexp_replace(fremark,' ',''),'')='',null,fremark) as remark,
if(nvl(regexp_replace(fmassproduction,' ',''),'')='',null,fmassproduction)  as mass_production,
if(nvl(regexp_replace(f_paez_text1,' ',''),'')='',null,f_paez_text1)  as paez_text1,
f_paez_checkbox as paez_checkbox,
f_paez_integer as paez_integer,
f_paez_checkbox1 as paez_checkbox1,
frefstatus as refstatus,
fisautoallocate as is_autoallocate,
fspuid as spu_id,
if(nvl(regexp_replace(fpinyin,' ',''),'')='',null,fpinyin) as pinyin,
if(nvl(regexp_replace(f_abc_text,' ',''),'')='',null,f_abc_text) as abc_text
from 
${ods_dbname}.ods_qkt_kde_bd_material_di 
where d='${pre1_date}'

union all 
select 
material_id,
material_number,
material_old_number,
mnemonic_code,
master_id,
material_group,
create_org_id,
use_org_id,
creator_id,
create_time,
modifier_id,
modify_time,
document_status,
is_forbid,
approver_id,
approve_time,
forbidder_id,
fforbid_time,
image,
plm_material_id,
material_src,
image_file_server,
img_storage_type,
is_sales_by_net,
paez_text,
remark,
mass_production,
paez_text1,
paez_checkbox,
paez_integer,
paez_checkbox1,
refstatus,
is_autoallocate,
spu_id,
pinyin,
abc_text
from 
${dwd_dbname}.dwd_kde_bd_material_info_df
) a
) b
where rn=1
) t
left join ${ods_dbname}.ods_qkt_kde_bd_material_lang_df t1 on t1.material_id=t.material_id and t1.d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

