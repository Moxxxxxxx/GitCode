#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： pms-小车开箱自检信息表
#-- 注意 ： 
#-- 输入表 : ods_qkt_pms_open_package_check_df、ods_qkt_pms_user_info_df、ods_qkt_pms_project_info_df、ods_qkt_pms_sales_territory_df
#-- 输出表 ：dwd.dwd_pms_open_package_check_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-11-25 CREATE 
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



insert overwrite table ${dwd_dbname}.dwd_pms_open_package_check_info_df partition(d='${pre1_date}')
select  
a.id,
a.tdrpe as pe_id,
c.lastname as pe_name,
a.tdsj as check_date,
a.djbh as check_number,
a.fhtzdhwb as shipments_order_no,
a.fhpc as shipments_batch,
upper(b.xmbm) as project_code,
regexp_replace(b.xmmc,'\t|\t|\n| ','') as project_name,
upper(b.sqbh) as pre_sale_code,
e.lastname as project_manager,
a.xmjd as project_stage,
case when a.sffb='是' then '0'
     when a.sffb='否' then '1'
     else null end as is_yjt,
a.cjh as agv_uuid,
a.agvlx as agv_type,
a.xh as agv_model_type,
a.cslcskm as init_mileage,
a.cswchlcxskm as test_finish_mileage,
case when a.kxsfhg='合格' then '1'
     when a.kxsfhg='不合格' then '0'
     else null end as is_open_package_pass,
regexp_replace(a.sm,'\t|\n|\r','') as remark_desc,
a.kxdjbmb as check_package_template,
a.kxlxyc as check_package_type,
case when a.kxdjjg='合格' then '1'
     when a.kxdjjg='不合格' then '0'
     else null end as is_check_pass,
case when a.kxzzjg='合格' then '1'
     when a.kxzzjg='不合格' then '0'
     else null end as is_final_open_package_pass,
a.mxjghj as detail_result_count,
a.erpcjh as erp_agv_uuid,
a.agvlxerp as erp_agv_type,
split(f.sjqy,' ')[0] as project_area,
a.gngw as project_region
from 
${ods_dbname}.ods_qkt_pms_open_package_check_df a
left join ${ods_dbname}.ods_qkt_pms_project_info_df b on a.xmbm=b.id and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_user_info_df c on a.tdrpe=c.id and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_user_info_df e on a.tdrpe=e.id and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_sales_territory_df f on a.xsqy=f.id and f.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
