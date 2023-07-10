#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops的基础项目信息表
#-- 注意 ： 每日全量数据
#-- 输入表 :dwd.dwd_share_project_base_info_df、dwd.dwd_bpm_app_K3flowentry_info_ful、dwd.dwd_bpm_technical_scheme_review_info_ful、dwd.dwd_bpm_project_info_ful、dwd.dwd_bpm_app_k3flow_info_ful
#-- 输出表 ：ads.ads_devops_project_base_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-03 CREATE 
# ------------------------------------------------------------------------------------------------


dwd_dbname=dwd
ads_dbname=ads
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

echo "##############################################hive:{start executor ads}####################################################################"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with tmp_devops_project_base_str1 as (
select
a.string1 as project_code,
b.string7 as personnel_type,
b.string9 as implementation_consultant
from 
${dwd_dbname}.dwd_bpm_app_k3flow_info_ful a
left join ${dwd_dbname}.dwd_bpm_app_K3flowentry_info_ful b on a.flowid =b.flowid 
where a.oflowmodelid='81679'
and b.string7='实施顾问'
),
tmp_devops_project_base_str2 as (
select 
if(nvl(b.mproject_code,'')<>'',b.project_code,a.pre_sale_code ) as project_code,
product_module
from 
${dwd_dbname}.dwd_bpm_technical_scheme_review_info_ful a
left join ${dwd_dbname}.dwd_bpm_project_info_ful b on upper(a.pre_sale_code)=b.mproject_code
)
insert overwrite table ${ads_dbname}.ads_devops_project_base_detail
select 
a.project_code,
a.project_name,
a.project_operation_state,
c.product_module as project_product,
'-1' as is_customized,
e.priority as project_level,
null as region_or_ft,
null as region_head,
b.implementation_consultant,
a.project_manager,
null as project_member
from 
${dwd_dbname}.dwd_share_project_base_info_df a
left join tmp_devops_project_base_str1 b on a.project_code=b.project_code
left join tmp_devops_project_base_str2 c on a.project_code=c.project_code
left join ${dwd_dbname}.dwd_bpm_project_info_ful e on a.project_code=e.project_code
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"