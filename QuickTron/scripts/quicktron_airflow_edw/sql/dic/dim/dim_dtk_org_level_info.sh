#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉组织层级关系信息维表
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_dtk_dingtalk_department_level_df
#-- 输出表 ：dim.dim_dtk_org_level_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 
#-- 2 wangziming 2021-12-30 modify 新增org_company_name 字段
#-- 3 wangziming 2022-01-10 modify 新增org_role_tye 字段
#-- 4 wangziming 2022-10-10 modify 过滤出 《上海快仓智能科技有限公司》组织的数据
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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

add jar /data/hive/jar/hie-udf-1.0-SNAPSHOT.jar;
create temporary function udf_concat_str as 'com.quicktron.controll.ConcatStrUDF';

insert overwrite table ${dim_dbname}.dim_dtk_org_level_info
select 
a.org_id,
a.org_name,
a.org_level_num,
a.org_id_1,
a.org_name_1,
a.org_id_2,
a.org_name_2,
a.org_id_3,
a.org_name_3,
a.org_id_4,
a.org_name_4,
a.org_id_5,
a.org_name_5,
a.org_id_6,
a.org_name_6,
a.org_id_7,
a.org_name_7,
a.org_id_8,
a.org_name_8,
a.org_id_9,
a.org_name_9,
a.org_id_10,
a.org_name_10,
a.org_path_id,
a.org_path_name,
a.org_company_name,
b.org_role_tye,
1 as is_valid
from 
(
select
id as org_id, 
name as org_name, 
level_num as org_level_num,
l1_id as org_id_1,
l1_name as org_name_1, 
l2_id as org_id_2, 
l2_name as org_name_2, 
l3_id as org_id_3, 
l3_name as org_name_3, 
l4_id as org_id_4, 
l4_name as org_name_4, 
l5_id as org_id_5, 
l5_name as org_name_5, 
l6_id as org_id_6, 
l6_name as org_name_6, 
l7_id as org_id_7, 
l7_name as org_name_7, 
l8_id as org_id_8, 
l8_name as org_name_8, 
l9_id as org_id_9, 
l9_name as org_name_9, 
l10_id as org_id_10, 
l10_name as org_name_10,
regexp_replace(udf_concat_str('/','id',l1_id,l2_id,l3_id,l4_id,l5_id,l6_id,l7_id,l8_id,l9_id,l10_id),'/id','') as org_path_id,
regexp_replace(udf_concat_str('/','name',l1_name,l2_name,l3_name,l4_name,l5_name,l6_name,l7_name,l8_name,l9_name,l10_name),'/name','') as org_path_name,
org_name as org_company_name
from 
${ods_dbname}.ods_qkt_dtk_dingtalk_department_level_df 
where d='${pre1_date}' and org_name='上海快仓智能科技有限公司'
) a 
left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='1') b on a.org_path_name=b.org_path_name
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

