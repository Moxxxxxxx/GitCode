#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工信息表
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_dkt_dingtalk_user_info_df、dim.dim_dtk_org_info、dim.dim_dtk_org_level_info
#-- 输出表 ：dim.dwd_dtk_emp_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 

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

add jar /data/hive/jar/hie-udf-1.0-SNAPSHOT.jar;
create temporary function udf_concat_str as 'com.quicktron.controll.ConcatStrUDF';

insert overwrite table ${dwd_dbname}.dwd_dtk_emp_info_df partition(d='${pre1_date}')
select
union_id,
open_id, 
remark,
t1.user_id as emp_id, 
if(is_boss='False','0','1') as is_boss, 
hired_date, 
tel as tel_number, 
department as org_ids, 
t2.org_cnames,
t2.org_path_id,
t2.org_path_name,
work_place, 
email,
order_code, 
if(is_leader='False','0','1') as is_leader, 
mobile as mobile_number, 
active as is_active, 
if(is_admin='False','0','1') as is_admin, 
avatar as avatar_url, 
if(is_hide='False','0','1') as is_hide, 
job_number, 
name as emp_name, 
extattr, 
state_code, 
\`position\` as emp_position
from 
${ods_dbname}.ods_qkt_dkt_dingtalk_user_info_df t1 
left join 
(
select 
p1.user_id,
concat_ws(',',collect_list(p1.org_id)) as org_ids,
concat_ws(',',collect_list(p2.org_name)) as org_cnames,
concat_ws(',',collect_list(udf_concat_str('/',p3.org_id,p3.org_id_1,p3.org_id_2,p3.org_id_3,p3.org_id_4,p3.org_id_5,p3.org_id_6,p3.org_id_7,p3.org_id_8,p3.org_id_9,p3.org_id_10))) as org_path_id,
concat_ws(',',collect_list(udf_concat_str('/',p3.org_name,p3.org_name_1,p3.org_name_2,p3.org_name_3,p3.org_name_4,p3.org_name_5,p3.org_name_6,p3.org_name_7,p3.org_name_8,p3.org_name_9,p3.org_name_10))) as org_path_name
from 
(
select 
a.user_id,
b.org_id
from 
${ods_dbname}.ods_qkt_dkt_dingtalk_user_info_df a
lateral view explode(split(a.department,',')) b as org_id
where a.d='${pre1_date}'
) p1
left join ${dim_dbname}.dim_dtk_org_info p2 on p1.org_id=p2.org_id
left join ${dim_dbname}.dim_dtk_org_level_info p3 on p1.org_id=p3.org_id
group by p1.user_id
) t2 on t1.user_id=t2.user_id
where t1.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
