#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： report可视化报表的用户维表
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_report_ab_user_ful、ods.ods_qkt_report_ab_role_ful、ods_qkt_report_ab_user_role_ful
#-- 输出表 ：dim.dim_report_dashboard_user_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-12 CREATE 
#-- 2 wangziming 2021-12-16 modify 增加用户于角色权限对应的关系字段

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



insert overwrite table ${dim_dbname}.dim_report_dashboard_user_info
select
a.id, 
if(a.first_name='admin',a.first_name,concat(a.last_name,a.first_name)) as user_name,
a.username as user_login_name, 
a.password as user_login_password,
a.email as user_email, 
a.active as is_active,
b.role_id,
b.role_cname
from 
${ods_dbname}.ods_qkt_report_ab_user_ful a
left join (select 
t1.user_id,
concat_ws(',',collect_set(t1.role_id)) as role_id,
concat_ws(',',collect_set(t2.name)) as role_cname
from ${ods_dbname}.ods_qkt_report_ab_user_role_ful t1
left join 
${ods_dbname}.ods_qkt_report_ab_role_ful t2 on t1.role_id=t2.id
group by t1.user_id
) b on a.id=b.user_id
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

