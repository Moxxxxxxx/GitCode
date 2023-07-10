#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成 git代码贡献量
#-- 注意 ： 每天的分区即为每天代码的贡献数据
#-- 输入表 : ods.ods_qkt_git_app_gitstats_da
#-- 输出表 ：dwd.dwd_git_app_git_stats_info_da
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-03 CREATE 
#-- 2 wangziming 2021-11-05 modify  修改逻辑，将每日git代码提交者信息与线下git用户进行合并（优先取git代码提交者信息）
#-- 3 wangziming 2021-11-16 modify 修改逻辑，关联线下git提交者用户表进行获取用户邮箱

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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table dim.dim_git_auth_user
select 
id,
git_password,
is_superuser,
git_author,
git_first_name,
git_last_name,
git_email,
is_staff,
is_active,
group_id,
git_user_email,
b.uuid as ones_user_uuid
from 
(
select
null as id,
null as git_password,
null as is_superuser,
null as git_author,
null as git_first_name,
null as git_last_name,
a1.email as git_email,
null as is_staff,
null as is_active,
null as group_id,
a1.email as git_user_email
from 
(
select 
distinct
lower(concat(email,'@flashhold.com')) as email
from 
(
select 
distinct
case when b.used_author is not null then b.author_ename
     when a.author like '%fashhold.com' then regexp_replace(a.author,'@flashhold.com','')
     else regexp_replace(a.author,'[^\\\\u4e00-\\\\u9fa5a-z]','') end as email
from ods.ods_qkt_git_app_gitstats_da a
left join dim.dim_git_used_author_offine b on a.author=b.used_author
) tt
) a1
left join ods.ods_qkt_git_auth_user_ful b1 on if(nvl(b1.email,'')='',concat(b1.username,'@flashhold.com'),b1.email) = a1.email
where b1.id is null 

UNION ALL 
select 
id,
password as git_password,
is_superuser,
username as git_author,
first_name as git_first_name,
last_name as git_last_name,
email as git_email,
is_staff,
is_active,
group_id,
if(nvl(email,'')='',concat(username,'@flashhold.com'),email) as git_user_email
from 
ods.ods_qkt_git_auth_user_ful
) t
left join (select uuid,email from ods.ods_qkt_ones_user_df where d='${pre1_date}')  b on t.git_user_email=b.email
;


"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table dim.dim_git_auth_user
select 
id,
git_password,
is_superuser,
git_author,
git_first_name,
git_last_name,
git_email,
is_staff,
is_active,
group_id,
git_user_email,
b.uuid as ones_user_uuid
from 
(
select
null as id,
null as git_password,
null as is_superuser,
null as git_author,
null as git_first_name,
null as git_last_name,
a1.email as git_email,
null as is_staff,
null as is_active,
null as group_id,
a1.email as git_user_email
from 
(
select 
distinct
lower(concat(email,'@flashhold.com')) as email
from 
(
select 
distinct
case when b.used_author is not null then b.author_ename
     when a.author like '%fashhold.com' then regexp_replace(a.author,'@flashhold.com','')
     else regexp_replace(a.author,'[^\\\\u4e00-\\\\u9fa5a-z]','') end as email
from ods.ods_qkt_git_app_gitstats_da a
left join dim.dim_git_used_author_offine b on a.author=b.used_author
) tt
) a1
left join ods.ods_qkt_git_auth_user_ful b1 on if(nvl(b1.email,'')='',concat(b1.username,'@flashhold.com'),b1.email) = a1.email
where b1.id is null 

UNION ALL 
select 
id,
password as git_password,
is_superuser,
username as git_author,
first_name as git_first_name,
last_name as git_last_name,
email as git_email,
is_staff,
is_active,
group_id,
if(nvl(email,'')='',concat(username,'@flashhold.com'),email) as git_user_email
from 
ods.ods_qkt_git_auth_user_ful
) t
left join (select uuid,email from ods.ods_qkt_ones_user_df where d='${pre1_date}')  b on t.git_user_email=b.email
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
