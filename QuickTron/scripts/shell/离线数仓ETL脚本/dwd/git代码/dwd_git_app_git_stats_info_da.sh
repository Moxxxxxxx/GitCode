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
#-- 2 wangziming 2021-11-16 modify 增加邮箱字段 git_user_email
#-- 3 wangziming 2022-03-10 modify 增加去重逻辑
#-- 4 wangziming 2022-06-30 modify 重新进行初始化以及增加提交者原始邮箱（修改整体唯一键）ctime、repo、author、author_email 作为联合主键
#-- 5 wangziming 2022-07-28 modify 修改拼接提交者邮箱的逻辑
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
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;



insert overwrite table dwd.dwd_git_app_git_stats_info_da partition(d)
select
  a.id,
  a.ctime as git_create_date,
  a.repo as git_repo,
  a.author as git_author,
  a.add_lines as add_lines_count,
  a.removed_lines as removed_lines_count,
  a.total_lines as total_lines_count,
case when b.author_ename is not null then concat(b.author_ename,'@flashhold.com') 
       else concat(regexp_replace(lower(split(a.author,'@')[0]),'[^a-z1-9\\\\u4e00-\\\\u9fa5]',''),'@flashhold.com') end as git_user_email,
  a.author_email as git_author_email,
  a.ctime as d
from 
${ods_dbname}.ods_qkt_git_app_gitstats_da a
left join ${dim_dbname}.dim_git_used_author_offine b on a.author=b.used_author
;
"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_git_app_git_stats_info_da partition(d)
select
  a.id,
  a.ctime as git_create_date,
  a.repo as git_repo,
  a.author as git_author,
  a.add_lines as add_lines_count,
  a.removed_lines as removed_lines_count,
  a.total_lines as total_lines_count,
  case when b.author_ename is not null then concat(b.author_ename,'@flashhold.com') 
       --else concat(regexp_replace(lower(split(a.author,'@')[0]),'[^a-z1-9\\\\u4e00-\\\\u9fa5]',''),'@flashhold.com') end as git_user_email,
     else concat(a.author,'@flashhold.com') end as git_user_email,
  a.author_email as git_author_email,
  a.ctime as d

from 
(
select
*,
row_number() over(partition by ctime,repo,author,author_email order by id asc) as rn
from
${ods_dbname}.ods_qkt_git_app_gitstats_da
where d='${pre1_date}'
) a
left join ${dim_dbname}.dim_git_used_author_offine b on  array_contains(split(b.used_author,','),a.author)
where a.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
