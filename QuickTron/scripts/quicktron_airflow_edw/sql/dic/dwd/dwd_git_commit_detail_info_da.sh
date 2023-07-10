#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 生成 git代码明细贡献量
#-- 注意 ： 每天的分区即为每天代码的贡献数据
#-- 输入表 : ods.ods_qkt_git_commit_detail_da
#-- 输出表 ：dwd.dwd_git_commit_detail_info_da
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-22 CREATE
#-- 2 wangziming 2022-09-01 modify 修改回流五天数据
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


insert overwrite table ${dwd_dbname}.dwd_git_commit_detail_info_da partition(d)
select
a.id,
a.git_commit_date,
a.git_commit_id,
a.git_repository,
a.git_commit_time,
a.git_author,
a.git_author_email,
a.git_branch,
regexp_replace(a.git_commit_desc,'\r|\n|\t','') as git_commit_desc,
a.add_lines_count,
a.removed_lines_count,
a.total_lines_count,
a.change_files_count,
a.is_no_merge,
case when b.author_ename is not null then concat(b.author_ename,'@flashhold.com') 
     else concat(a.git_author,'@flashhold.com') end as git_user_email,
a.git_commit_date as d
from 
(
select
*,
row_number() over(partition by git_commit_date,git_commit_id,git_repository order by id asc) as rn
from
${ods_dbname}.ods_qkt_git_commit_detail_da
where d>=date_sub('${pre1_date}',4)
) a
left join ${dim_dbname}.dim_git_used_author_offine b on  array_contains(split(b.used_author,','),a.git_author)
where a.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

