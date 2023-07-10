#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉宝仓制程标准工时
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_dtk_standard_working_hour_df
#-- 输出表 ：dwd.dwd_dtk_standard_working_hour_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-01-04 CREATE 
#-- 2 wangziming 2023-01-05 modify 逻辑变更开发
#-- 3 wangziming 2023-01-31 modify 进行天去重保留日期最晚的一条
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
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
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;


with tmp_standard_working_hour_str1 as (
select 
create_time as process_start_time, 
finish_time as process_end_time, 
substr(create_time,1,10) as process_start_date, 
substr(finish_time,1,10) as process_end_date, 
regexp_replace(process,'\\\\s+','') as product_process, 
regexp_replace(product_part_number,'\\\\s+','') as product_part_number, 
regexp_replace(model_code,'\\\\s+','') as model_code,
standard_time as standard_working_hour,
create_time
from 
(
select 
*,
row_number() over(partition by business_id order by finish_time desc) as rn
from 
${ods_dbname}.ods_qkt_dtk_standard_working_hour_df
where d='${pre1_date}'
) t
where t.rn=1
),
tmp_standard_working_hour_str2 as (
select 
product_process,
product_part_number,
null as model_code,
standard_working_hour,
process_start_date as start_date,
if(leg_process_start_date='9999-12-31','9999-12-31',date_sub(leg_process_start_date,1)) as end_date
from 
(
select 
process_start_date,
product_process,
product_part_number,
standard_working_hour,
lead(process_start_date,1,'9999-12-31') over(partition by product_process,product_part_number order by process_start_date asc) as leg_process_start_date,
row_number() over(partition by product_process,product_part_number order by create_time desc) as rn
from 
tmp_standard_working_hour_str1
where nvl(product_part_number,'')<>''
) t
where t.rn=1
),
tmp_standard_working_hour_str3 as (
select 
product_process,
null as product_part_number,
model_code,
standard_working_hour,
process_start_date as start_date,
if(leg_process_start_date='9999-12-31','9999-12-31',date_sub(leg_process_start_date,1)) as end_date
from 
(
select 
process_start_date,
product_process,
model_code,
standard_working_hour,
lead(process_start_date,1,'9999-12-31') over(partition by product_process,model_code order by process_start_date asc) as leg_process_start_date,
row_number() over(partition by product_process,model_code order by create_time desc) as rn
from 
tmp_standard_working_hour_str1
where nvl(product_part_number,'')=''
) t
where t.rn=1
)
insert overwrite table ${dwd_dbname}.dwd_dtk_standard_working_hour_info_df partition(d='${pre1_date}')
select 
product_process,
product_part_number,
model_code,
standard_working_hour,
start_date,
end_date
from 
tmp_standard_working_hour_str2

union all
select 
product_process,
product_part_number,
model_code,
standard_working_hour,
start_date,
end_date
from 
tmp_standard_working_hour_str3
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


