#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： datatron-生产计划线下表
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_datatron_production_plan_df,ods_qkt_dtk_standard_working_hour_df
#-- 输出表 ：dim.dim_product_plan_info_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-19 CREATE 
#-- 2 wangziming 2023-01-04 modify 增加字段单个标准工作工时 standard_working_hour
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

echo "##############################################hive:{start executor dim}####################################################################"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with tmp_product_plan_str1 as (
select 
*,substr(start_month,1,8) as m
from 
(
select 
*,row_number() over(partition by project_code,work_order_number,material_number,start_month order by id desc) as rn
from 
${ods_dbname}.ods_qkt_datatron_production_plan_df 
where d='${pre1_date}'
) t
where t.rn=1
),
tmp_standard_working_hour_str1 as (
select 
business_id,
regexp_replace(process,'\\\\s+','') as product_process, 
regexp_replace(product_part_number,'\\\\s+','') as product_part_number, 
regexp_replace(model_code,'\\\\s+','') as model_code,
standard_time as standard_working_hour
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
)
insert overwrite table ${dim_dbname}.dim_product_plan_info_offline
select 
a.id,
split(upper(a.project_code),'/')[0] as project_code,
a.project_name,
a.work_order_number,
a.material_number,
a.machine_type,
a.\`group\` as group_name,
a.name,
a.queue_number,
b.label as start_date,
substr(a.start_month,1,7) as start_month,
b.value as plan_num
from 
tmp_product_plan_str1 a
lateral view explode(
map(
concat(a.m,'01'),a.\`first\`, 
concat(a.m,'02'),a.\`second\`, 
concat(a.m,'03'),a.third, 
concat(a.m,'04'),a.fourth, 
concat(a.m,'05'),a.fifth, 
concat(a.m,'06'),a.sixth, 
concat(a.m,'07'),a.seventh, 
concat(a.m,'08'),a.eighth, 
concat(a.m,'09'),a.ninth, 
concat(a.m,'10'),a.tenth, 
concat(a.m,'11'),a.eleventh, 
concat(a.m,'12'),a.twelfth, 
concat(a.m,'13'),a.thirteenth, 
concat(a.m,'14'),a.fourteenth, 
concat(a.m,'15'),a.fifteenth, 
concat(a.m,'16'),a.sixteenth, 
concat(a.m,'17'),a.seventeenth, 
concat(a.m,'18'),a.eighteenth, 
concat(a.m,'19'),a.nineteenth, 
concat(a.m,'20'),a.twentieth, 
concat(a.m,'21'),a.twenty_first, 
concat(a.m,'22'),a.twenty_second, 
concat(a.m,'23'),a.twenty_third, 
concat(a.m,'24'),a.twenty_fourth, 
concat(a.m,'25'),a.twenty_fifth, 
concat(a.m,'26'),a.twenty_sixth, 
concat(a.m,'27'),a.twenty_seventh, 
concat(a.m,'28'),a.twenty_eighth, 
concat(a.m,'29'),a.twenty_ninth, 
concat(a.m,'30'),a.thirtieth, 
concat(a.m,'31'),a.thirtieth_first
)) b as label,value
where b.value is not null
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

