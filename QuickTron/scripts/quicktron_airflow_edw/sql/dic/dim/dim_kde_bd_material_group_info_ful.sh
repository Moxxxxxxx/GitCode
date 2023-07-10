#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： erp（kingdee）物料组别维度表'
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_kde_bd_material_group_df
#-- 输出表 ：dim.dim_kde_bd_material_group_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-30 CREATE 

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


insert overwrite table ${dim_dbname}.dim_kde_bd_material_group_info_ful
select
fid as id,
fnumber as materia_number,
fgroupid as group_id,
fparentid as parent_id,
ffullparentid as full_parent_id,
fleft as f_left,
fright as f_right,
fpointrate as point_rate,
fshowpos as show_pos,
fshowbigscreen as show_big_screen,
if(nvl(regexp_replace(fpictureupload,' ',''),'')='',null,fpictureupload) as picture_upload,
if(nvl(regexp_replace(fpictureurl,' ',''),'')='',null,fpictureurl)  as picture_url
from 
${ods_dbname}.ods_qkt_kde_bd_material_group_df
where d='${pre1_date}'

;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

