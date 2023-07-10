#!/bin/bash
## 设置环境变量
export HCAT_HOME=/opt/module/hive/hcatalog
export PATH=$PATH:$HCAT_HOME/bin


dbname=quicktronft_db
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else 
    do_date=`date -d "-1 day" +%F`
fi



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

use $dbname;

insert overwrite table dim_bpm_project_dict
select 
pcode as project_code,
pname as project_cname
from 
(
select 
pcode,
pname,
row_number() over(partition by pcode order by id desc) as rn
from 
quicktronft_db.ods_bpm_pm_project
) a
where rn=1
"
$hive -e "$sql"

