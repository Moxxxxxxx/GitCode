#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

dbname=ods
dim_dbname=dim
hive=/opt/module/hive-3.1.2/bin/hive

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

insert overwrite table $dim_dbname.dim_rcs_agv_type
select 
b.agv_type_code,
b.agv_type_name,
a.project_code,
date_sub(current_date(),1) as time_value
from 
dwd.dwd_rcs_basic_agv_info a
left join dwd.dwd_rcs_basic_agv_type_info b
on a.agv_type_id =b.id and a.project_code=b.project_code
where a.project_code<>'test_demo'
group by b.agv_type_code
,b.agv_type_name
,a.project_code;
"
#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"
#################################################################export#################################################################################
## MYSQL

#!/bin/bash

HOSTNAME="007.bg.qkt"                                         #数据库信息
PORT="3306"
USERNAME="dataplatform"
PASSWORD="quicktron_1014#"
DBNAME="evo_wds_base"                                           #数据库名称


/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/$DBNAME?useUnicode=true&characterEncoding=utf-8" \
--username $USERNAME \
--password $PASSWORD \
--table avg_type \
--columns "agv_type,project_code,time_value" \
--hcatalog-database dim \
--hcatalog-table dim_rcs_agv_type \
--input-fields-terminated-by "\t" \
--num-mappers 1  

