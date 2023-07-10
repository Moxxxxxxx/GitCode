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
insert overwrite table dim_rcs_agv_type
select 
b.agv_type_code,
b.agv_type_name,
a.project_code,
date_sub(current_date(),1) as time_value
from 
dwd_rcs_basic_agv_info a
left join dwd_rcs_basic_agv_type_info b
on a.agv_type_id =b.id and a.project_code=b.project_code
group by b.agv_type_code
,b.agv_type_name
,a.project_code;
"
$hive -e "$sql"

#################################################################export#################################################################################
## MYSQL

#!/bin/bash

HOSTNAME="172.31.237.2"                                         #数据库信息
PORT="3306"
USERNAME="root"
PASSWORD="tClEDdt6"
DBNAME="evo_wds_base"                                           #数据库名称

#插入数据
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from avg_type;

"
/opt/module/sqoop/bin/sqoop export \
--connect "jdbc:mysql://hadoop102:3306/evo_wds_base?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password tClEDdt6 \
--table avg_type \
--columns "agv_type,project_code,time_value" \
--hcatalog-database $dbname \
--hcatalog-table dim_rcs_agv_type \
--input-fields-terminated-by "\t" \
--num-mappers 1  
