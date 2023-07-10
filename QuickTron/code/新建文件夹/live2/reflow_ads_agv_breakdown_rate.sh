#!/bin/bash

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="pre"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from pre_agv_breakdown_rate where date(run_time) = '${pre1_date}';
"
exit
effo


echo "-------------------------------------------------------------------------------------------------向接口表插入数据----------------------------------------------------------------------------------------------- "


##表：故障覆盖率 ads_agv_breakdown_rate （机器人故障分析）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/pre?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table pre_agv_breakdown_rate \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_agv_breakdown_rate/d=${pre1_date}/* \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_num,time_type,agv_type_code,fenzi,fenmu,run_time,err_rate,create_time,update_time"