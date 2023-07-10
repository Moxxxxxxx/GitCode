#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_devops_envdeploy_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_devops_envdeploy_detail （devops环境部署明细）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_devops_envdeploy_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_devops_envdeploy_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "env_id,env_deploy_name,status_id,deploy_status,deploy_progress,owner_id,owner_name,server_master_ip,server_slave_ip,deploy_date,deploy_starttime,deploy_endtime,deploye_duration_s,dtk_user_name,team_ft,team_group,team_sub_group,emp_position"