# MYSQL
HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="dataplatform"
PASSWORD="quicktron_1014#"
DBNAME="ads"  


ssh -tt 008.bg.qkt <<effo

#插入数据
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "
delete from ads.ads_project_healthy_info where date(create_time) = date(sysdate());
"
exit
effo


##表：ads_project_healthy_info    --项目健康度
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_healthy_info \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_healthy_info \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,date_zone,project_code,project_name,product_name,project_ft,project_area,project_priority,project_operation_state,project_activation_sevendays,risk_level,workorder_num_sevendays,unsolve_workorder_num_sevendays,agv_breakdown_num_sevendays,agv_MTBF_sevendays,service_downtime_sevendays,system_breakdown_num_sevendays,upgrade_num_sevendays,create_time,update_time"