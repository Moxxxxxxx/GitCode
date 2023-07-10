#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "
\--readerPlugin rdbmsreader 
\--jdbcUrl jdbc:hive2://003.bg.qkt:10000/ads
\--userName wangyingying
\--passWord wangyingying4
\--querySql SELECT breakdown_id,carry_order_num, carry_task_num, amr_type, amr_type_des, mttr_error_num, amr_code, theory_time, error_duration, mttr_error_duration, add_mtbf, type_class, project_code, happen_time FROM ads.ads_amr_breakdown where d>=date_sub('\${pre1_date}',3)
\--separator 
\--writerPlugin clickhousewriter 
\--column breakdown_id,carry_order_num, carry_task_num, amr_type, amr_type_des, mttr_error_num, amr_code, theory_time, error_duration, mttr_error_duration, add_mtbf, type_class, project_code, happen_time
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads
\--table local_ads_amr_breakdown 
\--passWord pPTqoeOVaWJ8pQ==
\--userName super_user 
\--preSql alter table ads.local_ads_amr_breakdown delete where toDate(formatDateTime(happen_time,'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-3)
\--channel 1" "reflow_ads_amr_breakdown_ck"
