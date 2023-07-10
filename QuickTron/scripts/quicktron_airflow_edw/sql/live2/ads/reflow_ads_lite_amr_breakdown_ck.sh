#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "
\--readerPlugin rdbmsreader 
\--jdbcUrl jdbc:hive2://003.bg.qkt:10000/ads
\--userName wangyingying
\--passWord wangyingying4
\--querySql select data_time,breakdown_id,amr_code,amr_type,carry_order_num,right_order_num,amr_task,total_charge,exc_charge,error_duration,mttr_error_duration,mttr_error_num,start_time,end_time,actual_duration,project_code,happen_time from ads.ads_lite_amr_breakdown where d>=date_sub('\${pre1_date}',7)
\--separator 
\--writerPlugin clickhousewriter 
\--column data_time,breakdown_id,amr_code,amr_type,carry_order_num,right_order_num,amr_task,total_charge,exc_charge,error_duration,mttr_error_duration,mttr_error_num,start_time,end_time,actual_duration,project_code,happen_time
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads
\--table local_ads_lite_amr_breakdown 
\--passWord pPTqoeOVaWJ8pQ==
\--userName super_user 
\--preSql alter table ads.local_ads_lite_amr_breakdown delete where toDate(formatDateTime(happen_time,'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-7)
\--channel 1" "reflow_ads_lite_amr_breakdown_ck"
