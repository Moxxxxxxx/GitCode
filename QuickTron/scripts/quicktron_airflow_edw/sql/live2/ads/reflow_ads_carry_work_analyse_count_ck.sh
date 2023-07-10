#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "
\--readerPlugin rdbmsreader 
\--jdbcUrl jdbc:hive2://003.bg.qkt:10000/ads
\--userName wangyingying
\--passWord wangyingying4
\--querySql select upstream_work_id, work_id, work_path, start_point, target_point, work_state, first_classification, first_classification_desc, agv_type_code, agv_code, robot_num, wotk_duration_total, robot_assign_duration, robot_move_duration, station_executor_duration, work_create_time, work_complete_time,project_code from ads.ads_carry_work_analyse_count where d>=date_sub('\${pre1_date}',7)
\--separator 
\--writerPlugin clickhousewriter 
\--column upstream_work_id, work_id, work_path, start_point, target_point, work_state, first_classification, first_classification_desc, agv_type_code, agv_code, robot_num, wotk_duration_total, robot_assign_duration, robot_move_duration, station_executor_duration, work_create_time, work_complete_time,project_code
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads
\--table local_ads_carry_work_analyse_count 
\--passWord pPTqoeOVaWJ8pQ==
\--userName super_user 
\--preSql alter table ads.local_ads_carry_work_analyse_count delete where toDate(substring(work_create_time,1,10))>=toDate(date_sub(toDate('\${pre1_date}'),interval 7 day))
\--channel 1" "reflow_ads_carry_work_analyse_count_ck"
