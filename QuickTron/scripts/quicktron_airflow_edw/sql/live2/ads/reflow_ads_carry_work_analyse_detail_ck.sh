#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "
\--readerPlugin rdbmsreader 
\--jdbcUrl jdbc:hive2://003.bg.qkt:10000/ads
\--userName wangyingying
\--passWord wangyingying4
\--querySql select upstream_work_id, work_id, first_classification, first_classification_desc, agv_type_code, agv_code, job_state, job_state_desc, work_update_time, work_duration,project_code from ads.ads_carry_work_analyse_detail where d>=date_sub('\${pre1_date}',7)
\--separator 
\--writerPlugin clickhousewriter 
\--column upstream_work_id, work_id, first_classification, first_classification_desc, agv_type_code, agv_code, job_state, job_state_desc, work_update_time, work_duration,project_code
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads
\--table local_ads_carry_work_analyse_detail 
\--passWord pPTqoeOVaWJ8pQ==
\--userName super_user 
\--preSql alter table ads.local_ads_carry_work_analyse_detail delete where toDate(formatDateTime(toDate(substring(if(empty(work_update_time)=1,null,work_update_time),1,10)),'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-7)
\--channel 1" "reflow_ads_carry_work_analyse_detail_ck"
