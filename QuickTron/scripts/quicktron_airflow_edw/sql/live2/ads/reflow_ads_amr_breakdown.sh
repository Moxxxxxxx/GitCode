#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select data_time,project_code,happen_time,type_class,amr_type,amr_type_des,amr_code,breakdown_id,if(a.breakdown_id_single='UNKNOWN',null,a.breakdown_id_single) as breakdown_id_single,ROW_NUMBER() over(PARTITION BY project_code,happen_time,type_class,amr_type,amr_type_des,amr_code,breakdown_id ORDER BY a.breakdown_id_single ASC) AS breakdown_id_sort,carry_order_num,carry_task_num,theory_time,error_duration,mttr_error_duration,mttr_error_num,add_mtbf,d,pt 
           from ads.ads_amr_breakdown 
           lateral view explode(split(if(nvl(breakdown_id,'')='','UNKNOWN',breakdown_id),',')) a as breakdown_id_single
		   where pt = 'A51488' and d >= date_sub('\${pre1_date}',3) and d <= '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=spark;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column data_time,project_code,happen_time,type_class,amr_type,amr_type_des,amr_code,breakdown_id,breakdown_id_single,breakdown_id_sort,carry_order_num,carry_task_num,theory_time,error_duration,mttr_error_duration,mttr_error_num,add_mtbf,d,pt
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_amr_breakdown 
\--preSql delete from ads_amr_breakdown where (d >= DATE_SUB('\${pre1_date}',INTERVAL 3 DAY) and d <= '\${pre1_date}') or d <= DATE_SUB('\${pre1_date}',INTERVAL 99 DAY)
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_amr_breakdown"




