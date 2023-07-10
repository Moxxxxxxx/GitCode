#!/bin/bash
hostname="007.bg.qkt"                                       
port="3306"
username="data_sqoop"
password="quicktron_sqoop"

db_tables="evo_station.station
evo_station.station_point
evo_station.station_event
evo_rcs.basic_agv
evo_rcs.basic_agv_part
evo_rcs.basic_agv_type
evo_rcs.basic_area
evo_rcs.basic_charger
evo_rcs.basic_roller_part
evo_rcs.dsp_system_config
evo_rcs.rcs_agv_bucket
evo_rcs.rcs_agv_charging
evo_rcs.rcs_agv_error_dict
evo_rcs.rcs_agv_parts
evo_basic.basic_bucket_type
evo_basic.basic_bucket"


for a in $db_tables
do
 mysql -h${hostname}  -P${port}  -u${username} -p${password}  -e "
 truncate table $a
"
echo "#####################truncate table ${a}#####################"
done


db_tables_del="evo_rcs.rcs_scan_code_record,date_created
evo_rcs.agv_job_history,update_time
evo_rcs.rcs_agv_path_plan,create_time
evo_rcs.agv_job_event_notification,update_time
evo_rcs.agv_job_sub,update_time
evo_wcs_g2p.job_state_change,updated_date
evo_rcs.agv_job_history,job_finish_time
"

for a in $db_tables_del
do
 mysql -h${hostname}  -P${port}  -u${username} -p${password}  -e "
 delete from  ${a%,*} where substr(${a#*,},1,10)<date_sub(curdate(),interval 30 day);
"
echo "##################### delete from  ${a%,*} where substr(${a#*,},1,10)<date_sub(curdate(),interval 30 day);#####################"
done




echo "##########################################清空数据表done##########################################"
