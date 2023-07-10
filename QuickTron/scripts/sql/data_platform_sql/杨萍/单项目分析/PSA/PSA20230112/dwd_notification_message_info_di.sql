-- dwd_notification_message_info_di


-- insert overwrite table ${dwd_dbname}.dwd_notification_message_info_di partition(d,pt)
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d,
t.project_code as pt
from ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_time desc ) as rn 
from
${ods_dbname}.ods_qkt_notification_message_di 
) t
where t.rn=1
;




with tmp_notification_message_str1 as (
select 
distinct d,project_code
from 
(
select 
distinct substr(happen_at,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_notification_message_di
where d='${pre1_date}' 
and substr(happen_at,0,10)<>'${pre1_date}'

union all
select 
distinct substr(happen_at,0,10) as d,project_code
from 
${ods_dbname}.ods_qkt_rcs_notification_message_di
where d='${pre1_date}' 
and substr(happen_at,0,10)<>'${pre1_date}'
) t
),
tmp_notification_message_str2 as (
select 
b.*
from 
tmp_notification_message_str1 a
inner join ${dwd_dbname}.dwd_notification_message_info_di b on a.d=b.d and a.project_code=b.pt
)

--  insert overwrite table ${dwd_dbname}.dwd_notification_message_info_di partition(d,pt)
select 
id,
agv_code, 
message_id,
unit_type,
warning_type,
message_title,
service_name,
read_status,
notify_status,
notify_event_type,
notify_level,
notify_start_time,
notify_close_time,
message_body,
compress_message_body,
warehouse_id,
notify_created_user,
notify_created_app,
notify_created_time,
notify_updated_user,
notify_updated_app,
notify_updated_time,
project_code,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by notify_updated_time desc) as rn
from 
(
select  
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d
from 
${ods_dbname}.ods_qkt_notification_message_di
where d='${pre1_date}'


union all
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d
from
${ods_dbname}.ods_qkt_rcs_notification_message_di
where d='${pre1_date}'


union all
select 
id,
agv_code, 
message_id,
unit_type,
warning_type,
message_title,
service_name,
read_status,
notify_status,
notify_event_type,
notify_level,
notify_start_time,
notify_close_time,
message_body,
compress_message_body,
warehouse_id,
notify_created_user,
notify_created_app,
notify_created_time,
notify_updated_user,
notify_updated_app,
notify_updated_time,
project_code,
d
from 
tmp_notification_message_str2
) t
) rt 
where rt.rn=1
;