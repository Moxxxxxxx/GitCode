select 
count(distinct id) as total_recovery_num,
count(distinct case when `result` = 1 then id end)  as success_recovery_num,
count(distinct case when `result` = 0 then id end)  as fail_recovery_num,
avg(unix_timestamp(coalesce(end_time,sysdate()))- unix_timestamp(start_time)) as avg_recovery_duration
from phoenix_rms.robot_recovery_record
where `method` = '自恢复' 
and start_time BETWEEN {now_start_time} and {now_end_time}