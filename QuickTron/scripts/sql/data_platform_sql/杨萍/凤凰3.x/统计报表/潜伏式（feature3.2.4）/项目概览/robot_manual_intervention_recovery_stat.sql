select `method` as recovery_method,
count(distinct id) as recovery_num
from phoenix_rms.robot_recovery_record
where `method` != '自恢复' and `result` = 1
and start_time BETWEEN {now_start_time} and {now_end_time}
group by `method`