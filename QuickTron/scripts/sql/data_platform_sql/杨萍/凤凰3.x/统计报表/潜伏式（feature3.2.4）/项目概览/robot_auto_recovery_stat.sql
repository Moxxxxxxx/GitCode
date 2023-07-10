select 
    count(distinct id) as automatic_recovery_num
    ,count(distinct case when `result` = 1 then id end)  as automatic_recovery_success_num
    ,ROUND(count(distinct case when `result` = 1 then id end)/count(distinct id),4) AS automatic_recovery_success_rate
    ,count(distinct case when `result` = 0 then id end)  as automatic_recovery_fail_num
    ,ROUND(count(distinct case when `result` = 0 then id end)/count(distinct id),4) AS automatic_recovery_fail_rate
    ,ROUND(avg(case when `result`=1 then unix_timestamp(coalesce(end_time,sysdate()))- unix_timestamp(start_time) end)*1000,3) as automatic_recovery_avg_time
from phoenix_rms.robot_recovery_record
where `method` = '自恢复' 
and start_time BETWEEN {now_start_time} and {now_end_time}
