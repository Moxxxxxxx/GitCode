select
    max(create_sys_error_num)                                as new_breakdown_num
     , case 
	       when COALESCE(max(create_sys_error_num),0)=0 then 0
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_order_num) = 0 then concat(max(create_sys_error_num), '/', '0')
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_order_num) >= max(create_sys_error_num) then CONCAT('1 / ', round(max(create_order_num) / max(create_sys_error_num)))
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_order_num) < max(create_sys_error_num) then concat(round(max(create_sys_error_num) / max(create_order_num)), '/','1') 
		   else 0 end   as sys_breakdown_carry_order_rate
     , case 
	       when COALESCE(max(create_sys_error_num),0)=0 then 0
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_job_num) = 0 then concat(max(create_sys_error_num), '/', '0')
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_job_num) >= max(create_sys_error_num) then CONCAT('1 / ', round(max(create_job_num) / max(create_sys_error_num)))
           when COALESCE(max(create_sys_error_num),0)!=0 and max(create_job_num) < max(create_sys_error_num) then concat(round(max(create_sys_error_num) / max(create_job_num)), '/','1') 
		   else 0 end   as sys_breakdown_carry_task_rate		   
    ,max(end_error_time) / max(end_error_num)                 as mttr
    ,NULL AS mtbf
from (select count(distinct id) as create_sys_error_num,
             null               as create_order_num,
             null               as create_job_num,
             null               as end_error_num,
             null               as end_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and start_time BETWEEN {now_start_time} and {now_end_time}
      union all
      select null                         as sys_error_num,
             count(distinct tor.order_no) as create_order_num,
             count(distinct tocj.job_sn)  as create_job_num,
             null                         as end_error_num,
             null                         as end_error_time
      from phoenix_rss.transport_order tor
               left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id 
        where tor.create_time BETWEEN {now_start_time} and {now_end_time}
      union all
      select null                                                       as sys_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             count(distinct id)                                         as end_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time is not null
        and end_time BETWEEN {now_start_time} and {now_end_time}) t 