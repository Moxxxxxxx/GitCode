set @start_time ='2023-03-01 00:00:00';
set @end_time ='2023-03-21 23:59:59';
select @start_time,@end_time;

select 
    date_value,
    coalesce (max(job_num),0) as job_num,
    -- coalesce (max(error_num),0) as error_num,
    case when coalesce (max(job_num),0) !=0 then coalesce (max(error_num),0)/coalesce (max(job_num),0) end as job_error_rate
from 
    (
        select 
            date_format(tor.create_time,'%Y-%m-%d')  as date_value,
            count(distinct tocj.job_sn)             as job_num,
            null as error_num 
        from phoenix_rss.transport_order tor
		left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
        inner join phoenix_basic.basic_robot br on br.robot_code =tocj.robot_code and br.usage_state ='using'
        where tor.create_time >= @start_time and tor.create_time < @end_time
		-- and br.robot_code in ('qilin31_14','qilin31_16')
		-- and br.robot_type_code in ('H80A-HBDQR0N-91')
        group by date_value

        union all 
		
        select 
            date_format(bn.start_time,'%Y-%m-%d')  as date_value,
            null as order_num,
            count(distinct bn.id) as error_num
        from phoenix_basic.basic_notification bn
        inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
        where bn.alarm_module ='robot' and bn.alarm_level >=3
        and bn.start_time >= @start_time and bn.start_time <= @end_time
		-- and br.robot_code in ('qilin31_14','qilin31_16')
		-- and br.robot_type_code in ('H80A-HBDQR0N-91')
		-- AND bn.error_code in ('DSP0x4903','DSP0x042D')
        group by date_value
    )t 
group by date_value



--------------------------------------------------------------
select 
    date_value,
    coalesce (max(job_num),0) as job_num,
    -- coalesce (max(error_num),0) as error_num,
    case when coalesce (max(job_num),0) !=0 then coalesce (max(error_num),0)/coalesce (max(job_num),0) end as job_error_rate
from 
    (
        select 
            date_format(tor.create_time,'%Y-%m-%d')  as date_value,
            count(distinct tocj.job_sn)             as job_num,
            null as error_num 
        from phoenix_rss.transport_order tor
		left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
        inner join phoenix_basic.basic_robot br on br.robot_code =tocj.robot_code and br.usage_state ='using'
        where tor.create_time >= '{{ start_time }}' and tor.create_time < '{{ end_time }}'
        {% if robot_code %} AND br.robot_code IN {{ ja_concat_in(robot_code) }} {% endif %}  -- 机器人编码
        {% if robot_type_code %} AND br.robot_type_code IN {{ ja_concat_in(robot_type_code) }} {% endif %}  -- 机器人类型
        group by date_value

        union all 
		
        select 
            date_format(bn.start_time,'%Y-%m-%d')  as date_value,
            null as order_num,
            count(distinct bn.id) as error_num
        from phoenix_basic.basic_notification bn
        inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
        where bn.alarm_module ='robot' and bn.alarm_level >=3
        and bn.start_time >= '{{ start_time }}' and bn.start_time <= '{{ end_time }}'
        {% if robot_code %} AND br.robot_code IN {{ ja_concat_in(robot_code) }} {% endif %}  -- 机器人编码
        {% if robot_type_code %} AND br.robot_type_code IN {{ ja_concat_in(robot_type_code) }} {% endif %}  -- 机器人类型
        {% if error_code %} AND bn.error_code IN {{ ja_concat_in(error_code) }} {% endif %}  -- 错误码
        group by date_value
    )t 
group by date_value
