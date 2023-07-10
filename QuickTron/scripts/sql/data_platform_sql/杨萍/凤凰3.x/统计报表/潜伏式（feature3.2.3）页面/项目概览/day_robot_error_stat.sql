select '全场'                                                                            as stat_type,
       null                                                                              as robot_type_code,
       '全场'                                                                            as robot_type_name,          -- 机器人类型
     coalesce(sum(t.create_robot_error_num), 0)                                        as breakdown_num,            -- 故障次数	   
		case 
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_order_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0)),'/','1')
	 else 0 end as order_breakdown_rate, -- 故障率（搬运作业单）   
	    case when COALESCE(SUM(t.create_order_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0) end AS order_breakdown_rate_sort, -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)                                              as order_num,                -- 订单量
	 case 
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_job_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0)),'/','1')
	 else 0 end as carry_job_breakdown_rate, -- 故障率（搬运任务）	
    case when COALESCE(SUM(t.create_job_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0) end as carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序	
       COALESCE(sum(t.create_job_num), 0)                                                as carry_job_num,            -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)                                              as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0)                                            as the_day_error_time,
       ROUND((coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /
             coalesce(sum(t.the_day_run_time), 0), 4)                                    as oee,
       COALESCE(sum(t.end_error_num), 0)                                                 as end_error_num,
       coalesce(sum(t.end_error_time), 0)                                                as end_error_time,
       coalesce(sum(t.end_error_time), 0) /
       COALESCE(sum(t.end_error_num), 0)                                                 as mttr,
       NULL                                                                              AS mtbf
from (select count(distinct id) as create_robot_error_num,
             null               as create_order_num,
             null               as create_job_num,
             null               as end_error_num,
             null               as end_error_time,
             null               as the_day_run_time,
             null               as the_day_error_time
      from ({tb_day_robot_error_detail}) t -- day_robot_error_detail.sql		 
      where start_time BETWEEN {now_start_time} and {now_end_time}
      union all
      select null                         as create_robot_error_num,
             count(distinct tor.order_no) as create_order_num,
             count(distinct tocj.job_sn)  as create_job_num,
             null                         as end_error_num,
             null                         as end_error_time,
             null                         as the_day_run_time,
             null                         as the_day_error_time
      from phoenix_rss.transport_order tor
               left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id 
               where tor.create_time BETWEEN {now_start_time} and {now_end_time}
      union all
      select null                                                       as create_robot_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             count(distinct id)                                         as end_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_error_time,
             null                                                       as the_day_run_time,
             null                                                       as the_day_error_time
      from ({tb_day_robot_error_detail}) t -- day_robot_error_detail.sql		 
      where end_time is not null
        and end_time BETWEEN {now_start_time} and {now_end_time}
      union all
      select null               as create_robot_error_num,
             null               as create_order_num,
             null               as create_job_num,
             null               as end_error_num,
             null               as end_error_time,
             sum(stat_duration) as the_day_run_time,
             null               as the_day_error_time
      from ({tb_hour_robot_state_detail_duration}) tb -- hour_robot_state_detail_duration.sql
      where online_state = 'REGISTERED'
         or work_state = 'ERROR'
		 or is_error = 1
      union all
      select null                       as create_robot_error_num,
             null                       as create_order_num,
             null                       as create_job_num,
             null                       as end_error_num,
             null                       as end_error_time,
             null                       as the_day_run_time,
             sum(the_hour_cost_seconds) as the_day_error_time
      from ({tb_hour_robot_error_time_detail}) t -- hour_robot_error_time_detail.sql
     ) t
union all
select '各类机器人'                                                        as stat_type,
       t.robot_type_code,
       t.robot_type_name,  -- 机器人类型
       coalesce(sum(t.create_robot_error_num), 0)                          as breakdown_num, -- 故障次数	   
	case 
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_order_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0)),'/','1')
	 else 0 end as order_breakdown_rate, -- 故障率（搬运作业单）	
    case when COALESCE(SUM(t.create_order_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0) end AS order_breakdown_rate_sort, -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)                                as order_num, -- 订单量   
	 case 
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_job_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0)),'/','1')
	 else 0 end as carry_job_breakdown_rate, -- 故障率（搬运任务）		 
    case when COALESCE(SUM(t.create_job_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0) end AS carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序	
       COALESCE(sum(t.create_job_num), 0)                                  as carry_job_num, -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)                                as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0)                              as the_day_error_time,
       (coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /
       coalesce(sum(t.the_day_run_time), 0)                                as oee,
       COALESCE(sum(t.end_error_num), 0)                                   as end_error_num,
       coalesce(sum(t.end_error_time), 0)                                  as end_error_time,
       coalesce(sum(t.end_error_time), 0) /
       COALESCE(sum(t.end_error_num), 0)                                   as mttr,
       NULL                                                                AS mtbf
from (select br.robot_code,
             brt.robot_type_code,
             brt.robot_type_name,
             t1.create_robot_error_num,
             t2.create_order_num,
             t2.create_job_num,
             t3.end_error_num,
             t3.end_error_time,
             t4.the_day_run_time,
             t5.the_day_error_time
      from phoenix_basic.basic_robot br
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
               left join (select robot_code, count(distinct id) as create_robot_error_num
                          from ({tb_day_robot_error_detail})t  -- day_robot_error_detail.sql
                          where start_time BETWEEN {now_start_time} and {now_end_time}
                          group by robot_code) t1 on t1.robot_code = br.robot_code
               left join (select tocj.robot_code,
                                 count(distinct tocj.order_no) as create_order_num,
                                 count(distinct tocj.job_sn)   as create_job_num
                          from phoenix_rss.transport_order tor
                                   inner join phoenix_rss.transport_order_carrier_job tocj
                                              on tocj.order_id = tor.id and tocj.robot_code is not null and
                                                 tocj.robot_code <> '' 
             					where tor.create_time BETWEEN {now_start_time} and {now_end_time}
                          group by tocj.robot_code) t2 on t2.robot_code = br.robot_code
               left join (select robot_code,
                                 count(distinct id)                                         as end_error_num,
                                 sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_error_time
                          from ({tb_day_robot_error_detail})t   -- day_robot_error_detail.sql
                          where end_time is not null
                            and end_time BETWEEN {now_start_time} and {now_end_time}
                          group by robot_code) t3 on t3.robot_code = br.robot_code
               left join (select robot_code,
                                 sum(stat_duration) as the_day_run_time
                          from ({tb_hour_robot_state_detail_duration}) tb -- hour_robot_state_detail_duration.sql
                          where online_state = 'REGISTERED'
                             or work_state = 'ERROR'
							 or is_error = 1
                          group by robot_code) t4 on t4.robot_code = br.robot_code
               left join (select robot_code,
                                 sum(the_hour_cost_seconds) as the_day_error_time
                          from ({tb_hour_robot_error_time_detail})t   -- hour_robot_error_time_detail.sql
                          group by robot_code) t5 on t5.robot_code = br.robot_code
      where br.usage_state = 'using') t
group by t.robot_type_code, t.robot_type_name