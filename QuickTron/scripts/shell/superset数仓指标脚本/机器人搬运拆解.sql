select 
       a.order_no
       ,a.create_time as order_create_time
       ,a.update_time as order_update_time
       ,a.dispatch_robot_code
       ,b.robot_ip
       ,b.first_classification_name
       ,ifnull(c.error_qty,0) as error_qty
       ,a.duration as order_duration
       ,d.duration as lift_up_duration
       ,e.duration as lift_down_duration
       ,ifnull(f.order_rotate_count,0) as order_rotate_count
       ,ifnull(f.order_termial_guide_duration,0) as order_termial_guide_duration
from
(
       select 
              order_no
              ,dispatch_robot_code 
              ,update_time
              ,create_time
              ,sum(unix_timestamp(update_time) - unix_timestamp(create_time)) as duration
       from 
              phoenix_rss.transport_order 
       where 
              order_state = 'COMPLETED'

              and date_format(update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')

       group by 
              order_no,dispatch_robot_code
) a 
left join 
(
       select 
              br.robot_code
              ,br.ip as robot_ip
              ,case
           when brt.first_classification = 'WORKBIN' then '料箱车'
           when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
           when brt.first_classification = 'CARRIER' then '潜伏式'
           when brt.first_classification = 'ROLLER' then '辊筒'
           when brt.first_classification = 'FORKLIFT' then '堆高全向车'
           when brt.first_classification = 'DELIVER' then '投递车'
           when brt.first_classification = 'SC' then '四向穿梭车'
           else brt.first_classification 
           end as first_classification_name
       from 
              phoenix_basic.basic_robot br 
              left join phoenix_basic.basic_robot_type brt on br.robot_type_id = brt.id 
       ) b on a.dispatch_robot_code = b.robot_code
       
left join     

(
       select        
              tocj.order_no
              ,count(1) as error_qty
       from 
              phoenix_basic.basic_notification bn 
              left join phoenix_rss.transport_order_carrier_job tocj on bn.robot_job = tocj.job_sn
       where 
              bn.id in (select distinct error_id from(
              select * from qt_smartreport.qt_basic_notification_clear4_realtime  
              )e_list)
              and tocj.order_no is not null 
              
              and date_format(bn.create_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
              and date_format(tocj.update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
              
       group by tocj.order_no 
) c on a.order_no = c.order_no
left join 
(
       select 
              a.order_no
              ,MAX(a.create_time) as start_time
              ,MAX(b.create_time) as end_time
              ,unix_timestamp(MAX(b.create_time)) - unix_timestamp(MAX(a.create_time)) as duration
       from 
       (
              select        
                     order_no
                     ,execute_state
                     ,create_time
              from 
                     phoenix_rss.transport_order_link
              where 
                     execute_state = 'LIFT_UP_START'

                     and date_format(update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')

              ) a inner join 
              (
              select        
                     order_no
                     ,execute_state
                     ,create_time
              from 
                     phoenix_rss.transport_order_link
              where 
                     execute_state = 'LIFT_UP_DONE'

                     and date_format(update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')

              ) b on a.order_no = b.order_no and a.create_time < b.create_time      
              group by 
                     order_no
) d on a.order_no = d.order_no
left join 
(
              select 
                     a.order_no
                     ,MAX(a.create_time) as start_time
                     ,MAX(b.create_time) as end_time
                     ,unix_timestamp(MAX(b.create_time)) - unix_timestamp(MAX(a.create_time)) as duration
              from 
              (
              select        
                     order_no
                     ,execute_state
                     ,create_time
              from 
                     phoenix_rss.transport_order_link
              where 
                     execute_state = 'PUT_DOWN_START'

                     and date_format(update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')

              ) a inner join 
              (
              select        
                     order_no
                     ,execute_state
                     ,create_time
              from 
                     phoenix_rss.transport_order_link
              where 
                     execute_state = 'PUT_DOWN_DONE'

                     and date_format(update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')

              ) b on a.order_no = b.order_no and a.create_time < b.create_time      
              group by 
                     order_no
) e on a.order_no = e.order_no
left join 
(
       select 
              tocj.order_no 
              ,sum(jasd.rotate_count) as order_rotate_count
              ,sum(unix_timestamp(terminal_guide_end_time) - unix_timestamp(terminal_guide_start_time)) as order_termial_guide_duration 
       from 
              phoenix_rms.job_action_statistics_data jasd
              left join phoenix_rss.transport_order_carrier_job tocj on jasd.job_sn = tocj.job_sn
       where 
              tocj.order_no is not null 
              and date_format(tocj.update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
       group by 
              tocj.order_no
) f on a.order_no = f.order_no