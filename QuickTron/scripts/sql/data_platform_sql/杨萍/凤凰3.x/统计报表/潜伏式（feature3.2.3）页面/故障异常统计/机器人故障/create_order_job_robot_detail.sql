select tor.create_time,
       tor.order_no,
       tocj.job_sn,
       tocj.robot_code
from phoenix_rss.transport_order tor
         inner join phoenix_rss.transport_order_carrier_job tocj
                    on tocj.order_id = tor.id and tocj.robot_code is not null and
                       tocj.robot_code <> ''
where  tor.create_time BETWEEN {start_time} and {end_time}				   