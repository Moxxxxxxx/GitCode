select tor.create_time,
       tor.order_no,
       tocj.job_sn
from phoenix_rss.transport_order tor
         left join phoenix_rss.transport_order_carrier_job tocj
                   on tocj.order_no = tor.order_no
where tor.order_state != 'CANCELED'
  and tor.create_time BETWEEN {start_time} and {end_time}						   