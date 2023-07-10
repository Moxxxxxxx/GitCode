select hour_value AS x,
       coalesce(sum(create_order_num), 0)   as upstream_order_num,
       coalesce(sum(abnormal_order_num), 0) as abnormal_order_num,
       coalesce(sum(canceled_order_num), 0) as canceled_order_num
from (select DATE_FORMAT(create_time, '%Y-%m-%d %H:00:00') as hour_value,
             count(distinct order_no)        as create_order_num,
             null                            as abnormal_order_num,
             null                            as canceled_order_num
      from phoenix_rss.transport_order
      where create_time BETWEEN {now_start_time} and {now_end_time}
      group by hour_value
      union all
      select DATE_FORMAT(update_time, '%Y-%m-%d %H:00:00')                                                                          as hour_value,
             null                                                                                                     as create_order_num,
             count(distinct case
                                when order_state in ('ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED', 'PENDING')
                                    then order_no end)                                                                as abnormal_order_num,
             count(distinct case when order_state in ('CANCELED') then order_no end)                                  as canceled_order_num
      from phoenix_rss.transport_order
      where update_time BETWEEN {now_start_time} and {now_end_time}
      group by hour_value) t
group by hour_value