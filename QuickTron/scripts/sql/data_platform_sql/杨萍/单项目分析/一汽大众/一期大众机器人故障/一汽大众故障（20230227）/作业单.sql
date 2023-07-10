
select DATE_FORMAT(create_time, '%Y-%m-%d') as date_value,
count(distinct order_no) as order_num
from phoenix_rss.transport_order
group by 1