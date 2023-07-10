-- 凤凰3.X CARRIER逻辑
-- union all 
SELECT '' AS id, -- 主键
       t.project_code, -- 项目编码
       COALESCE (max(t.create_order_num),0)  AS send_num, -- 下发单量
       COALESCE (max(t.canceled_order_num),0) AS cancel_num, -- 取消单量
       COALESCE (max(t.abnormal_order_num),0) AS exc_num, -- 异常单量
       t.cur_hour AS count_date, -- 统计小时
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       t.cur_date as d,
       t.project_code AS pt 
from 
(SELECT * FROM ${dim_dbname}.dim_collection_project_record_ful WHERE project_version like '3.%') c
inner join 
(select 
project_code,substr(order_create_time,1,10) as cur_date,
DATE_FORMAT(order_create_time,'yyyy-MM-dd HH:00:00') AS cur_hour,
count(distinct order_no)        as create_order_num,
null as abnormal_order_num,
null as canceled_order_num
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where d >= '${pre11_date}'
group by project_code,substr(order_create_time,1,10),DATE_FORMAT(order_create_time,'yyyy-MM-dd HH:00:00') 
union all 
select 
project_code,substr(order_update_time,1,10) as cur_date,
DATE_FORMAT(order_update_time,'yyyy-MM-dd HH:00:00') AS cur_hour,
null as create_order_num,
count(distinct case when order_state in ('ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED', 'PENDING') then order_no end)  as abnormal_order_num,
count(distinct case when order_state in ('CANCELED') then order_no end) as canceled_order_num
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di
where d >= '${pre11_date}'
group by project_code,substr(order_update_time,1,10),DATE_FORMAT(order_update_time,'yyyy-MM-dd HH:00:00')
)t on t.project_code =c.project_code 
group by t.project_code,t.cur_date,t.cur_hour
   
