#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
project_code=A51118


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- 单项目出入库单量统计 ads_single_project_order_statistics 

INSERT overwrite table ${ads_dbname}.ads_single_project_order_statistics
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       NULL as cur_hour, -- 统计小时
       '日' as run_type, -- 时间维度
       t2.project_code, -- 项目编码
       '出库' as order_type, -- 订单类型
       nvl(p.order_num,0) as order_num, -- 出库订单数量（单）
       nvl(pw.orderline_num,0) as orderline_num, -- 出库订单行数量（行）
       nvl(pw.quantity_num,0) as quantity_num, -- 出库拣选件数（件）
       nvl(CAST(pw.quantity_num / p.order_num as decimal(10,2)),0) as quantity_order_rate, --件单比（件/单）
       nvl(CAST(pw.orderline_num / p.order_num as decimal(10,2)),0) as orderline_order_rate, --行单比（行/单）
       nvl(CAST(pw.quantity_num / pw.orderline_num as decimal(10,2)),0) as quantity_orderline_rate, --件行比（件/行）
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_day_date
  WHERE days = '${pre1_date}'
)t1
LEFT JOIN 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1,2) -- 到人
)t2
LEFT JOIN 
(
  SELECT TO_DATE(p.order_updated_time) as cur_date,
         p.project_code,
         COUNT(DISTINCT p.id) as order_num
  FROM ${dwd_dbname}.dwd_picking_order_info p
  WHERE TO_DATE(p.order_updated_time) = '${pre1_date}' AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.order_state = 'DONE' -- 订单已完成
  GROUP BY TO_DATE(p.order_updated_time),p.project_code
)p 
ON t1.days = p.cur_date AND t2.project_code = p.project_code 
LEFT JOIN 
(
  SELECT TO_DATE(pw.work_updated_time) as cur_date,
         pw.project_code,
         COUNT(DISTINCT pw.id) as orderline_num,
         SUM(nvl(pw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_g2p_picking_work_detail_info pw
  WHERE TO_DATE(pw.work_updated_time) = '${pre1_date}' AND pw.d >= DATE_ADD(CURRENT_DATE(), -7) AND pw.quantity = pw.fulfill_quantity
  GROUP BY TO_DATE(pw.work_updated_time),pw.project_code
  
  UNION ALL 
  
  SELECT TO_DATE(pw.work_updated_time) as cur_date,
         pw.project_code,
         COUNT(DISTINCT pw.id) as orderline_num,
         SUM(nvl(pw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_picking_work_detail_info pw
  WHERE TO_DATE(pw.work_updated_time) = '${pre1_date}' AND pw.d >= DATE_ADD(CURRENT_DATE(), -7) AND pw.quantity = pw.fulfill_quantity AND pw.pt = 'C35052' -- 虹迪版本升级后没有w2p的数据，只能在wes中取值
  GROUP BY TO_DATE(pw.work_updated_time),pw.project_code
)pw
ON t1.days = pw.cur_date AND t2.project_code = pw.project_code

UNION ALL 

-- 入库单量
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       NULL as cur_hour, -- 统计小时
       '日' as run_type, -- 时间维度
       t2.project_code, -- 项目编码
       '入库' as order_type, -- 订单类型
       nvl(r.order_num,0) as order_num, -- 入库订单数量（单）
       nvl(rw.orderline_num,0) as orderline_num, -- 入库订单行数量（行）
       nvl(rw.quantity_num,0) as quantity_num, -- 入库拣选件数（件）
       nvl(CAST(rw.quantity_num / r.order_num as decimal(10,2)),0) as quantity_order_rate, --件单比（件/单）
       nvl(CAST(rw.orderline_num / r.order_num as decimal(10,2)),0) as orderline_order_rate, --行单比（行/单）
       nvl(CAST(rw.quantity_num / rw.orderline_num as decimal(10,2)),0) as quantity_orderline_rate, --件行比（件/行）
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_day_date
  WHERE days = '${pre1_date}'
)t1
LEFT JOIN 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1,2) -- 到人
)t2
LEFT JOIN 
(
  SELECT TO_DATE(r.order_updated_time) as cur_date,
         r.project_code,
         COUNT(DISTINCT r.id) as order_num
  FROM ${dwd_dbname}.dwd_replenish_order_info r
  WHERE TO_DATE(r.order_updated_time) = '${pre1_date}' AND r.d >= DATE_ADD(CURRENT_DATE(), -7) AND r.order_state = 'DONE' -- 订单已完成
  GROUP BY TO_DATE(r.order_updated_time),r.project_code
)r
ON t1.days = r.cur_date AND t2.project_code = r.project_code 
LEFT JOIN 
(
  SELECT TO_DATE(rw.work_updated_time) as cur_date,
         rw.project_code,
         COUNT(DISTINCT rw.id) as orderline_num,
         SUM(nvl(rw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_replenish_work_detail_info rw
  WHERE TO_DATE(rw.work_updated_time) = '${pre1_date}' AND rw.d >= DATE_ADD(CURRENT_DATE(), -7) AND rw.quantity = rw.fulfill_quantity
  GROUP BY TO_DATE(rw.work_updated_time),rw.project_code
)rw
ON t1.days = rw.cur_date AND t2.project_code = rw.project_code

UNION ALL 

-- 分时出库单量
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       t2.hourofday as cur_hour, -- 统计小时
       '小时' as run_type, -- 时间维度
       t3.project_code, -- 项目编码
       '出库' as order_type, -- 订单类型
       nvl(p.order_num,0) as order_num, -- 出库订单数量（单）
       nvl(pw.orderline_num,0) as orderline_num, -- 出库订单行数量（行）
       nvl(pw.quantity_num,0) as quantity_num, -- 出库拣选件数（件）
       0 as quantity_order_rate, --件单比（件/单）
       0 as orderline_order_rate, --行单比（行/单）
       0 as quantity_orderline_rate, --件行比（件/行）
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_day_date
  WHERE days = '${pre1_date}'
)t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour t2
LEFT JOIN 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1,2) -- 到人
)t3
LEFT JOIN 
(
  SELECT TO_DATE(p.order_updated_time) as cur_date,
         lpad(HOUR(p.order_updated_time),2,0) as cur_hour,
         p.project_code,
         COUNT(DISTINCT p.id) as order_num
  FROM ${dwd_dbname}.dwd_picking_order_info p
  WHERE TO_DATE(p.order_updated_time) = '${pre1_date}' AND p.d >= DATE_ADD(CURRENT_DATE(), -7) AND p.order_state = 'DONE' -- 订单已完成
  GROUP BY TO_DATE(p.order_updated_time),lpad(HOUR(p.order_updated_time),2,0),p.project_code
)p
ON t1.days = p.cur_date AND t2.hourofday = p.cur_hour AND t3.project_code = p.project_code
LEFT JOIN 
(
  SELECT TO_DATE(pw.work_updated_time) as cur_date,
         lpad(HOUR(pw.work_updated_time),2,0) as cur_hour,
         pw.project_code,
         COUNT(DISTINCT pw.id) as orderline_num,
         SUM(nvl(pw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_g2p_picking_work_detail_info pw
  WHERE TO_DATE(pw.work_updated_time) = '${pre1_date}' AND pw.d >= DATE_ADD(CURRENT_DATE(), -7) AND pw.quantity = pw.fulfill_quantity
  GROUP BY TO_DATE(pw.work_updated_time),lpad(HOUR(pw.work_updated_time),2,0),pw.project_code
  
  UNION ALL 
  
  SELECT TO_DATE(pw.work_updated_time) as cur_date,
         lpad(HOUR(pw.work_updated_time),2,0) as cur_hour,
         pw.project_code,
         COUNT(DISTINCT pw.id) as orderline_num,
         SUM(nvl(pw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_picking_work_detail_info pw
  WHERE TO_DATE(pw.work_updated_time) = '${pre1_date}' AND pw.d >= DATE_ADD(CURRENT_DATE(), -7) AND pw.quantity = pw.fulfill_quantity AND pw.pt = 'C35052' -- 虹迪版本升级后没有w2p的数据，只能在wes中取值
  GROUP BY TO_DATE(pw.work_updated_time),lpad(HOUR(pw.work_updated_time),2,0),pw.project_code
)pw
ON t1.days = pw.cur_date AND t2.hourofday = pw.cur_hour AND t3.project_code = pw.project_code

UNION ALL 

-- 入库单量
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       t2.hourofday as cur_hour, -- 统计小时
       '小时' as run_type, -- 时间维度
       t3.project_code, -- 项目编码
       '入库' as order_type, -- 订单类型
       nvl(r.order_num,0) as order_num, -- 入库订单数量（单）
       nvl(rw.orderline_num,0) as orderline_num, -- 入库订单行数量（行）
       nvl(rw.quantity_num,0) as quantity_num, -- 入库拣选件数（件）
       0 as quantity_order_rate, --件单比（件/单）
       0 as orderline_order_rate, --行单比（行/单）
       0 as quantity_orderline_rate, --件行比（件/行）
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_day_date
  WHERE days = '${pre1_date}'
)t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour t2
LEFT JOIN 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1,2) -- 到人
)t3
LEFT JOIN 
(
  SELECT TO_DATE(r.order_updated_time) as cur_date,
         lpad(HOUR(r.order_updated_time),2,0) as cur_hour,
         r.project_code,
         COUNT(DISTINCT r.id) as order_num
  FROM ${dwd_dbname}.dwd_replenish_order_info r
  WHERE TO_DATE(r.order_updated_time) = '${pre1_date}' AND r.d >= DATE_ADD(CURRENT_DATE(), -7) AND r.order_state = 'DONE' -- 订单已完成
  GROUP BY TO_DATE(r.order_updated_time),lpad(HOUR(r.order_updated_time),2,0),r.project_code
)r
ON t1.days = r.cur_date AND t2.hourofday = r.cur_hour AND t3.project_code = r.project_code
LEFT JOIN 
(
  SELECT TO_DATE(rw.work_updated_time) as cur_date,
         lpad(HOUR(rw.work_updated_time),2,0) as cur_hour,
         rw.project_code,
         COUNT(DISTINCT rw.id) as orderline_num,
         SUM(nvl(rw.fulfill_quantity,0)) as quantity_num
  FROM ${dwd_dbname}.dwd_replenish_work_detail_info rw
  WHERE TO_DATE(rw.work_updated_time) = '${pre1_date}' AND rw.d >= DATE_ADD(CURRENT_DATE(), -7) AND rw.quantity = rw.fulfill_quantity
  GROUP BY TO_DATE(rw.work_updated_time),lpad(HOUR(rw.work_updated_time),2,0),rw.project_code
)rw
ON t1.days = rw.cur_date AND t2.hourofday = rw.cur_hour AND t3.project_code = rw.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash



#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
#json_name=(ads_project_view_warehousing_order_num.json ads_project_view_warehousing_order_num_today.json)

#ssh -tt hadoop@003.bg.qkt <<effo
#for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#$datax  -p "-Dpre1_date='${pre1_date}'" ${json_dir}ads_project_view_warehousing_order_num_today.json
#exit
#effo

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select t1.project_code,t1.order_type,t1.cur_hour,t1.in_num,t2.out_num
           from (
                                               select t1.project_code,'nums' as order_type,t1.cur_hour,t1.order_num as in_num
                                               from ads.ads_single_project_order_statistics t1
                                               where t1.cur_date = '\${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                               union all
                                               select t1.project_code,'lines' as order_type,t1.cur_hour,t1.orderline_num as in_num
                                               from ads.ads_single_project_order_statistics t1
                                               where t1.cur_date = '\${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                               union all
                                               select t1.project_code,'pieces' as order_type,t1.cur_hour,t1.quantity_num as in_num
                                               from ads.ads_single_project_order_statistics t1
                                               where t1.cur_date = '\${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                             )t1
                                             left join
                                             (
                                               select t2.project_code,'nums' as order_type,t2.cur_hour,t2.order_num as out_num
                                               from ads.ads_single_project_order_statistics t2
                                               where t2.cur_date = '\${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                               union all
                                               select t2.project_code,'lines' as order_type,t2.cur_hour,t2.orderline_num as out_num
                                               from ads.ads_single_project_order_statistics t2
                                               where t2.cur_date = '\${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                               union all
                                               select t2.project_code,'pieces' as order_type,t2.cur_hour,t2.quantity_num as out_num
                                               from ads.ads_single_project_order_statistics t2
                                               where t2.cur_date = '\${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                             )t2
                                             on t1.project_code = t2.project_code and t1.order_type = t2.order_type and t1.cur_hour = t2.cur_hour
                                             order by t1.project_code,t1.order_type,t1.cur_hour;
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code,order_type,time_sharing,in_num,out_num
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_project_view_warehousing_order_num_today 
\--preSql truncate table ads_project_view_warehousing_order_num_today 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_view_warehousing_order_num_today"


start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select t1.project_code,t1.order_num as order_num1,t1.orderline_num as orderline_num1,t1.quantity_num as quantity_num1,t1.quantity_order_rate as quantity_order_rate1,t1.quantity_orderline_rate as quantity_orderline_rate1,t1.orderline_order_rate as orderline_order_rate1,t2.order_num,t2.orderline_num,t2.quantity_num,t2.quantity_order_rate,t2.quantity_orderline_rate,t2.orderline_order_rate
                                             from ads.ads_single_project_order_statistics t1
                                             left join ads.ads_single_project_order_statistics t2
                                             on t1.cur_date = t2.cur_date and t1.project_code = t2.project_code and t1.run_type = t2.run_type and t1.order_type != t2.order_type
                                             where t1.cur_date = '\${pre1_date}' and t1.run_type = '日' and t1.order_type = '入库'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code,in_order_num,in_order_line_num,in_total_num,in_piece_order,in_piece_line,in_line_order,out_order_num,out_order_line_num,out_total_num,out_piece_order,out_piece_line,out_line_order
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_project_view_warehousing_order_num 
\--preSql truncate table ads_project_view_warehousing_order_num 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_view_warehousing_order_num"