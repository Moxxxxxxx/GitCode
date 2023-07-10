#!/bin/bash

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

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