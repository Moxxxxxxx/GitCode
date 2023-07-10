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
json_name=(ads_project_view_breakdown_top5.json ads_project_view_error_code_top5.json ads_project_view_sys_order_ansys.json ads_project_view_sys_order_ansys_rank.json ads_project_view_manual_recovery.json ads_project_view_dead_lock_num_dis.json ads_project_view_traffic_jam_num_dis.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo