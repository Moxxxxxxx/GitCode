#!/bin/bash


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/tmp/
json_name=(hive_ads_carry_work_analyse_count_ck.json hive_ads_carry_work_analyse_detail_ck.json hive_ads_amr_breakdown_ck.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done