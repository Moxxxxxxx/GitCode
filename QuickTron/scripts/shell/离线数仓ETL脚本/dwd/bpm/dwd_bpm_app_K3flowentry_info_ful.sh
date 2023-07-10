#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"

columns=id,flowid,fentryid,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,number1,number2,number3,number4,number5,number6,number7,number8,number9,number10,number11,number12,number13,number14,number15,number16,number17,number18,number19,number20,number29,number21,number22,number23,number24,number28,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,description,updatetime,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string58,cashflow,zdbh,forder,isamark,string51,string52,string53,string54,string55,string56,string57,string59,string60,string61,string62,string63,string64,string65,number40,number41,number42,number25,number26,number27,number30,number31,number32,number33,number34,number35,number36,number37,number38,number39,number43,number44,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,infinity

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_bpm_app_K3flowentry_info_ful 
select 
$columns
from 
ods_qkt_bpm_app_K3flowentry_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
