
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

columns=userid,loginname,password,webpwd,name,sex,job,telno,cornet,mobile,email,qq,edulevel,school,istemp,startdate,enddate,deleted,agentstatus,iscc,agentnote,loginsystems,createid,createdate,updateid,updatedate,ldap,createadlog,lastupdatead,k3userid,user_headp,user_signature,signature,netdisk,netdisksize,netdiskfiletype,id,frobidden,guid,loginerror,temp_deptname,emailuserpsw,inemail,k3pwd,temp_deptid,netdiskupload,netisdown,clientid,adguid,adfguid,isadmin,flowsubmithabit,twopsw,guidno,k3username,language,isfirstlanding,k3userid1


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_bpm_ts_user_info_ful
select 
$columns
from 
ods_qkt_bpm_ts_user_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

