#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 汇联易申请单数据
#-- 注意 ： 每日按天增量分区
#-- 输入表 : quality_data.hly_dtl_report
#-- 输出表 ：ods.ods_qkt_hly_application_form_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-15 CREATE 

# ------------------------------------------------------------------------------------------------
## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_hly_application_form_di
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=hly_dtl_report
incre_column_1=gmt_modified
incre_column_2=update_time
datax_incre_column=datax_update_time
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

## hcatalog不支持文件覆盖，为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table/d=$pre1_date
if [ $? -eq 0 ] ;then 
    hdfs dfs -rm -r $target_dir$table/d=$pre1_date
    echo 'clean up'
else 
    echo 'not clean up' 
fi


###################### agv_job->ods_qkt_rcs_agv_job(T-1每日增量量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
departmentcode, 
setofbooksid, 
legalentityoid, 
companyoid, 
applicantoid, 
setofbooksname, 
applicationtypedesc, 
formname, 
businesscode, 
version, 
submittedbyname,
submittedbyempid, 
applicantname, 
applicantempid, 
applicantcomname,
applicantcomcode, 
applicantentityname, 
applicantdeptname, 
applicantcustdeptnumber, 
applicantdeptpath, 
title, 
companyname, 
companycode, 
departmentname, 
departmentpath, 
internalparticipant, 
participantempid, 
externalparticipant, 
appstatusdesc,
expensetypecategoryname, 
expensetypename, 
ebamount, 
currencycode, 
ebcompanycurrencyrate, 
ebbasecurrencyamount,
ebpaymenttypedesc, 
ebremark, 
appsubmittedbydate, 
appapprovaldate, 
lastapprover, 
totalamount, 
referencebusinesscode, 
itinerarytypename, 
itinerarycity, 
itinerarydate, 
referenceexpensetypename, 
referencetotalamount, 
referencebasetotalamount, 
applicationstartdate, 
applicationenddate, 
days, 
expensedepartmentname, 
custdeptnumber, 
itheaddate, 
itheadcity, 
cci1code, 
cci2code, 
cci3code, 
cci4code, 
cci5code, 
cci6code, 
cci1, 
cci2, 
cci3, 
cci4, 
cci5, 
cci6, 
integrationid, 
firstsubmitteddate, 
cci3oid, 
itheadremark, 
ebcurrencycode, 
cci5oid, 
cci2oid, 
followingapprover, 
dt_create_time, 
dt_update_time
from $mysql_table  where date_format($datax_incre_column,'%Y-%m-%d')=date_add('$pre1_date',interval 1 day)  and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"



