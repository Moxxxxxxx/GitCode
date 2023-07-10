#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集汇联易的个人报销费用数据
#-- 注意 ： 每天全量
#-- 输入表 : quality_data.hly_reimbursement
#-- 输出表 ：ods.ods_qkt_hly_personal_reimbursement_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-14 CREATE 
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_hly_personal_reimbursement_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=hly_reimbursement
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



 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
businesscode, formtypedesc, applicantname, applicantempid, 
applicantcomname, applicantcomcode, applicantdeptname, 
applicantcustdeptnumber, submittedbyname, companyname, 
companycode, relatedappbusinesscode, title, submitteddate, 
reimbstatusdesc, reimblastmodifieddate, reimblastapprover, 
reimbauditapprovaldate, reimbauditapprover, currencycode, 
totalamount, functionalcurrencycode, exchagerate, basecurrencyamount,
basereimbpaymentamount, realpaymentbaseamount, formname, 
loanbusinesscode, labelname, labeltoast, setofbooksid, 
legalentityoid, companyoid, departmentoid, applicantoid, 
cci1code, integrationid, receivedate, reimbrealpaymentdate,
applicantdeptpath, departmentpath, custdeptnumber, 
origdocumentsequence, participantempid, cci4oid, lastapprovalnode, 
lastapprovaldate, approvalnode, createdbyname, followingapprover, 
approvalandauditname, internalparticipant, externalparticipant, 
departmentcode, cci3code, approvaldate, cci3oid, departmentname, 
collectorname, cci2oid, activateddate, financeapprovalnode, 
financeapprovaldate, receivestatusdesc, applicanttaxpayernumber, 
cci2, cci3, paymentcurrencycode, cci4, reimbbookdate, cci4code, 
cci5oid, cci5, applicantlegalname, receivescancodedate, 
reimbprintviewdate, cci2code, cci5code, dt_create_time, dt_update_time
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

