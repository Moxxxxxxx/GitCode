#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 erp采购退料单据分录财务表，保存退料单分录财务相关信息，与分录明细数据1对1
#-- 注意 ： 每天全量
#-- 输入表 : kingdee.t_pur_mrbentry_f
#-- 输出表 ：ods.ods_qkt_kde_pur_mrb_entry_finance_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-07 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_kde_pur_mrb_entry_finance_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=kingdee
mysql_table=t_pur_mrbentry_f
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
fentryid, fid, fpriceunitid, fpriceunitqty, fprice, ftaxprice, fpricecoefficient, fsysprice, 
fupprice, fdownprice, fdiscountrate, fdiscount, ftaxnetprice, famount, famount_lc, ftaxrate, 
ftaxamount, ftaxamount_lc, fallamount, fallamount_lc, fbilldisapportion, fbefbilldisamt,
 fbefbilldisallamt, fbefdisamt, fbefdisallamt, freplenishincltaxamt, freplenishexcltaxamt, 
 fkeapincltaxamt, fkeapexcltaxamt, fjoinedqty, funjoinqty, fjoinedamount, funjoinamount, 
 ffullyjoined, fjoinstatus, fbaseunitprice, fpurcost, finvoicedqty, fprocessfee, 
 fmaterialcosts, ftaxcombination, fcostprice, fcostamount, fcostamount_lc, fchargeprojectid, 
 finvoicedstatus, finvoicedjoinqty, fbillingclose, fcostprice_lc, fpricebaseqty, 
 fsetpriceunitid, fcarryunitid, fcarryqty, fcarrybaseqty, fapnotjoinqty, fapjoinamount, 
 fpricelistentry, fprocessfee_lc, fmaterialcosts_lc
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

