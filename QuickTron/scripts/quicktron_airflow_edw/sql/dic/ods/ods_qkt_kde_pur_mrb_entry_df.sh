#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 erp退料单明细表体-存储退料单明细物料信息，与退料单1:n对应存储
#-- 注意 ： 每天全量
#-- 输入表 : kingdee.t_pur_mrbentry
#-- 输出表 ：ods.ods_qkt_kde_pur_mrb_entry_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-27 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_kde_pur_mrb_entry_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=kingdee
mysql_table=t_pur_mrbentry
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
fentryid, fid, fseq, fmaterialid, funitid, frmmustqty, 
frmrealqty, freplenishqty, fkeapamtqty, fstockid, fstocklocid, 
fstockstatusid, fauxpropid, fbomid, flot, flot_text, fsupplierlot, 
fproducedate, fexpperiod, fexpunit, fexpirydate, fbaseunitid, fbasereplayqty, 
fbaseunitqty, fauxunitid, fauxunitqty, fbasejoinqty, fshelflife, fsrcbilltypeid, 
fsrcfid, fsrcbillno, fsrcrowid, fsrcseq, fcontractno, forderno, freqtraceno, fnote, 
fstockflag, fownertypeid, fownerid, fkeepertypeid, fkeeperid, fjoinqty, fbasepoqty, 
fbaseapjoinqty, fbflowid, fbasekeapamtqty, freceivestockid, freceivestocklocid, 
freceivestockstatusid, freceivelot, freceivelot_text, freceivestockflag, fporequireorgid, 
freceiveauxpropid, fmtono, fprojectno, fbasejoinsalreturnqty, fjoinsalreturnqty, fextauxunitid, 
fextauxunitqty, freceivemtono, fisreceiveupdatestock, fgiveaway, fpoorderentryid, fprojectdetail, fbarcode
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

