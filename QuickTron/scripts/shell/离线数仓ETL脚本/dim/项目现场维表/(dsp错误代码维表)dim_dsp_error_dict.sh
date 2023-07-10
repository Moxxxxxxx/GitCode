#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=quicktronft_db
table=dim_dsp_error_dict
outdir=/opt/module/sqoop/log_script
#target_dir=/warehouse/quicktronft_db/ods/
target_dir=/user/hive/warehouse/quicktronft_db.db/
mysql_dbname=evo_wds_base
mysql_table=dsp_error_dict
#incre_column=updated_date
hive=/opt/module/hive/bin/hive

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


###################### dsp_error_dict->dim_dsp_error_dict(T-1每日全量)
 /opt/module/sqoop/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://hadoop102:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username root \
--password tClEDdt6 \
--query "select code as error_code,name as error_ename,display_name as error_cname,type as error_type,error_level from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"





