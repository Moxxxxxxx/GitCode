#!/bin/bash

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
dbname=ods
table=ods_qkt_bpm_app_k3flow_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=bpm
mysql_table=App_K3Flow
incre_column=gmt_modified
#hive=/opt/module/hive-3.1.2/scripts/hive
hive=/opt/module/hive-3.1.2/bin/hive
hive_username=wangziming
hive_passwd=wangziming1

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



###################### station->ods_qkt_station_station(T-1每日全量->分区)
 /opt/module/sqoop-1.4.7/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
id,FlowID,FlowStatus,FlowModelID,ApplyID,ApplyName,DeptID,DeptName,OrgID,OrgName,FlowName,StartDate,EndDate,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12,date13,date14,date15,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,Number1,Number2,Number3,Number4,Number5,Number6,Number7,Number8,Number9,Number10,Number11,Number12,Number13,Number14,Number15,Number16,Number17,Number18,Number19,Number20,Number21,Number22,Number23,Number24,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,Description,BackSucess,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string51,string52,string53,string54,string55,string56,string57,string58,string59,string60,string61,string62,string63,string64,string65,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,string91,string92,string93,string94,string95,string96,string97,string98,string99,string100,bool11,bool12,bool13,bool14,bool15,bool16,bool17,bool18,bool19,bool20,text1,oFlowModelID,ApplyAcc,ErpMsgID,VoucherID,CheckID,text2,text3,text4,text5,cash,budget,cashflow,zdbh,cashflow2,cashflow1,GUID,PrintCount,Office1,FileType,FileSize,Number25,Number26,Number27,Number28,Number29,Number30,Number31,Number32,Number33,Number34,Number35,Number36,Number37,Number38,Number39,Number40,Number41,Number42,Number43,Number44,Number45,backSql 
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 3 \
--split-by id \
--hcatalog-database $dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

echo "#########################ods成功导入分区数据###############################"
###################################################################################################################


