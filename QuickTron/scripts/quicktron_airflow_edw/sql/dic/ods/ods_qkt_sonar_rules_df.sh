#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： sonar规则信息记录
#-- 注意 ：每日T-1快照日期
#-- 输入表 : quality_data.sonar_rules
#-- 输出表 ：ods.ods_qkt_sonar_rules_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2021-12-20 modify split-by 使用非数字会造成数据重复
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_sonar_rules_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=sonar_rules
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




 /opt/module/sqoop-1.4.7/bin/sqoop import  -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select 
 name, plugin_rule_key, plugin_key, plugin_config_key, plugin_name, \`scope\`, description, priority, status, \`language\`, def_remediation_function, def_remediation_gap_mult, def_remediation_base_effort, gap_description, system_tags, is_template, description_format, rule_type, security_standards, is_ad_hoc, is_external, created_at, updated_at, uuid, template_uuid
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database $ods_dbname \
--hcatalog-table $table \
--hcatalog-partition-keys d \
--hcatalog-partition-values $pre1_date \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"


