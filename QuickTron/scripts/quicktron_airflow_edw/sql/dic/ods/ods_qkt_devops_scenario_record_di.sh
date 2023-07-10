#！/bin/bash
# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops事件记录
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : 
#-- 输出表 ：ods.ods_qkt_devops_scenario_record_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-15 CREATE  初始化分区
# ------------


dbname=ods
table=ods_qkt_devops_scenario_record_di
target_dir=/user/hive/warehouse/ods.db/
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

##为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table/d=$pre1_date
if [ $? -eq 0 ] ;then
    sql="use $dbname;
    alter table $table drop partition(d='$pre1_date');
    alter table $table  add partition(d='$pre1_date');"
    echo "clean up and add partition $target_dir$table/d=$pre1_date"
else
    sql="use $dbname;
    alter table $table  add partition(d='$pre1_date');"
    echo "not clean up and mkidr $target_dir$table/d=$pre1_date" 
fi

$hive  -e "$sql"

##设置datax变量
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=ods_qkt_devops_scenario_record_di.json

###远程到172.31.237.4上进行datax数据采集脚本调用
#ssh -tt hadoop@003.bg.qkt <<effo
##执行命令
$datax  -p "-Dpre1_date=${pre1_date}" $json_dir$json_name

#exit
#effo
