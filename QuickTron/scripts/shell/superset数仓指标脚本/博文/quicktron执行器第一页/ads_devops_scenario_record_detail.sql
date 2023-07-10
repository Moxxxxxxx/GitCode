#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops操作详表
#-- 注意 ： 
#-- 输入表 : dwd.dwd_devops_scenario_record_info_di
#-- 输出表 ：ads.ads_devops_scenario_record_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-15 CREATE
#-- 2 查博文 2021-12-15 增加筛选条件:环境管理,环境模板管理,数据集
#-- 1 查博文 2021-12-16 增加字段 week_year,week_period
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--devops操作详表 ads_devops_scenario_record_detail 

INSERT overwrite table ${ads_dbname}.ads_devops_scenario_record_detail
SELECT
    user_id                                               as user_id,
    operation_type                                        as operation_type,
    operation_sub_type                                    as operation_sub_type,
    submit_time                                           as submit_time,
    d                                                     as d,
    CONCAT(YEAR(d),'-',WEEKOFYEAR(d),'周')                          as week_year,
  CONCAT(date_add(d,1 - case when dayofweek(d) = 1 then 7 else dayofweek(d) - 1 end) ,'~', date_add(d,7 - case when dayofweek(d) = 1 then 7 else dayofweek(d) - 1 end) )          
                                            as week_period
FROM(
  SELECT 
    user_id,
    operation_type,
    operation_sub_type,
    submit_time,
    d
  FROM 
    ${dwd_dbname}.dwd_devops_scenario_record_info_di
  WHERE 
    operation_type IN ('环境管理','环境模板管理','数据集') 
  ) t;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_devops_scenario_record_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：devops操作详表 ads_devops_scenario_record_detail 
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_devops_scenario_record_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_devops_scenario_record_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "user_id,operation_type,operation_sub_type,submit_time,d,week_year,week_period"

echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "