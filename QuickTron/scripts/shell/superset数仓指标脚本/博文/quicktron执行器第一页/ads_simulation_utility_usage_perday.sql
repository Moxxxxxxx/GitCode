#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 仿真环境使用频率天维度
#-- 注意 ： 
#-- 输入表 : dws.dws_simulation_utility_detail,dim.dim_day_date
#-- 输出表 ：ads.ads_simulation_utility_usage_perday
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-14 CREATE
#-- 1 查博文 2021-12-21 将来源表从ads层切换成dws层
#-- 2 查博文 2021-12-21 增加过滤条件，现在的运行则为一次有效的使用频次。
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
--仿真环境使用频率天维度 ads_simulation_utility_usage_perday （仿真环境使用分析单日使用频率）

INSERT overwrite table ${ads_dbname}.ads_simulation_utility_usage_perday
SELECT 
  data_node                       as date_node,
  bucket_usage_count              as bucket_usage_count,
  container_usage_count           as container_usage_count
FROM 
  (
  SELECT 
  aa.data_node,
  sum(bucket_count) as bucket_usage_count,
  sum(container_count) as container_usage_count
  FROM
    (
    SELECT 
      to_date(start_time) as data_node,
      count(case when simulation_sub_type = '货架搬运(潜伏式)' AND action_detail = '仿真场景运行' THEN 1 ELSE NULL END) as bucket_count, 
      count(case when simulation_sub_type = '纯料箱搬运' AND action_detail = '仿真场景运行' THEN 1 ELSE NULL END) as container_count
    FROM 
      ${dws_dbname}.dws_simulation_utility_detail 
    WHERE 
      user_name <> 'admin'  
    GROUP BY to_date(start_time)
    
    UNION ALL 
    
    SELECT 
      to_date(days) as data_node, 
      0 as bucket_count,
      0 as container_count 
    FROM 
      ${dim_dbname}.dim_day_date 
    WHERE 
      days BETWEEN (SELECT MIN(start_time) FROM ${ads_dbname}.ads_simulation_utility_detail) AND current_date() 
    ) aa
  GROUP BY aa.data_node
  ORDER BY data_node DESC
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

truncate table ads_simulation_utility_usage_perday;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：仿真环境使用频率天维度 ads_simulation_utility_usage_perday （仿真环境使用分析单日使用频率）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_simulation_utility_usage_perday \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_simulation_utility_usage_perday \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "date_node,bucket_usage_count,container_usage_count"


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "