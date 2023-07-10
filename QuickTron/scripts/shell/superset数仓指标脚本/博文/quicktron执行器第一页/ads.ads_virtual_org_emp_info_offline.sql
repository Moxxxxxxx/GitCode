#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 天日期维表
#-- 注意 ： 
#-- 输入表 : dim.dim_virtual_org_emp_info_offline
#-- 输出表 ：ads.ads_virtual_org_emp_info_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-02-11 CREATE
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"

echo "----------------------------------------------------------------------------------###########hive dim => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_virtual_org_emp_info_offline;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：天日期维表 dim_day_date  
/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/$DBNAME?useUnicode=true&characterEncoding=utf-8" \
--username $USERNAME \
--password $PASSWORD \
--table ads_virtual_org_emp_info_offline \
--columns "emp_code,emp_name,role_type,module_branch,org_id,org_name,virtual_org_name" \
--hcatalog-database dim \
--hcatalog-table dim_virtual_org_emp_info_offline \
--input-fields-terminated-by "\t" \
--num-mappers 1  


echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "