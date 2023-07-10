#!/bin/bash
HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="dataplatform"
PASSWORD="quicktron_1014#"
DBNAME="evo_wds_base"             


ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_detail;

"
exit
effo


/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/${DBNAME}?useUnicode=true&characterEncoding=utf-8" \
--username ${USERNAME} \
--password ${PASSWORD} \
--table ads_project_detail \
--columns "project_code,project_name,project_status,project_product,is_customized,project_level,region_or_ft,region_head,implementation_consultant,project_manager,project_member" \
--hcatalog-database ads \
--hcatalog-table ads_devops_project_base_detail \
--input-fields-terminated-by "\t" \
--num-mappers 1  