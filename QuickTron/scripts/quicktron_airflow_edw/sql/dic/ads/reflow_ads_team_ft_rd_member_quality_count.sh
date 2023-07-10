#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre11_date=$1
else
    pre11_date=`date -d "-1 day" +%F`
fi


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from ads.ads_team_ft_rd_member_quality_count where '${pre11_date}' >= start_date AND '${pre11_date}' <= end_date;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##研发成员质量统计表 ads_team_ft_rd_member_quality_count
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_rd_member_quality_count \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_rd_member_quality_count \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,start_date,end_date,date_scope,month_date,bug_type,project_classify_name,severity_level,team_ft,team_group,team_sub_group,team_last_group,team_member,is_count,new_bug_num,solve_bug_num,total_bug_num,best_find_bug_num,total_pending_bug_num,repairing_bug_num,unpending_bug_num,pending_bug_num,history_legacy_bug_num,history_pending_bug_num,solve_bug_num_new,best_find_bug_xqps_num,best_find_bug_scbs_num,best_find_bug_svtcs_num,best_find_bug_fatcs_num,best_find_bug_kfsj_num,best_find_bug_scsyx_num,best_find_bug_dgncs_num,best_find_bug_jccs_num,best_find_bug_sxjd_num,find_bug_dgncs_num,find_bug_scbs_num,find_bug_jccs_num,find_bug_scsyx_num,find_bug_fatcs_num,find_bug_shyx_num,find_bug_svtcs_num,timely_solve_num,solve_duration,create_time,update_time"