#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ： d     
#-- 功能描述 ： 获取git明细代码贡献量
#-- 注意 ：
#-- 输入表 : quality_data.git_commit_detail
#-- 输出表 ods.ods_qkt_git_commit_detail_da
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-22 CREATE 
#-- 2 wangziming 2022-09-01 modify 修改回流五天数据，并改为datax进行数据采集
# ------------------------------------------------------------------------------------------------

start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase  quality_data
\--userName root 
\--passWord quicktron123456 
\--querySql  select id, git_commit_date, git_commit_id, git_repository,git_commit_time, git_author, git_author_email, git_branch, git_commit_desc, add_lines_count, removed_lines_count, total_lines_count, change_files_count, is_no_merge, create_time,update_time,git_commit_date as d from git_commit_detail where git_commit_date>=date_sub('\${pre1_date}',interval 4 day )
\--separator 
\--writerPlugin hivewriter 
\--dataBase ods 
\--table ods_qkt_git_commit_detail_da 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d 
\--column id, git_commit_date:1, git_commit_id, git_repository, git_commit_time:2, git_author, git_author_email, git_branch, git_commit_desc, add_lines_count, removed_lines_count, total_lines_count, change_files_count, is_no_merge, create_time:2, update_time:2,d:1" "ods_qkt_git_commit_detail_da"