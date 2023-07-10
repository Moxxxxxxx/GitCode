#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--项目git提交明细 ads_project_git_detail （研发团队能效）

INSERT overwrite table ${ads_dbname}.ads_project_git_detail
SELECT '' as id,
       cast(td.work_date as date) as work_date,
       SUBSTRING_INDEX(tg.git_repository,'/',1) as root_directory,
       SUBSTRING_INDEX(SUBSTRING_INDEX(tg.git_repository,'/',2),'/',-1) as second_level_directory,
       tg.git_repository,
       cast(nvl(tg.add_lines_count, 0) as bigint) as add_lines_count,
       cast(nvl(tg.removed_lines_count, 0) as bigint) as removed_lines_count,
       cast(nvl(tg.total_lines_count, 0) as bigint) as current_lines_count,
       SUM(cast(nvl(tg.total_lines_count, 0) as bigint)) over(PARTITION BY tg.git_repository order by cast(td.work_date as date)) as total_lines_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT DISTINCT days as work_date
  FROM ${dim_dbname}.dim_day_date
  WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
) td
LEFT JOIN
(
  SELECT to_date(t1.git_commit_date) as work_date,
         t1.git_repository,
         SUM(t1.add_lines_count) as add_lines_count,
         SUM(t1.removed_lines_count) as removed_lines_count,
         SUM(t1.total_lines_count) as total_lines_count
  FROM 
  (
    SELECT t.git_commit_date,
           split(t.git_repository,'/opt/gitlab/data/repositories/')[1] as git_repository,
           t.add_lines_count,
           t.removed_lines_count,
           t.total_lines_count
    FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t
    WHERE ((t.git_repository LIKE '/opt/gitlab/data/repositories/hardware%' AND t.git_commit_date >= '2022-01-13') OR (t.git_repository NOT LIKE '/opt/gitlab/data/repositories/hardware%') OR (t.git_repository is NULL)) 
    UNION ALL 
    SELECT '2022-01-12' as git_commit_date,
           'hardware/hw_motionControl/fhmcapp.git' as git_repository,
           0 as add_lines_count,
           0 as removed_lines_count,
           152110 as total_lines_count
    UNION ALL 
    SELECT '2022-01-12' as git_commit_date,
           'hardware/hw_motionControl/libmcdevice.git' as git_repository,
           0 as add_lines_count,
           0 as removed_lines_count,
           136201 as total_lines_count 
     UNION ALL 
     SELECT '2022-01-12' as git_commit_date,
            'hardware/hw_motionControl/libmcmodule.git' as git_repository,
            0 as add_lines_count,
            0 as removed_lines_count,
            129416 as total_lines_count  
     UNION ALL 
     SELECT '2022-01-12' as git_commit_date,
            'hardware/hw_motionControl/libmcservice.git' as git_repository,
            0 as add_lines_count,
            0 as removed_lines_count,
            140483 as total_lines_count
     UNION ALL 
     SELECT '2022-01-12' as git_commit_date,
            'hardware/hw_motionControl/libmcbase.git' as git_repository,
            0 as add_lines_count,
            0 as removed_lines_count,
            252175 as total_lines_count
   )t1
   GROUP BY to_date(t1.git_commit_date),t1.git_repository
) tg 
ON tg.work_date = td.work_date
GROUP BY cast(td.work_date as date),SUBSTRING_INDEX(tg.git_repository,'/',1),SUBSTRING_INDEX(SUBSTRING_INDEX(tg.git_repository,'/',2),'/',-1),tg.git_repository,cast(nvl(tg.add_lines_count, 0) as bigint),cast(nvl(tg.removed_lines_count, 0) as bigint),cast(nvl(tg.total_lines_count, 0) as bigint);
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"