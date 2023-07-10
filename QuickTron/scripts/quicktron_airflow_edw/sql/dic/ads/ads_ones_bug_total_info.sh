#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads




    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ones缺陷累计燃尽图表 ads_ones_bug_total_info

INSERT overwrite table ${ads_dbname}.ads_ones_bug_total_info
SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
       date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
       d.days, -- 统计日期
       abc.accumulate_bug_created, -- 累计创建数量
       abc1.accumulate_bug_closed, -- 累计关闭数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dim_dbname}.dim_day_date d
ON d.days >= date(s.start_time) AND d.days <= date(s.end_time)
LEFT JOIN    
-- 累计创建数量
(
  SELECT t1.ones_project_uuid, -- ones编码
         t1.ones_project_name, -- ones项目名称
         t1.project_type_name, -- 项目类型名称
         t1.project_bpm_code, -- 内部项目编码
         t1.project_bpm_name, -- 内部项目名称
         t1.sprint_classify_name, -- 迭代名称
         t1.sprint_create_time, -- 迭代开始时间
         t1.sprint_end_time, -- 迭代结束时间
         t1.task_create_date, -- 缺陷创建日期
         SUM(t2.accumulate_bug_created) as accumulate_bug_created
  FROM        
  (
    SELECT pc.uuid as ones_project_uuid, -- ones编码
           pc.project_classify_name as ones_project_name, -- ones项目名称
           pc.project_type_name, -- 项目类型名称
           pc.project_bpm_code, -- 内部项目编码
           b.project_name as project_bpm_name, -- 内部项目名称
           s.sprint_classify_name as sprint_classify_name, -- 迭代名称
           date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
           date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
           d.days as task_create_date, -- 缺陷创建日期
           IF(t.work_num is null,0,t.work_num) as accumulate_bug_created -- 创建缺陷数量
    FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
    LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
    ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
    LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
    ON pc.uuid = s.project_uuid
    LEFT JOIN ${dim_dbname}.dim_day_date d
    ON d.days >= date(s.start_time) AND d.days <= date(s.end_time)
    LEFT JOIN 
    (
    SELECT t.project_uuid,
           t.sprint_uuid,
           t.project_bpm_code,
           date(t.task_create_time) as task_create_date,
           COUNT(DISTINCT t.uuid) as work_num
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname = '缺陷'
    GROUP BY t.project_uuid,t.sprint_uuid,t.project_bpm_code,date(t.task_create_time)
    )t
    ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid AND pc.project_bpm_code = t.project_bpm_code AND t.task_create_date = d.days
    WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
      AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
      AND pc.project_type_name = '内部研发项目'
      AND s.status != 4 -- 剔除已删除的迭代
      AND pc.project_status = 1 -- 项目有效
  )t1
  LEFT JOIN 
  (
    SELECT pc.uuid as ones_project_uuid, -- ones编码
           pc.project_classify_name as ones_project_name, -- ones项目名称
           pc.project_type_name, -- 项目类型名称
           pc.project_bpm_code, -- 内部项目编码
           b.project_name as project_bpm_name, -- 内部项目名称
           s.sprint_classify_name as sprint_classify_name, -- 迭代名称
           date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
           date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
           d.days as task_create_date, -- 缺陷创建日期
           IF(t.work_num is null,0,t.work_num) as accumulate_bug_created -- 创建缺陷数量
    FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
    LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
    ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
    LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
    ON pc.uuid = s.project_uuid
    LEFT JOIN ${dim_dbname}.dim_day_date d
    ON d.days >= date(s.start_time) AND d.days <= date(s.end_time)
    LEFT JOIN 
    (
    SELECT t.project_uuid,
           t.sprint_uuid,
           t.project_bpm_code,
           date(t.task_create_time) as task_create_date,
           COUNT(DISTINCT t.uuid) as work_num
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname = '缺陷'
    GROUP BY t.project_uuid,t.sprint_uuid,t.project_bpm_code,date(t.task_create_time)
    )t
    ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid AND pc.project_bpm_code = t.project_bpm_code AND t.task_create_date = d.days
    WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
      AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
      AND pc.project_type_name = '内部研发项目'
      AND s.status != 4 -- 剔除已删除的迭代
      AND pc.project_status = 1 -- 项目有效
  )t2
  ON t1.ones_project_uuid = t2.ones_project_uuid AND t1.project_type_name = t2.project_type_name AND t1.project_bpm_code = t2.project_bpm_code AND t1.sprint_classify_name = t2.sprint_classify_name AND t1.task_create_date >= t2.task_create_date
  GROUP BY t1.ones_project_uuid,t1.ones_project_name,t1.project_type_name,t1.project_bpm_code,t1.project_bpm_name,t1.sprint_classify_name,t1.sprint_create_time,t1.sprint_end_time,t1.task_create_date
)abc
ON abc.ones_project_uuid = pc.uuid AND abc.project_type_name = pc.project_type_name AND abc.project_bpm_code = pc.project_bpm_code AND abc.sprint_classify_name = s.sprint_classify_name AND abc.task_create_date = d.days
-- 累计关闭数量
LEFT JOIN 
(
  SELECT t1.ones_project_uuid, -- ones编码
         t1.ones_project_name, -- ones项目名称
         t1.project_type_name, -- 项目类型名称
         t1.project_bpm_code, -- 内部项目编码
         t1.project_bpm_name, -- 内部项目名称
         t1.sprint_classify_name, -- 迭代名称
         t1.sprint_create_time, -- 迭代开始时间
         t1.sprint_end_time, -- 迭代结束时间
         t1.task_close_date, -- 缺陷创建日期
         SUM(t2.accumulate_bug_closed) as accumulate_bug_closed
  FROM        
  (
    SELECT pc.uuid as ones_project_uuid, -- ones编码
           pc.project_classify_name as ones_project_name, -- ones项目名称
           pc.project_type_name, -- 项目类型名称
           pc.project_bpm_code, -- 内部项目编码
           b.project_name as project_bpm_name, -- 内部项目名称
           s.sprint_classify_name as sprint_classify_name, -- 迭代名称
           date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
           date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
           d.days as task_close_date, -- 缺陷创建日期
           IF(t.work_num is null,0,t.work_num) as accumulate_bug_closed -- 关闭缺陷数量
    FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
    LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
    ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
    LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
    ON pc.uuid = s.project_uuid
    LEFT JOIN ${dim_dbname}.dim_day_date d
    ON d.days >= date(s.start_time) AND d.days <= date(s.end_time)
    LEFT JOIN 
    (
      SELECT t.project_uuid,
             t.sprint_uuid,
             t.project_bpm_code,
             date(c.task_process_time) as task_close_date,
             COUNT(DISTINCT t.uuid) as work_num
      FROM ${dwd_dbname}.dwd_ones_task_info_ful t
      LEFT JOIN 
      (
        SELECT c.task_uuid,
               c.task_process_time,
               c.old_task_field_value,
               c.new_task_field_value,
               row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
        FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
        WHERE c.task_process_field = 'field005'
      )c
      ON t.uuid = c.task_uuid AND c.rn = 1
      WHERE t.issue_type_cname = '缺陷' AND c.new_task_field_value = '已关闭'
      GROUP BY t.project_uuid,t.sprint_uuid,t.project_bpm_code,date(c.task_process_time)
    )t
    ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid AND pc.project_bpm_code = t.project_bpm_code AND t.task_close_date = d.days
    WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
      AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
      AND pc.project_type_name = '内部研发项目'
      AND s.status != 4 -- 剔除已删除的迭代
      AND pc.project_status = 1 -- 项目有效   
  )t1
  LEFT JOIN 
  (
    SELECT pc.uuid as ones_project_uuid, -- ones编码
           pc.project_classify_name as ones_project_name, -- ones项目名称
           pc.project_type_name, -- 项目类型名称
           pc.project_bpm_code, -- 内部项目编码
           b.project_name as project_bpm_name, -- 内部项目名称
           s.sprint_classify_name as sprint_classify_name, -- 迭代名称
           date_format(s.start_time, 'yyyy-MM-dd HH:mm:ss') as sprint_create_time, -- 迭代开始时间
           date_format(s.end_time, 'yyyy-MM-dd HH:mm:ss') as sprint_end_time, -- 迭代结束时间
           d.days as task_close_date, -- 缺陷创建日期
           IF(t.work_num is null,0,t.work_num) as accumulate_bug_closed -- 关闭缺陷数量
    FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
    LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
    ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
    LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
    ON pc.uuid = s.project_uuid
    LEFT JOIN ${dim_dbname}.dim_day_date d
    ON d.days >= date(s.start_time) AND d.days <= date(s.end_time)
    LEFT JOIN 
    (
      SELECT t.project_uuid,
             t.sprint_uuid,
             t.project_bpm_code,
             date(c.task_process_time) as task_close_date,
             COUNT(DISTINCT t.uuid) as work_num
      FROM ${dwd_dbname}.dwd_ones_task_info_ful t
      LEFT JOIN 
      (
        SELECT c.task_uuid,
               c.task_process_time,
               c.old_task_field_value,
               c.new_task_field_value,
               row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
        FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
        WHERE c.task_process_field = 'field005'
      )c
      ON t.uuid = c.task_uuid AND c.rn = 1
      WHERE t.issue_type_cname = '缺陷' AND c.new_task_field_value = '已关闭'
      GROUP BY t.project_uuid,t.sprint_uuid,t.project_bpm_code,date(c.task_process_time)
    )t
    ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid AND pc.project_bpm_code = t.project_bpm_code AND t.task_close_date = d.days
    WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
      AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
      AND pc.project_type_name = '内部研发项目'
      AND s.status != 4 -- 剔除已删除的迭代
      AND pc.project_status = 1 -- 项目有效   
  )t2
  ON t1.ones_project_uuid = t2.ones_project_uuid AND t1.project_type_name = t2.project_type_name AND t1.project_bpm_code = t2.project_bpm_code AND t1.sprint_classify_name = t2.sprint_classify_name AND t1.task_close_date >= t2.task_close_date
  GROUP BY t1.ones_project_uuid,t1.ones_project_name,t1.project_type_name,t1.project_bpm_code,t1.project_bpm_name,t1.sprint_classify_name,t1.sprint_create_time,t1.sprint_end_time,t1.task_close_date
)abc1
ON abc1.ones_project_uuid = pc.uuid AND abc1.project_type_name = pc.project_type_name AND abc1.project_bpm_code = pc.project_bpm_code AND abc1.sprint_classify_name = s.sprint_classify_name AND abc1.task_close_date = d.days
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
      AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
      AND pc.project_type_name = '内部研发项目'
      AND s.status != 4 -- 剔除已删除的迭代
      AND pc.project_status = 1 -- 项目有效
;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"