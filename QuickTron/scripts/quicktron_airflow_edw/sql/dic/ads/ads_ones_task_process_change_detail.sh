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
--ones需求缺陷状态拉链表 ads_ones_task_process_change_detail

INSERT overwrite table ${ads_dbname}.ads_ones_task_process_change_detail
SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       t.issue_type_cname as work_type, -- 工作项类型
       t.\`number\` as ones_work_id, -- ones工作项编码
       t.summary as ones_work_summary, -- ones工作项标题
       date_format(t.task_create_time, 'yyyy-MM-dd HH:mm:ss') as work_create_time, -- ones工作项创建时间
       t.task_status_cname as work_status, -- ones工作项状态
       t.task_priority_value as work_priority, -- 优先级
       '状态' as process_change_type,
       c.old_task_field_value1 as old_status, -- 变更旧值
       c.new_task_field_value1 as new_status, -- 变更新值
       date_format(c.task_process_time1, 'yyyy-MM-dd HH:mm:ss') as switch_time, -- 变更时间
       c.task_process_user1 as switch_user, -- 变更人
       c.task_process_user2 as last_switch_user, -- 上一状态变更人
       IF(c.rn1 = 1,null,(unix_timestamp(c.task_process_time1)-unix_timestamp(c.task_process_time2))) as switch_gap, -- 状态流转时间差
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t
ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid
-- 状态变更记录
LEFT JOIN 
(
  SELECT c.task_uuid as task_uuid1,
         c.task_process_user as task_process_user1,
         c.task_process_time as task_process_time1,
         c.old_task_field_value as old_task_field_value1,
         c.new_task_field_value as new_task_field_value1,
         c.rn as rn1,
         c1.task_uuid as task_uuid2,
         c1.task_process_user as task_process_user2,
         c1.task_process_time as task_process_time2,
         c1.old_task_field_value as old_task_field_value2,
         c1.new_task_field_value as new_task_field_value2,
         c1.rn as rn2
  FROM 
  (
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time asc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
  )c
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time asc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态
  )c1
  ON c.task_uuid = c1.task_uuid AND c.rn = c1.rn + 1
)c
ON t.uuid = c.task_uuid1
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
  AND t.issue_type_cname in ('需求','缺陷')
  AND c.task_uuid1 is not NULL
  
UNION ALL 

SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       t.issue_type_cname as work_type, -- 工作项类型
       t.\`number\` as ones_work_id, -- ones工作项编码
       t.summary as ones_work_summary, -- ones工作项标题
       date_format(t.task_create_time, 'yyyy-MM-dd HH:mm:ss') as work_create_time, -- ones工作项创建时间
       t.task_status_cname as work_status, -- ones工作项状态
       t.task_priority_value as work_priority, -- 优先级
       '评论' as process_change_type,
       null as old_status, -- 变更旧值
       c.task_comment_content as new_status, -- 变更新值
       date_format(c.task_process_time, 'yyyy-MM-dd HH:mm:ss') as switch_time, -- 变更时间
       c.task_reviewer_name as switch_user, -- 变更人
       null as last_switch_user, -- 上一状态变更人
       null as switch_gap, -- 状态流转时间差
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t
ON pc.uuid = t.project_uuid AND s.sprint_uuid = t.sprint_uuid
-- 评论变更记录
LEFT JOIN 
(
  SELECT c.task_uuid,
         IF(c.task_message_staus = 'updated',c.task_change_time,c.task_process_time) as task_process_time,
         c.task_comment_content,
         c.task_reviewer_name
  FROM ${dwd_dbname}.dwd_ones_task_process_comments_change_info_his c
  WHERE c.task_message_staus != 'deleted'
)c
ON t.uuid = c.task_uuid
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
  AND t.issue_type_cname in ('需求','缺陷')
  AND c.task_comment_content is not NULL;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"