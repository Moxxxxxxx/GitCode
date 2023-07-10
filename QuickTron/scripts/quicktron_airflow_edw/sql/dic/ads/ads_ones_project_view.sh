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
--ones项目概览表 ads_ones_project_view

INSERT overwrite table ${ads_dbname}.ads_ones_project_view
SELECT '' as id, -- 主键
       pc.uuid as ones_project_uuid, -- ones编码
       pc.project_classify_name as ones_project_name, -- ones项目名称
       pc.project_type_name, -- 项目类型名称
       pc.project_bpm_code, -- 内部项目编码
       b.project_name as project_bpm_name, -- 内部项目名称
       pc.project_assign_name as person_incharge, -- 项目负责人
       sq.sprint_qty as sprint_qty, -- 迭代数量
       s.sprint_classify_name as sprint_classify_name, -- 迭代名称
       date(s.start_time) as sprint_create_time, -- 迭代开始时间
       date(s.end_time) as sprint_end_time, -- 迭代结束时间
       IF(s1.sprint_uuid = s.sprint_uuid,1,0) as is_newset, -- 是否最新迭代
       IF(tsq.test_success_qty is null,0,tsq.test_success_qty) as test_success_qty, -- 成功提测次数
       IF(tiq.test_intime_qty is null,0,tiq.test_intime_qty) as test_intime_qty, -- 及时提测次数
       IF(ttq.total_test_qty is null,0,ttq.total_test_qty) as total_test_qty, -- 已提测总次数
       IF(tdq.total_demand_qty is null,0,tdq.total_demand_qty) as total_demand_qty, -- 需求总数
       IF(ebq.effective_bug_qty is null,0,ebq.effective_bug_qty) as effective_bug_qty, -- 有效bug数量
       IF(sbq.solved_bug_qty is null,0,sbq.solved_bug_qty) as solved_bug_qty, -- 已解决bug数量
       IF(tbq.total_bug_qty is null,0,tbq.total_bug_qty) as total_bug_qty, -- bug总数
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful pc
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b
ON pc.project_bpm_code = b.project_code AND b.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
LEFT JOIN ${dim_dbname}.dim_ones_sprint_info s
ON pc.uuid = s.project_uuid
-- 是否最新迭代is_newset
LEFT JOIN 
(
  SELECT *,row_number()over(PARTITION by s.project_uuid order by s.start_time desc)rn
  FROM ${dim_dbname}.dim_ones_sprint_info s
  WHERE s.status != 4
) s1
ON pc.uuid = s1.project_uuid AND s1.rn = 1
-- 迭代数量sprint_qty
LEFT JOIN 
(
  SELECT s.project_uuid,
         COUNT(DISTINCT sprint_uuid) as sprint_qty
  FROM ${dim_dbname}.dim_ones_sprint_info s
  WHERE s.status != 4
  GROUP BY s.project_uuid
)sq
ON pc.uuid = sq.project_uuid
-- 成功提测次数test_success_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(c.uuid) as test_success_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
  ON t.uuid = c.task_uuid 
  WHERE t.issue_type_cname = '需求'
    AND c.task_process_field = 'field005' -- 状态 
    AND c.old_task_field_value = '已提测' AND c.new_task_field_value = '单功能测试中'
  GROUP BY t.project_uuid,t.sprint_uuid
)tsq
ON pc.uuid = tsq.project_uuid AND s.sprint_uuid = tsq.sprint_uuid
-- 及时提测次数test_intime_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(c.uuid) as test_intime_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
  ON t.uuid = c.task_uuid 
  WHERE t.issue_type_cname = '需求'
    AND c.task_process_field = 'field005' -- 状态 
    AND c.new_task_field_value = '已提测'
    AND c.task_process_time <= t.measure_close_time -- 已提测状态时间<=提测截止时间
  GROUP BY t.project_uuid,t.sprint_uuid 
)tiq
ON pc.uuid = tiq.project_uuid AND s.sprint_uuid = tiq.sprint_uuid
-- 已提测总次数total_test_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(c.uuid) as total_test_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
  ON t.uuid = c.task_uuid 
  WHERE t.issue_type_cname = '需求'
    AND c.task_process_field = 'field005' -- 状态 
    AND c.new_task_field_value = '已提测'
  GROUP BY t.project_uuid,t.sprint_uuid
)ttq
ON pc.uuid = ttq.project_uuid AND s.sprint_uuid = ttq.sprint_uuid
-- 需求总数total_demand_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(DISTINCT t.uuid) as total_demand_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  WHERE t.issue_type_cname = '需求'
  GROUP BY t.project_uuid,t.sprint_uuid
)tdq
ON pc.uuid = tdq.project_uuid AND s.sprint_uuid = tdq.sprint_uuid
-- 有效bug数量effective_bug_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         sum(if(c.is_effective = '有效Bug' or c.task_uuid is null,1,0)) as effective_bug_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN 
  (
    SELECT *,
           IF(c.old_task_field_value IN ('非Bug','重复Bug'),'无效Bug','有效Bug') as is_effective,
           row_number()over(PARTITION by c.task_uuid order by c.task_process_time desc)rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = 'field005' -- 状态 
      AND c.new_task_field_value = '已关闭'
  )c
  ON t.uuid = c.task_uuid AND c.rn = 1
  WHERE t.issue_type_cname = '缺陷'
  GROUP BY t.project_uuid,t.sprint_uuid
)ebq
ON pc.uuid = ebq.project_uuid AND s.sprint_uuid = ebq.sprint_uuid
-- 已解决bug数量solved_bug_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(DISTINCT t.uuid) as solved_bug_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
  ON t.uuid = c.task_uuid 
  WHERE t.issue_type_cname = '缺陷'
    AND c.task_process_field = 'field005' -- 状态 
    AND c.new_task_field_value IN ('已修复','非Bug','无法重现','重复Bug')
  GROUP BY t.project_uuid,t.sprint_uuid
)sbq
ON pc.uuid = sbq.project_uuid AND s.sprint_uuid = sbq.sprint_uuid
-- bug总数total_bug_qty
LEFT JOIN 
(
  SELECT t.project_uuid,
         t.sprint_uuid,
         COUNT(DISTINCT t.uuid) as total_bug_qty
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  WHERE t.issue_type_cname = '缺陷'
  GROUP BY t.project_uuid,t.sprint_uuid
)tbq
ON pc.uuid = tbq.project_uuid AND s.sprint_uuid = tbq.sprint_uuid
WHERE pc.org_name_1 IN ('系统中台','智能搬运FT','箱式FT','AMR FT') 
  AND pc.uuid NOT IN ('HksEArTZU74hPNwq','2XT6VUv4nRSLkPZI') -- [系统中台]系统中台FT-外部需求任务看板 , [智能搬运FT]智能搬运FT-交付组内部看板
  AND pc.project_type_name = '内部研发项目'
  AND s.status != 4 -- 剔除已删除的迭代
  AND pc.project_status = 1 -- 项目有效
;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"