#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-16 增加出差、公出数据源
#-- 2 wangyingying 2022-12-29 增加钉钉和汇联易去重判断
#-- 3 wangyingying 2023-01-05 调整人员范围
#-- 4 wangyingying 2023-01-10 调整出差、公出逻辑
# ------------------------------------------------------------------------------------------------


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
--团队小组成员效能 ads_team_ft_role_member_work_efficiency （团队能效）

WITH travel_detail AS 
(
  SELECT *,
         IF(t.rn1 = 1 AND t.rn2 = 1,1,0) AS is_valid -- 是否有效
  FROM 
  (
    SELECT tt.*,
           DENSE_RANK() OVER(partition by tt.originator_user_id,tt.stat_date order by tt.travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY tt.originator_user_id,tt.stat_date,tt.period_type ORDER BY tt.period_type) as rn2
    FROM 
    (
      SELECT t.create_time,
             t.originator_user_id,
             t.travel_date AS stat_date,
             t.every_days AS travel_days,
             t.period_type,
             CASE WHEN t.period_type = '全天' THEN '全天出差'
                  WHEN t.period_type = '下午' THEN '下半天出差'
                  WHEN t.period_type = '上午' THEN '上半天出差' END AS travel_type,
             t.data_source
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
    )tt
  )t
),
business_detail AS 
(
  SELECT *,
         IF(t.rn1 = 1 AND t.rn2 = 1,1,0) AS is_valid -- 是否有效
  FROM 
  (
    SELECT tt.*,
           DENSE_RANK() OVER(partition by tt.originator_user_id,tt.stat_date order by tt.travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY tt.originator_user_id,tt.stat_date,tt.period_type ORDER BY tt.period_type) as rn2
    FROM 
    (
      SELECT t.create_time,
             t.originator_user_id,
             t.attend_bus_date AS stat_date,
             t.every_days AS travel_days,
             t.period_type,
             CASE WHEN t.period_type = '全天' THEN '全天公出'
                  WHEN t.period_type = '下午' THEN '下半天公出'
                  WHEN t.period_type = '上午' THEN '上半天公出' END AS travel_type,
             t.data_source
      FROM ${dwd_dbname}.dwd_dtk_process_attendance_business_dayily_info_df t
      WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
    )tt
  )t
),
work_day AS 
(
  SELECT nvl(tmp1.originator_user_id,nvl(tmp2.originator_user_id,tmp3.originator_user_id)) as originator_user_id,
         nvl(tmp1.stat_date,nvl(tmp2.stat_date,tmp3.stat_date)) as stat_date,
         tmp1.create_time as travel_create_time,
         tmp1.travel_type as travel_type,
         tmp2.create_time as work_home_create_time,
         tmp2.travel_type as work_home_type,
         tmp3.create_time as attend_bus_create_time,
         tmp3.travel_type as attend_bus_type
  FROM
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days,
           t1.data_source
    FROM travel_detail t1
    WHERE t1.is_valid = 1
  )tmp1
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days    
    FROM 
    (
      SELECT w.originator_user_id,
             w.create_time,
             cast(w.work_home_date as date) as stat_date,
             CASE when w.period_type = '全天' THEN '全天居家'
                  when w.period_type = '下午' THEN '下半天居家'
                  when w.period_type = '上午' THEN '上半天居家' end as travel_type,
             w.every_days as travel_days,
             row_number()over(PARTITION by w.originator_user_id,cast(w.work_home_date as date) order by w.create_time desc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_for_home_dayily_info_df w
      WHERE w.is_valid = 1 AND w.d = '${pre1_date}' AND w.approval_result = 'agree' AND w.approval_status = 'COMPLETED' 
    )t1
    WHERE t1.rn = 1 
  )tmp2
  ON tmp1.originator_user_id = tmp2.originator_user_id AND tmp1.stat_date = tmp2.stat_date
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days,
           t1.data_source
    FROM business_detail t1
    WHERE t1.is_valid = 1
  )tmp3
  ON tmp1.originator_user_id = tmp3.originator_user_id AND tmp1.stat_date = tmp3.stat_date
)


INSERT overwrite table ${ads_dbname}.ads_team_ft_role_member_work_efficiency
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.team_last_group,
	   tud.emp_position,
       tud.user_name as team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '内部工作项目' as project_classify_name,
       cast(tud.days as date) as work_date,
       IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) as day_type, -- 判断是否加班 、 判断是否请假 、 判断工作日是否哺乳假 、 判断周末是否哺乳假 ： 周末哺乳假 => 周末 、 工作日哺乳假 => 工作日-哺乳假 、 请假 => 请假类型 、 加班 => 加班
       CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 6  -- 全天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 3 -- 周末半天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 3 -- 工作日半天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 5 -- 哺乳假全天-1小时
            ELSE 0 END as lowest_saturation_workhour,
       CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 10  -- 全天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 5 -- 周末半天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 5 -- 工作日半天
            WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 9 -- 哺乳假全天-1小时
            ELSE 0 END as highest_saturation_workhour, 
       CASE WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) != 0 AND 
                 (IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('周末','节假日','全天请假') 
                 OR 
                 (tud.day_type in('周末') AND IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假')))
                 THEN '*加班'
            WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) >
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 10  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 5 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 5 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 9 -- 哺乳假全天-1小时
                 ELSE 0 END THEN '过度饱和'
            WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) <
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 6  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 3 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 3 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 5 -- 哺乳假全天-1小时
                 ELSE 0 END THEN '未饱和'
            ELSE '饱和' END is_saturation,  
       CASE WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) != 0 AND tud.day_type in('周末','节假日') AND
                 IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('周末','节假日','全天请假','下半天请假','上半天请假') 
                 THEN 0 -- 未提交加班差值为0
            WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) >
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 10  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 5 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 5 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 9 -- 哺乳假全天-1小时
                 ELSE 0 END
                 THEN 
                 cast(nvl(t2.work_hour, 0) as decimal(10, 2)) - 
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 10  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 5 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 5 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 9 -- 哺乳假全天-1小时
                 ELSE 0 END
            WHEN cast(nvl(t2.work_hour, 0) as decimal(10, 2)) <
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 6  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 3 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 3 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 5 -- 哺乳假全天-1小时
                 ELSE 0 END
                 THEN 
                 cast(nvl(t2.work_hour, 0) as decimal(10, 2)) -
                 CASE WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日','调休','全天加班') THEN 6  -- 全天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天加班','上半天加班') THEN 3 -- 周末半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('下半天请假','上半天请假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 3 -- 工作日半天
                      WHEN IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) in ('工作日-哺乳假','调休-哺乳假') THEN 5 -- 哺乳假全天-1小时
                 ELSE 0 END
            ELSE 0 END saturation_d_value,
       cast(nvl(t1.code_quantity, 0) as bigint)                          as code_quantity,
       cast(nvl(t2.work_hour, 0) as decimal(10, 2))                      as work_hour,
       cast(nvl(t15.work_hour, 0) as decimal(10, 2))                     as invalid_work_hour,
       cast(nvl(t16.work_hour, 0) as decimal(10, 2))                     as code_unusual_work_hour,
       cast(nvl(t17.work_hour, 0) as decimal(10, 2))                     as breakrule_work_hour,
       cast(nvl(t3.newly_increased_defect_num, 0) as bigint)             as newly_increased_defect_num,
       cast(nvl(t4.solve_defect_num, 0) as bigint)                       as solve_defect_num,
       cast(nvl(t5.close_defect_num, 0) as bigint)                       as close_defect_num,
       cast(nvl(t6.newly_increased_task_num, 0) as bigint)               as newly_increased_task_num,
       cast(nvl(t7.solve_task_num, 0) as bigint)                         as solve_task_num,
       cast(nvl(t8.close_task_num, 0) as bigint)                         as close_task_num,
       cast(nvl(t9.newly_increased_demand_num, 0) as bigint)             as newly_increased_demand_num,
       cast(nvl(t10.solve_demand_num, 0) as bigint)                      as solve_demand_num,
       cast(nvl(t11.close_demand_num, 0) as bigint)                      as close_demand_num,
       cast(nvl(t12.newly_increased_workorder_num, 0) as bigint)         as newly_increased_workorder_num,
       cast(nvl(t13.solve_workorder_num, 0) as bigint)                   as solve_workorder_num,
       cast(nvl(t14.close_workorder_num, 0) as bigint)                   as close_workorder_num,
       cast(nvl(t18.work_hour, 0) as decimal(10, 2))                     as manage_work_hour,
       cast(nvl(t19.once_day_work_times, 0) as decimal(10, 2))           as once_day_work_times,
       cast(nvl(t20.once_over_sixhour_work_times, 0) as decimal(10, 2))  as once_over_sixhour_work_times,
       t21.attendance_working_time,
       t21.attendance_off_time,
       t21.attendance_working_place,
       t21.attendance_off_place,
       cast(nvl(t21.clock_in_work_hour, 0) as decimal(10, 2))            as clock_in_work_hour,
       cast(nvl(t22.travel_days, 0) as decimal(10, 2))                   as travel_days,
       nvl(t22.travel_type,'公司')                                       as travel_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')           as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')           as update_time
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
	     tu.team_sub_group,
         tu.team_last_group,
         tu.emp_id,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tu.is_need_fill_manhour,
         tu.emp_position,
         td.days,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type    
  FROM
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    tg.org_name_5 as team_last_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job,
                    tmp.is_need_fill_manhour,
                    te.hired_date,
                    te.quit_date,
                    te.emp_position
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc,m.org_id asc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司'
      AND IF(te.is_job = 1,tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 IN ('制造部') AND tg.org_name_3 IN ('工程组','测试')) OR (tg.org_name_2 IN ('研发管理部') AND tg.org_name_3 IN ('产研质量组','效能工具组')),te.dept_name LIKE '%AMR FT%' OR te.dept_name LIKE '%智能搬运FT%' OR te.dept_name LIKE '%硬件自动化%' OR te.dept_name LIKE '%箱式FT%' OR te.dept_name LIKE '%系统中台%' OR te.dept_name LIKE '%制造部%' OR te.dept_name LIKE '%研发管理部%') -- 只筛选AMR FT、智能搬运FT、硬件自动化、箱式FT、系统中台、制造部、研发管理部
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
  ) td
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') 
) tud
-- 代码量统计
LEFT JOIN 
(
  SELECT to_date(t1.git_commit_date) as stat_date,
         t1.git_author_email as true_email,
         SUM(IF(nvl(t1.add_lines_count,0) >= 2000,2000,nvl(t1.add_lines_count,0))) as code_quantity
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  WHERE t1.git_repository NOT LIKE '%software/phoenix/aio/phoenix-rcs-aio.git'
  GROUP BY to_date(t1.git_commit_date),t1.git_author_email
) t1
ON t1.true_email = tud.user_email AND t1.stat_date = tud.days
-- 工时统计
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t2 
ON t2.user_email = tud.user_email AND t2.stat_date = tud.days
-- 新增缺陷数量
LEFT JOIN 
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t3 
ON t3.task_assign_email = tud.user_email AND t3.stat_date = tud.days
-- 解决缺陷数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name != '工单问题汇总' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t4 
ON t4.task_solver_email = tud.user_email AND t4.stat_date = tud.days
-- 关闭缺陷数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_close_email,
         count(distinct t1.uuid)        as close_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name != '工单问题汇总' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t5 
ON t5.task_close_email = tud.user_email AND t5.stat_date = tud.days 
-- 新增任务数量
LEFT JOIN
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t6 
ON t6.task_assign_email = tud.user_email AND t6.stat_date = tud.days
-- 解决任务数量
LEFT JOIN
( 
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name != '工单问题汇总' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t7 
ON t7.task_solver_email = tud.user_email AND t7.stat_date = tud.days
-- 关闭任务数量
LEFT JOIN
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_close_email,
         count(distinct t1.uuid)        as close_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务'  AND t1.project_classify_name != '工单问题汇总'AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t8 
ON t8.task_close_email = tud.user_email AND t8.stat_date = tud.days
-- 新增需求数量
LEFT JOIN 
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t9 
ON t9.task_assign_email = tud.user_email AND t9.stat_date = tud.days
-- 解决需求数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name != '工单问题汇总' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t10 
ON t10.task_solver_email = tud.user_email AND t10.stat_date = tud.days
-- 关闭需求数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
		 t1.task_close_email,
         count(distinct t1.uuid)        as close_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name != '工单问题汇总' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t11 
ON t11.task_close_email = tud.user_email AND t11.stat_date = tud.days
-- 新增工单数量
LEFT JOIN 
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_workorder_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.project_classify_name = '工单问题汇总'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t12
ON t12.task_assign_email = tud.user_email AND t12.stat_date = tud.days
-- 解决工单数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_workorder_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.project_classify_name = '工单问题汇总' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t13
ON t13.task_solver_email = tud.user_email AND t13.stat_date = tud.days
-- 关闭工单数量
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
		 t1.task_close_email,
         count(distinct t1.uuid)        as close_workorder_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.project_classify_name = '工单问题汇总' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t14 
ON t14.task_close_email = tud.user_email AND t14.stat_date = tud.days
-- 无效工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is null 
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t15
ON t15.user_email = tud.user_email AND t15.stat_date = tud.days
-- 编码异常
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
    AND (IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null),'未知项目编码',t1.external_project_code) = '未知项目编码'
	       OR IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null),'未知项目编码',t1.project_bpm_code) = '未知项目编码')
	AND t1.project_type_name != '技术&管理工作'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t16
ON t16.user_email = tud.user_email AND t16.stat_date = tud.days
-- 违规登记
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) > 7
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t17
ON t17.user_email = tud.user_email AND t17.stat_date = tud.days
-- 技术&管理工作时长
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name = '技术&管理工作'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t18
ON t18.user_email = tud.user_email AND t18.stat_date = tud.days
-- 当天登记总次数
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         count(t.uuid) as once_day_work_times
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t19
ON t19.user_email = tud.user_email AND t19.stat_date = tud.days
-- 单次登记大于6小时次数
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         count(t.uuid) as once_over_sixhour_work_times
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.task_spend_hours > 6
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t20
ON t20.user_email = tud.user_email AND t20.stat_date = tud.days
-- 打卡工时数
LEFT JOIN 
(
  SELECT e.emp_id,
         e.emp_name,
         e.att_checkin_work_date as attendance_work_date,
         e.att_checkin_start_time as attendance_working_time,
         e.att_checkin_end_time as attendance_off_time,
         IF(e.start_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_start_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_start_place,IF(e.start_att_checkin_type IN ('钉钉签到','用户打卡'),e.att_checkin_start_place,e.start_att_checkin_type)) as attendance_working_place,
         IF(e.end_att_checkin_type = '考勤机打卡（指纹/人脸打卡）' AND e.att_checkin_end_place IN ('宝仓智能科技(苏州)有限公司_52989','B05考勤机','B05员工通道','B06考勤机'),e.att_checkin_end_place,IF(e.end_att_checkin_type IN ('钉钉签到','用户打卡'),e.att_checkin_end_place,e.end_att_checkin_type)) as attendance_off_place,
         cast((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(e.att_checkin_start_time))/3600 as decimal(10,1)) as clock_in_work_hour
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di e
)t21
ON tud.emp_id = t21.emp_id AND tud.days = t21.attendance_work_date
-- 出勤类型
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%出差' THEN '出差'
              when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%居家' THEN '居家'
              when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%公出' THEN '公出' end as travel_type,
         SUM(CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '全天%' THEN 1
                  when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%半天%' THEN 0.5 end) as travel_days
  FROM work_day
  GROUP BY originator_user_id,stat_date,
           CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%出差' THEN '出差'
                when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%居家' THEN '居家'
                when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%公出' THEN '公出' end
)t22
ON t22.originator_user_id = tud.emp_id AND t22.stat_date = tud.days
-- 请假统计
LEFT JOIN 
(
  SELECT l1.originator_user_id,
         l1.stat_date,
         case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type
  FROM 
  (
    SELECT l.originator_user_id,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                       when l.period_type = '下午' THEN '下半天请假'
                                                                                                       when l.period_type = '上午' THEN '上半天请假' 
                                                                                                       when l.period_type = '其它' THEN '哺乳假' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l1
  LEFT JOIN 
  (
    SELECT l.originator_user_id,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                       when l.period_type = '下午' THEN '下半天请假'
                                                                                                       when l.period_type = '上午' THEN '上半天请假' 
                                                                                                       when l.period_type = '其它' THEN '哺乳假' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
  WHERE l1.rn = 1 
)tt1
ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
-- 加班统计
LEFT JOIN 
(
  SELECT l1.applicant_userid,
         l1.stat_date,
         case when l2.work_overtime_type is null THEN l1.work_overtime_type else '全天加班' END as work_overtime_type
  FROM 
  (
    SELECT l.applicant_userid,
           cast(l.overtime_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天加班'
                when l.period_type = '下午' THEN '下半天加班'
                when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
           row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                        when l.period_type = '下午' THEN '下半天加班'
                                                                                                        when l.period_type = '上午' THEN '上半天加班' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
    WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l1
  LEFT JOIN 
  (
    SELECT l.applicant_userid,
           cast(l.overtime_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天加班'
                when l.period_type = '下午' THEN '下半天加班'
                when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
           row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                        when l.period_type = '下午' THEN '下半天加班'
                                                                                                        when l.period_type = '上午' THEN '上半天加班' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
    WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.applicant_userid = l2.applicant_userid AND l1.stat_date = l2.stat_date AND l1.work_overtime_type != l2.work_overtime_type
  WHERE l1.rn = 1 
)tt2
ON tt2.applicant_userid = tud.emp_id AND tt2.stat_date = tud.days;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      