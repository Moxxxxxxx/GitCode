--ads_project_service_day_month    --项目投入劳务人天月统计

INSERT overwrite table ${ads_dbname}.ads_project_service_day_month
SELECT '' as id, -- 主键
       t1.true_project_code as project_code,
       t1.month_scope as cur_month,
       t1.service_type,
       IF(t2.days is null,0,t2.days) as days,
       IF(t2.hours is null,0,t2.hours) as hours,
       IF(t2.check_duration_day is null,'0.0天0.0小时',t2.check_duration_day) as check_duration_day,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT p.true_project_code,
         td.month_scope,
         p.service_type,
         p.days,
         p.hours,
         p.check_duration_day
  FROM
  (
    SELECT DISTINCT date_format(d.days,'yyyy-MM') as month_scope
    FROM ${dim_dbname}.dim_day_date d
  )td
  left join
  (
    SELECT tmp.true_project_code,
           tmp.month_scope,
           tmp.service_type,
           tmp.days,
           tmp.hours,
           tmp.check_duration_day,
           row_number()over(PARTITION by tmp.true_project_code,tmp.service_type order by tmp.month_scope)rn
    FROM 
    (   
      SELECT tt.month_scope,
             tt.project_code,
             tt.service_type,
             tt.days,
             tt.hours,
             tt.check_duration_day,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code,tt.month_scope,tt.service_type order by h.start_time desc)rn
      FROM 
      (
        SELECT date_format(tmp.cur_date,'yyyy-MM') as month_scope,
               tmp.project_code,
        	   tmp.service_type,
        	   SUM(substring_index(check_duration_day,'天',1)) as days,
        	   SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))) as hours,
         	   CONCAT(SUM(substring_index(check_duration_day,'天',1)),'天',SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))),'小时') as check_duration_day
      	FROM 
        (
          SELECT tt1.cur_date,
                 tt1.project_code,
             	 tt1.project_name,
                 tt1.project_ft,
               	 tt1.project_operation_state,
               	 tt1.originator_dept_name as team_name,
               	 tt1.originator_user_name as member_name,
                 tt1.service_type,
                 SUM(tt1.check_duration) as check_duration_hour,
                 case when SUM(tt1.check_duration) < 4 then '0天'
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
                      when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day
          FROM 
          (
            SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
            FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
          	LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
           	ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt1
          LEFT JOIN 
          (
        	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
        	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
            LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
            ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt2
          ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
          WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
          GROUP BY tt1.cur_date,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type
      	)tmp
        GROUP BY date_format(tmp.cur_date,'yyyy-MM'),tmp.project_code,tmp.service_type
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tt.project_code or s.project_sale_code = tt.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )p
  ON td.month_scope >= p.month_scope AND td.month_scope <= date_format(DATE_ADD(CURRENT_DATE(), -1),'yyyy-MM')
  WHERE p.true_project_code is not null AND p.rn = 1
)t1
LEFT JOIN
-- PE人天
(
  SELECT a.true_project_code,
         a.month_scope,
         a.service_type,
         SUM(a.days) as days,
         SUM(a.hours) as hours, 
         CONCAT(SUM(substring_index(a.check_duration_day,'天',1)),'天',SUM(IF(substring_index(substring_index(a.check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(a.check_duration_day,'天',-1),'小时',1))),'小时') as check_duration_day
  FROM
  (
    SELECT tmp.*
    FROM 
    (   
      SELECT tt.month_scope,
             tt.project_code,
             tt.service_type,
             tt.days,
             tt.hours,
             tt.check_duration_day,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code,tt.month_scope,tt.service_type order by h.start_time desc)rn
      FROM 
      (
        SELECT date_format(tmp.cur_date,'yyyy-MM') as month_scope,
               tmp.project_code,
        	   tmp.service_type,
        	   SUM(substring_index(check_duration_day,'天',1)) as days,
        	   SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))) as hours,
         	   CONCAT(SUM(substring_index(check_duration_day,'天',1)),'天',SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))),'小时') as check_duration_day
      	FROM 
        (
          SELECT tt1.cur_date,
                 tt1.project_code,
             	 tt1.project_name,
                 tt1.project_ft,
               	 tt1.project_operation_state,
               	 tt1.originator_dept_name as team_name,
               	 tt1.originator_user_name as member_name,
                 tt1.service_type,
                 SUM(tt1.check_duration) as check_duration_hour,
                 case when SUM(tt1.check_duration) < 4 then '0天'
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
                      when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day
          FROM 
          (
            SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
            FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
          	LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
           	ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt1
          LEFT JOIN 
          (
        	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
        	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
            LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
            ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt2
          ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
          WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
          GROUP BY tt1.cur_date,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type
      	)tmp
        GROUP BY date_format(tmp.cur_date,'yyyy-MM'),tmp.project_code,tmp.service_type
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tt.project_code or s.project_sale_code = tt.project_code)
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )a 
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code,a.month_scope,a.service_type
)t2
ON t1.true_project_code = t2.true_project_code and t1.month_scope = t2.month_scope and t1.service_type = t2.service_type;