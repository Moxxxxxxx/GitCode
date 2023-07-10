--ads_project_service_cost    --项目劳务成本统计

INSERT overwrite table ${ads_dbname}.ads_project_service_cost
SELECT '' as id,
       a.project_code,
       a.project_name,
       a.fh_project_code,
       a.fh_project_name,
       a.project_status,
       a.labour_type_id,
       a.labour_type,
       a.area,
       a.pm_name,
       a.spm_name,
       a.labour_budget_contract,
       a.labour_budget_incremental,
       a.actual_labour,
       a.oneweek_work_num,
       a.avg_oneweek_work_num, 
       a.twoweek_work_num,
       a.avg_twoweek_work_num,
       a.threeweek_work_num,
       a.avg_threeweek_work_num,
       a.fourweek_work_num,
       a.avg_fourweek_work_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time  
FROM
(
  SELECT tmp1.project_code,
         tmp1.project_name,
         tmp1.fh_project_code,
         tmp1.fh_project_name,
         tmp1.project_status,
         tmp1.labour_type_id,
         tmp1.labour_type,
         tmp1.area,
         tmp1.pm_name,
         tmp1.spm_name,
         tmp1.labour_budget_contract, -- 预算保持A的
         tmp1.labour_budget_incremental, -- 变更预算保持A的
         CONCAT((substring_index(tmp1.actual_labour,'天',1) + substring_index(nvl(tmp2.actual_labour,'0.0'),'天',1)),'天',(IF(substring_index(substring_index(tmp1.actual_labour,'天',-1),'小时',1) ='',0.0,substring_index(substring_index(tmp1.actual_labour,'天',-1),'小时',1)) + IF(substring_index(substring_index(nvl(tmp2.actual_labour,'0.0'),'天',-1),'小时',1) ='',0.0,substring_index(substring_index(nvl(tmp2.actual_labour,'0.0'),'天',-1),'小时',1))),'小时') as actual_labour, -- 实际消耗FH和A叠加
         IF(tmp2.project_code is null,tmp1.oneweek_work_num,tmp1.oneweek_work_num + tmp2.oneweek_work_num) as oneweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.avg_oneweek_work_num,CAST((tmp1.oneweek_work_num + tmp2.oneweek_work_num)/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -8),cast(date_format(DATE_ADD(CURRENT_DATE(), -8),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_oneweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.twoweek_work_num,tmp1.twoweek_work_num + tmp2.twoweek_work_num) as twoweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.avg_twoweek_work_num,CAST((tmp1.twoweek_work_num + tmp2.twoweek_work_num)/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -15),cast(date_format(DATE_ADD(CURRENT_DATE(), -15),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_twoweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.threeweek_work_num,tmp1.threeweek_work_num + tmp2.threeweek_work_num) as threeweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.avg_threeweek_work_num,CAST((tmp1.threeweek_work_num + tmp2.threeweek_work_num)/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -22),cast(date_format(DATE_ADD(CURRENT_DATE(), -22),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_threeweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.fourweek_work_num,tmp1.fourweek_work_num + tmp2.fourweek_work_num) as fourweek_work_num, -- 工单数FH和A叠加
         IF(tmp2.project_code is null,tmp1.avg_fourweek_work_num,CAST((tmp1.fourweek_work_num + tmp2.fourweek_work_num)/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -29),cast(date_format(DATE_ADD(CURRENT_DATE(), -29),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_fourweek_work_num -- 工单数FH和A叠加
  FROM
  (
    SELECT p.project_code, -- 项目编码
           p.project_name, -- 项目名称
           p.mproject_code as fh_project_code, -- 项目售前编号
           p.mproject_name as fh_project_name, -- 项目售前名称
           p.project_status, -- 项目阶段
           m.material_code as labour_type_id,
           m.position_group as labour_type,
           p.area, -- 区域
           p.pm_name, -- pm名称
           IF(p.spm_name is null,t3.spm_name,p.spm_name) as spm_name, -- spm名称
           IF(t1.project_code LIKE 'A%' OR t1.project_code LIKE 'E%',IF(t1.labour_budget_contract is null,0,t1.labour_budget_contract),IF(t7.labour_budget_contract is null,0,t7.labour_budget_contract)) as labour_budget_contract, -- 合同签署劳务预算（人/天）
           IF(t2.project_code LIKE 'A%' OR t2.project_code LIKE 'E%',IF(t2.labour_budget_incremental is null,0,t2.labour_budget_incremental),IF(t8.labour_budget_incremental is null,0,t8.labour_budget_incremental)) as labour_budget_incremental, -- 申请增加的劳务预算（人/天）
           CASE WHEN m.position_group = '运维劳务' THEN IF(t4.check_duration_day is null,'0.0天',t4.check_duration_day)
                WHEN m.position_group = '实施劳务' THEN IF(t4.check_duration_day is null,'0.0天',t4.check_duration_day)
                WHEN m.position_group = 'PM' OR m.position_group = 'PE' OR m.position_group = 'TE' OR m.position_group = '顾问' THEN IF(t5.trip_duration is null,'0.0天',t5.trip_duration)
                WHEN m.position_group = '研发' THEN IF(t6.work_hour is null,'0.0天',t6.work_hour)
           ELSE '0.0天' END as actual_labour, -- 实际的劳务消耗（人/天） 
           IF(t12.work_num is null,0,t12.work_num) as oneweek_work_num, -- 近一周工单数
           IF(t12.work_num is null,0,CAST(t12.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -8),cast(date_format(DATE_ADD(CURRENT_DATE(), -8),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_oneweek_work_num, -- 近一周平均工单数
           IF(t11.work_num is null,0,t11.work_num) as twoweek_work_num, -- 近两周工单数
           IF(t11.work_num is null,0,CAST(t11.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -15),cast(date_format(DATE_ADD(CURRENT_DATE(), -15),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_twoweek_work_num, -- 近二周平均工单数
           IF(t10.work_num is null,0,t10.work_num) as threeweek_work_num, -- 近三周工单数
           IF(t10.work_num is null,0,CAST(t10.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -22),cast(date_format(DATE_ADD(CURRENT_DATE(), -22),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_threeweek_work_num, -- 近三周平均工单数
           IF(t9.work_num is null,0,t9.work_num) as fourweek_work_num, -- 近四周工单数
           IF(t9.work_num is null,0,CAST(t9.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -29),cast(date_format(DATE_ADD(CURRENT_DATE(), -29),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_fourweek_work_num -- 近四周平均工单数
    FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
    -- 物料基础维表
    LEFT JOIN ${dim_dbname}.dim_bpm_material_mapping_info_offline m
    ON 1 = 1
    -- BPM-外部项目交接单
    LEFT JOIN 
    (
      SELECT kf.string31 as project_code, -- 项目编码
             kfe.string22 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_contract -- 合同签署劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf 
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '81687' AND kfe.string22 IN ('R5S90044', 'R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30'
      GROUP BY kf.string31,kfe.string22
    )t1
    ON p.project_code = t1.project_code AND m.material_code = t1.labour_type
    -- BPM-外部项目交接变更单
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.labour_type,
             SUM(tmp.labour_budget_incremental) as labour_budget_incremental
      FROM
      (
        SELECT kf.string6 as project_code, -- 项目编码
               kfe.string43 as labour_type, -- 劳务类型
               SUM(kfe.number24) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
        FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
        LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
        ON kf.flowid = kfe.flowid
        LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
        ON kf.flowid = ef.flow_id
        WHERE kf.oflowmodelid = '82209' AND (kfe.string43 IN ('R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') OR (kfe.string43 = 'R5S90044' and kf.startdate <= '2022-02-28')) AND ef.flow_status = '30'
        GROUP BY kf.string6,kfe.string43
  
        UNION ALL 
      
        SELECT p.project_code, -- 项目编码
               'R5S90044' as labour_type, -- 劳务类型
               SUM(p.total_approved_days) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
        FROM ${dwd_dbname}.dwd_dtk_special_labor_approval_process_info_ful p
        WHERE p.process_result = 'agree' AND p.process_status = 'COMPLETED' AND p.create_time >= '2022-03-01'
        GROUP BY p.project_code
      )tmp
      GROUP BY tmp.project_code,tmp.labour_type
    )t2
    ON p.project_code = t2.project_code AND m.material_code = t2.labour_type
    -- 补充spm字段
    LEFT JOIN 
    (
      SELECT kf.string31 as project_code,
             kf.string30 as project_name,
             kf.string35 as spm_name
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE oflowmodelid= '81687' and string31 is not null AND ef.flow_status = '30'
      GROUP BY string31,string35,kf.string30
      ORDER BY string35 DESC
    )t3
    ON p.project_code = t3.project_code and p.project_name = t3.project_name
    -- 钉钉-劳务考勤
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.service_type,
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
      GROUP BY tmp.project_code,tmp.service_type
    )t4
    ON p.project_code = t4.project_code and m.position_group = t4.service_type
    -- 钉钉-出差申请
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.member_function,
             CONCAT(SUM(tmp.trip_duration),'天')  as trip_duration
      FROM 
      (
        SELECT tt.cur_date,
               tt.create_time,
               tt.business_id,
               tt.project_code,
               tt.project_name,
               tt.project_ft,
               tt.project_operation_state,
               tt.team_name,
               tt.member_name,
               tt.member_function,
               tt.trip_duration,
               tt.start_time,
               tt.end_time
        FROM 
        (
          SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date, -- 统计时间
                 t.create_time,
                 t.business_id, -- 审批业务单号
                 t.project_code, -- 项目编号
                 b.project_name, -- 项目名称
                 IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                 b.project_operation_state, -- 项目运营阶段
                 i.org_cnames as team_name, -- 团队
                 i.emp_name as member_name, -- 成员
                 CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
                      WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
                      WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
                      WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
                      ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
                 t.business_travel_days as trip_duration, -- 出差天数（天）
                 CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
                 CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time, -- 出差结束时间
                 row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
          FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
          LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
          ON t.originator_user_id = i.emp_id
          LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
          ON t.project_code = b.project_code
          WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.is_project_matching = '1' AND t.d = DATE_ADD(CURRENT_DATE(), -1) -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
            AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
            AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
            AND b.d = DATE_ADD(CURRENT_DATE(), -1)
        )tt
        WHERE tt.rn = 1
      )tmp
      GROUP BY tmp.project_code,tmp.member_function
    )t5
    ON p.project_code = t5.project_code and m.position_group = t5.member_function
    -- 研发工时
    LEFT JOIN 
    (
      SELECT tmp1.project_code,
             tmp2.org_role_type,
             CONCAT(round(COALESCE(sum(t.task_spend_hours) / 100000 / 8, 0), 2) ,'天') as work_hour
      FROM 
      (
        SELECT t.uuid,w.ticket_id,w.project_code
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v -- ones对应属性ticket_id和uuid映射关系
        ON v.field_value = w.ticket_id and v.field_uuid = 'S993wZTA' and v.field_value is not null
        LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t -- ones工单数据
        ON t.uuid = v.task_uuid 
        WHERE t.project_classify_name ='工单问题汇总' 
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp1
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
      ON tmp1.uuid = t.task_uuid
      LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
      ON tou.uuid = t.user_uuid and tou.user_status = 1
      LEFT JOIN 
      (
        SELECT *,row_number()over(PARTITION by m.emp_id order by m.org_role_type desc)rn
        FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      )tmp2
      ON tmp2.email = tou.user_email
      WHERE 1 = 1 
        and t.task_type = 1 -- 实际工时
        and t.status = 1 -- 有效
        and t.user_uuid is not null --人员不为空
        and tmp2.rn = 1 -- 按角色排序取其中之一
        and tmp2.org_role_type = '研发' -- 只取研发角色的工时 
      GROUP BY tmp1.project_code,tmp2.org_role_type
    )t6
    ON p.project_code = t6.project_code and m.position_group = t6.org_role_type
    -- 前置项目BOM录入
    LEFT JOIN 
    (
      SELECT kf.string16 as project_code, -- 项目编码
             kfe.string14 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_contract -- 合同签署劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf 
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '82640' AND kfe.string14 IN ('R5S90044', 'R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30'
      GROUP BY kf.string16,kfe.string14
    )t7
    ON p.project_code = t7.project_code AND m.material_code = t1.labour_type
    -- 合同评审前BOM变更
    LEFT JOIN 
    (
      SELECT kf.string16 as project_code, -- 项目编码
             kfe.string14 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
     ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '82641' AND kfe.string43 IN ('R5S90031', 'R5S90044', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30' 
      GROUP BY kf.string16,kfe.string14
    )t8
    ON p.project_code = t8.project_code AND m.material_code = t8.labour_type
    -- 近四周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 4 -- 当前周+前四周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t9
    ON p.project_code = t9.project_code AND m.position_group = t9.role_type
    -- 近三周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 3 -- 当前周+前三周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t10
    ON p.project_code = t10.project_code AND m.position_group = t10.role_type
    -- 近二周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 2 -- 当前周+前两周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t11
    ON p.project_code = t11.project_code AND m.position_group = t11.role_type
    -- 近一周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 1 -- 当前周+前一周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t12
    ON p.project_code = t12.project_code AND m.position_group = t12.role_type
    WHERE (p.project_code LIKE 'A%' OR p.project_code LIKE 'E%' OR  p.project_code LIKE 'FH%')
  )tmp1
  LEFT JOIN
  (
    SELECT p.project_code, -- 项目编码
           p.project_name, -- 项目名称
           p.mproject_code as fh_project_code, -- 项目售前编号
           p.mproject_name as fh_project_name, -- 项目售前名称
           p.project_status, -- 项目阶段
           m.material_code as labour_type_id,
           m.position_group as labour_type,
           p.area, -- 区域
           p.pm_name, -- pm名称
           IF(p.spm_name is null,t3.spm_name,p.spm_name) as spm_name, -- spm名称
           IF(t1.project_code LIKE 'A%' OR t1.project_code LIKE 'E%',IF(t1.labour_budget_contract is null,0,t1.labour_budget_contract),IF(t7.labour_budget_contract is null,0,t7.labour_budget_contract)) as labour_budget_contract, -- 合同签署劳务预算（人/天）
           IF(t2.project_code LIKE 'A%' OR t2.project_code LIKE 'E%',IF(t2.labour_budget_incremental is null,0,t2.labour_budget_incremental),IF(t8.labour_budget_incremental is null,0,t8.labour_budget_incremental)) as labour_budget_incremental, -- 申请增加的劳务预算（人/天）
           CASE WHEN m.position_group = '运维劳务' THEN IF(t4.check_duration_day is null,'0.0天',t4.check_duration_day)
                WHEN m.position_group = '实施劳务' THEN IF(t4.check_duration_day is null,'0.0天',t4.check_duration_day)
                WHEN m.position_group = 'PM' OR m.position_group = 'PE' OR m.position_group = 'TE' OR m.position_group = '顾问' THEN IF(t5.trip_duration is null,'0.0天',t5.trip_duration)
                WHEN m.position_group = '研发' THEN IF(t6.work_hour is null,'0.0天',t6.work_hour)
           ELSE '0.0天' END as actual_labour, -- 实际的劳务消耗（人/天）
           IF(t12.work_num is null,0,t12.work_num) as oneweek_work_num, -- 近一周工单数
           IF(t12.work_num is null,0,CAST(t12.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -8),cast(date_format(DATE_ADD(CURRENT_DATE(), -8),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_oneweek_work_num, -- 近一周平均工单数
           IF(t11.work_num is null,0,t11.work_num) as twoweek_work_num, -- 近两周工单数
           IF(t11.work_num is null,0,CAST(t11.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -15),cast(date_format(DATE_ADD(CURRENT_DATE(), -15),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_twoweek_work_num, -- 近二周平均工单数
           IF(t10.work_num is null,0,t10.work_num) as threeweek_work_num, -- 近三周工单数
           IF(t10.work_num is null,0,CAST(t10.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -22),cast(date_format(DATE_ADD(CURRENT_DATE(), -22),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_threeweek_work_num, -- 近三周平均工单数
           IF(t9.work_num is null,0,t9.work_num) as fourweek_work_num, -- 近四周工单数
           IF(t9.work_num is null,0,CAST(t9.work_num/(DATEDIFF(DATE_ADD(CURRENT_DATE(), -1) ,date_sub(DATE_ADD(CURRENT_DATE(), -29),cast(date_format(DATE_ADD(CURRENT_DATE(), -29),'u') as int) - 1)) + 1) as decimal(10,2))) as avg_fourweek_work_num -- 近四周平均工单数
    FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
    -- 物料基础维表
    LEFT JOIN ${dim_dbname}.dim_bpm_material_mapping_info_offline m
    ON 1 = 1
    -- BPM-外部项目交接单
    LEFT JOIN 
    (
      SELECT kf.string31 as project_code, -- 项目编码
             kfe.string22 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_contract -- 合同签署劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf 
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '81687' AND kfe.string22 IN ('R5S90044', 'R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30'
      GROUP BY kf.string31,kfe.string22
    )t1
    ON p.project_code = t1.project_code AND m.material_code = t1.labour_type
    -- BPM-外部项目交接变更单
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.labour_type,
             SUM(tmp.labour_budget_incremental) as labour_budget_incremental
      FROM
      (
        SELECT kf.string6 as project_code, -- 项目编码
               kfe.string43 as labour_type, -- 劳务类型
               SUM(kfe.number24) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
        FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
        LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
        ON kf.flowid = kfe.flowid
        LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
        ON kf.flowid = ef.flow_id
        WHERE kf.oflowmodelid = '82209' AND (kfe.string43 IN ('R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') OR (kfe.string43 = 'R5S90044' and kf.startdate <= '2022-02-28')) AND ef.flow_status = '30'
        GROUP BY kf.string6,kfe.string43
  
        UNION ALL 
      
        SELECT p.project_code, -- 项目编码
               'R5S90044' as labour_type, -- 劳务类型
               SUM(p.total_approved_days) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
        FROM ${dwd_dbname}.dwd_dtk_special_labor_approval_process_info_ful p
        WHERE p.process_result = 'agree' AND p.process_status = 'COMPLETED' AND p.create_time >= '2022-03-01'
        GROUP BY p.project_code
      )tmp
      GROUP BY tmp.project_code,tmp.labour_type
    )t2
    ON p.project_code = t2.project_code AND m.material_code = t2.labour_type
    -- 补充spm字段
    LEFT JOIN 
    (
      SELECT kf.string31 as project_code,
             kf.string30 as project_name,
             kf.string35 as spm_name
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE oflowmodelid= '81687' and string31 is not null AND ef.flow_status = '30'
      GROUP BY string31,string35,kf.string30
      ORDER BY string35 DESC
    )t3
    ON p.project_code = t3.project_code and p.project_name = t3.project_name
    -- 钉钉-劳务考勤
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.service_type,
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
      GROUP BY tmp.project_code,tmp.service_type
    )t4
    ON p.project_code = t4.project_code and m.position_group = t4.service_type
    -- 钉钉-出差申请
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             tmp.member_function,
             CONCAT(SUM(tmp.trip_duration),'天')  as trip_duration
      FROM 
      (
        SELECT tt.cur_date,
               tt.create_time,
               tt.business_id,
               tt.project_code,
               tt.project_name,
               tt.project_ft,
               tt.project_operation_state,
               tt.team_name,
               tt.member_name,
               tt.member_function,
               tt.trip_duration,
               tt.start_time,
               tt.end_time
        FROM 
        (
          SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date, -- 统计时间
                 t.create_time,
                 t.business_id, -- 审批业务单号
                 t.project_code, -- 项目编号
                 b.project_name, -- 项目名称
                 IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                 b.project_operation_state, -- 项目运营阶段
                 i.org_cnames as team_name, -- 团队
                 i.emp_name as member_name, -- 成员
                 CASE WHEN i.emp_position in ('海外项目经理','海外项目经理兼售前','项目经理','PM Leader','PM','欧洲外英语区交付leader','项目交付组Leader','项目助理','欧洲分公司二区交付leader') THEN 'PM'
                      WHEN i.emp_position in ('海外项目工程师','项目工程师','华北项目实施','实施调试工程师','实施工程师','实施运维工程师','项目实施','项目实施工程师','PE Leader','总部PE leader','华东PE Leader','FAE','FAE Leader','FAE工程师','海外工程师','现场经理','海外交付工程师') THEN 'PE'
                      WHEN i.emp_position in ('技术支持工程师','技术支持组Leader') THEN 'TE'
                      WHEN i.emp_position in ('实施顾问','实施顾问组长') THEN '顾问'
                      ELSE '其他' END as member_function, -- 职能【PM,PE,TE,顾问】
                 t.business_travel_days as trip_duration, -- 出差天数（天）
                 CONCAT(t.start_date,' ',t.start_am_or_pm) as start_time, -- 出差开始时间
                 CONCAT(t.end_date,' ',t.end_am_or_pm) as end_time, -- 出差结束时间
                 row_number()over(PARTITION by t.project_code,t.originator_user_name,t.start_date order by t.create_time desc)rn
          FROM ${dwd_dbname}.dwd_dtk_process_business_travel_df t
          LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df i
          ON t.originator_user_id = i.emp_id
          LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
          ON t.project_code = b.project_code
          WHERE t.approval_status = 'COMPLETED' AND t.approval_result = 'agree' AND t.is_project_matching = '1' AND t.d = DATE_ADD(CURRENT_DATE(), -1) -- 人员以<上海快仓智能科技有限公司>为准,项目以<有效匹配即1>的为准
            AND (t.project_code like 'A%' OR t.project_code like 'C%' OR t.project_code like 'FH%' OR t.project_code like 'E%')
            AND i.d = DATE_ADD(CURRENT_DATE(), -1) AND i.org_company_name = '上海快仓智能科技有限公司'
            AND b.d = DATE_ADD(CURRENT_DATE(), -1)
        )tt
        WHERE tt.rn = 1
      )tmp
      GROUP BY tmp.project_code,tmp.member_function
    )t5
    ON p.project_code = t5.project_code and m.position_group = t5.member_function
    -- 研发工时
    LEFT JOIN 
    (
      SELECT tmp1.project_code,
             tmp2.org_role_type,
             CONCAT(round(COALESCE(sum(t.task_spend_hours) / 100000 / 8, 0), 2) ,'天') as work_hour
      FROM 
      (
        SELECT t.uuid,w.ticket_id,w.project_code
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v -- ones对应属性ticket_id和uuid映射关系
        ON v.field_value = w.ticket_id and v.field_uuid = 'S993wZTA' and v.field_value is not null
        LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t -- ones工单数据
        ON t.uuid = v.task_uuid 
        WHERE t.project_classify_name ='工单问题汇总' 
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp1
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
      ON tmp1.uuid = t.task_uuid
      LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
      ON tou.uuid = t.user_uuid and tou.user_status = 1
      LEFT JOIN 
      (
        SELECT *,row_number()over(PARTITION by m.emp_id order by m.org_role_type desc)rn
        FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      )tmp2
      ON tmp2.email = tou.user_email
      WHERE 1 = 1 
        and t.task_type = 1 -- 实际工时
        and t.status = 1 -- 有效
        and t.user_uuid is not null --人员不为空
        and tmp2.rn = 1 -- 按角色排序取其中之一
        and tmp2.org_role_type = '研发' -- 只取研发角色的工时 
      GROUP BY tmp1.project_code,tmp2.org_role_type
    )t6
    ON p.project_code = t6.project_code and m.position_group = t6.org_role_type
    -- 前置项目BOM录入
    LEFT JOIN 
    (
      SELECT kf.string16 as project_code, -- 项目编码
             kfe.string14 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_contract -- 合同签署劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf 
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
      ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '82640' AND kfe.string14 IN ('R5S90044', 'R5S90031', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30'
      GROUP BY kf.string16,kfe.string14
    )t7
    ON p.project_code = t7.project_code AND m.material_code = t1.labour_type
    -- 合同评审前BOM变更
    LEFT JOIN 
    (
      SELECT kf.string16 as project_code, -- 项目编码
             kfe.string14 as labour_type, -- 劳务类型
             SUM(kfe.number1) as labour_budget_incremental -- 申请增加的劳务预算（人/天）
      FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful kfe
      ON kf.flowid = kfe.flowid
      LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
     ON kf.flowid = ef.flow_id
      WHERE kf.oflowmodelid = '82641' AND kfe.string43 IN ('R5S90031', 'R5S90044', 'R6S90074', 'R6S90060', 'R2S90220', 'R6S90601', 'R6S90059') AND ef.flow_status = '30' 
      GROUP BY kf.string16,kfe.string14
    )t8
    ON p.project_code = t8.project_code AND m.material_code = t8.labour_type
    -- 近四周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 4 -- 当前周+前四周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t9
    ON p.project_code = t9.project_code AND m.position_group = t9.role_type
    -- 近三周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 3 -- 当前周+前三周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t10
    ON p.project_code = t10.project_code AND m.position_group = t10.role_type
    -- 近二周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 2 -- 当前周+前两周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t11
    ON p.project_code = t11.project_code AND m.position_group = t11.role_type
    -- 近一周工单数量
    LEFT JOIN 
    (
      SELECT tmp.project_code,
             '研发' as role_type, -- 只对应研发
             COUNT(DISTINCT tmp.ticket_id) as work_num
      FROM 
      (
        SELECT w.ticket_id,w.project_code,w.created_time
        FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
        WHERE weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 1 -- 当前周+前一周
          and w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'
      )tmp
      GROUP BY tmp.project_code
    )t12
    ON p.project_code = t12.project_code AND m.position_group = t12.role_type
    WHERE (p.project_code LIKE 'A%' OR p.project_code LIKE 'E%' OR  p.project_code LIKE 'FH%')
  )tmp2
  ON tmp1.fh_project_code = tmp2.project_code AND tmp1.labour_type_id = tmp2.labour_type_id 
)a 
LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful p 
ON a.project_code = p.mproject_code
WHERE p.project_code IS NULL;