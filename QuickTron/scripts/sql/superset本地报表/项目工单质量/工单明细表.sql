--ads_ft_work_order_detail    --工单明细表（项目工单质量）

INSERT overwrite table ${ads_dbname}.ads_ft_work_order_detail
SELECT '' as id, -- 主键
       ftw.project_ft, -- 所属产品线
       t.project_code, -- 项目编码
       t.project_name, -- 项目名称
       t.project_operation_state, -- 项目运营阶段
       t.product_name, -- 场景（即产品名称）
       t.current_version, -- 产品版本
       t.project_area, -- 项目区域
       t.project_priority, --项目评级
       t.ticket_id, -- 工单编码
       t.summary, -- 工单标题
       t.first_category, -- 一级分类（现象）
       t.second_category, -- 二级分类（现象）
       t.third_category, -- 三级分类（现象）
       t.first_class, -- 一级分类（模块）
       t.second_class, -- 二级分类（模块）
       t.third_class, -- 三级分类（模块）
       t.memo, -- 工单描述
       t.case_status, -- 工单状态
       IF(t.case_status = '6-已关闭','已关闭',IF(t.ticket_id is not null,'未关闭',NULL)) as is_closed, -- 工单是否关闭
       t.case_origin_code, -- 工单来源（微信，钉钉，微信后台，钉钉后台）
       case when t.duty_type in ('早班', '中班') then '白班' 
            when t.duty_type = '晚班' then '夜班' end as duty_type, --值班（白班，夜班）
       case when t.workorder_close_type = '售后' or t.workorder_close_type = '实施' then '设备故障工单'
            when t.workorder_close_type = '技术支持' or (t.workorder_close_type = '研发' and t.issue_type_cname = '任务') then '恢复工单'
            when t.workorder_close_type = '研发' and t.issue_type_cname = '缺陷' then '缺陷工单'
            when t.workorder_close_type = '硬件自动化' then '硬件自动化工单'
            else IF(t.ticket_id is not null,'其他工单',NULL) end as work_order_type, -- 工单类型（设备故障工单，恢复工单，缺陷工单，硬件自动化工单，其他工单）
       t.problem_type, -- 问题类型
       date_format(t.created_time, 'yyyy-MM-dd HH:mm:ss') as created_time, -- 工单创建时间
       t.owner_name as te_user_name, -- 工单对应TE成员
       t.owner_emp_position as te_user_position,  -- 工单对应TE成员职位
       date_format(t.respond_time, 'yyyy-MM-dd HH:mm:ss') as respond_time, --工单响应时间
       t.respond_user, -- 工单响应成员
       t.respond_user_position, -- 工单响应成员职位
       cast(round(t.create_respond_seconds / 3600, 2) as decimal(10, 2)) as respond_duration, -- 工单响应时长
       case when t.is_te_autonomous = '1' then 'TE自主解决' else IF(t.ticket_id is not null,'非TE自主解决',NULL) end as is_te_autonomous, -- 是否TE自主解决（TE自主解决，非TE自主解决）
       date_format(t.to_rb_time, 'yyyy-MM-dd HH:mm:ss') as to_rb_time, -- 转ones的时间
       t.task_no, --ones工作项编码
       t.issue_type_cname, --转ones的工作项类型（任务，缺陷）
       t.ones_create_time, -- ones工作项创建时间
       t.ones_assign_user_dept, -- ones工作项当前负责人所在部门
       t.ones_assign_user, -- ones工作项当前负责人
       t.ones_assign_user_position, -- ones工作项当前负责人职位
       t.ones_solver_user_dept, -- ones工作项解决人所在部门
       t.ones_solver_user, -- ones工作项解决人
       t.ones_solver_user_position, -- ones工作项解决人职位
       date_format(t.ones_solve_time, 'yyyy-MM-dd HH:mm:ss') as ones_solve_time, -- ones研发解决时间
       cast(round((unix_timestamp(t.ones_solve_time) - unix_timestamp(t.to_rb_time))/ 3600, 2) as decimal(10, 2)) as ones_solve_duration, -- ones研发解决时长
       t.ones_close_user_dept, -- ones工作项关闭人所在部门
       t.ones_close_user, -- ones工单关闭人
       t.ones_close_user_position, -- ones工单关闭人职位
       t.ones_close_time, -- ones工单关闭时间
       cast(round((unix_timestamp(t.ones_close_time) - unix_timestamp(t.to_rb_time))/ 3600, 2) as decimal(10, 2)) as ones_close_duration, -- ones工单关闭时长
       t.workorder_close_type, -- 工单关闭方（售后，技术支持，研发）
       t.workorder_close_user, -- 工单关闭人
       t.workorder_close_user_position, -- 工单关闭人职位
       date_format(t.workorder_close_time, 'yyyy-MM-dd HH:mm:ss') as workorder_close_time, -- 工单关闭时间
       cast(round(t.create_close_seconds / 3600, 2) as decimal(10, 2))  as workorder_close_duration, -- 工单关闭时长
       t.person_charge_num, -- 工单经手人数
       t.is_repeat_activate, -- 是否二次激活
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT DISTINCT tft.ft_id,
                  tft.ft_name as project_ft
  FROM ${dim_dbname}.dim_ft_team_info_offline tft
) ftw
LEFT JOIN
(
  SELECT if(tpf.project_ft is null,'未知',tpf.project_ft) as project_ft,
         IF(tpf.true_project_code is null,t.project_code,tpf.true_project_code) as project_code,
         tpf.true_project_sale_code as project_sale_code,
         tpf.project_name,
         if(tpf.project_operation_state_group = 'UNKNOWN','未知',tpf.project_operation_state_group) as project_operation_state,
         if(tpf.product_name = 'UNKNOWN','未知',tpf.product_name) as product_name,
         if(tpf.current_version = 'UNKNOWN','其它',tpf.current_version) as current_version,
         if(tpf.project_area_place is null,'未知',tpf.project_area_place) as project_area,
         tpf.project_priority,
         t.ticket_id,
         t.memo,
         t.case_status,
         t.case_origin_code,
         t.close_name as workorder_close_type,
         t.created_time,
         t.respond_user,
         t.respond_user_position,
         t.respond_time,
         t.close_user as workorder_close_user,
         t.close_user_position as workorder_close_user_position,
         t.close_time as workorder_close_time,
         t.to_rb_time,
         t.create_close_seconds,
         t.owner_name,
         t.owner_emp_position,
         t.duty_type,
         t.create_respond_seconds,
         t.first_category,
         t.second_category,
         t.third_category,
         t.person_charge_num,
         IF(t.is_repeat_activate > 0,'是','否') as is_repeat_activate,
         t.problem_type,
         t1.work_order_id,
         t1.task_no,
         t1.summary,
         t1.issue_type_cname,
         t1.ones_create_time, 
         t1.ones_assign_user_dept,
         t1.ones_assign_user,
         t1.ones_assign_user_position,
         t1.ones_solver_user_dept,
         t1.ones_solver_user,
         t1.ones_solver_user_position,
         t1.ones_solve_time,
         t1.ones_close_user_dept,
         t1.ones_close_user,
         t1.ones_close_user_position,
         t1.ones_close_time,
         case when t.close_name = '技术支持' and t.close_time is not null then 1 else 0 end as is_te_autonomous,
         t1.first_class,
         t1.second_class,
         t1.third_class
  FROM 
  (
    SELECT t1.ticket_id,
           t1.memo,
           t1.case_status,
           t1.case_origin_code,
           t1.close_name,
           t1.created_time,
           t1.project_code,
           t1.owner_name,
           IF(e.emp_position is null,'UNKONWN',e.emp_position) as owner_emp_position,
           t1.duty_type,
           t1.first_category,
           t1.second_category,
           t1.third_category,
           t1.problem_type,
           IF(t2.respond_user is null,'UNKONWN',t2.respond_user) as respond_user,
           IF(t2.respond_user_position is null,'UNKONWN',t2.respond_user_position) as respond_user_position,
           t2.respond_time,
           IF(t2.close_user is null,'UNKONWN',t2.close_user) as close_user,
           IF(t2.close_user_position is null,'UNKONWN',t2.close_user_position) as close_user_position,
           t2.close_time,
           t2.to_rb_time,               
           unix_timestamp(t2.respond_time) - unix_timestamp(t1.created_time)                                                     as create_respond_seconds,
           unix_timestamp(t2.close_time) - unix_timestamp(t1.created_time)                                                       as create_close_seconds,
           IF(t3.person_charge_num is null,0,t3.person_charge_num) + IF(t4.person_charge_num is null,0,t4.person_charge_num)     as person_charge_num,
           IF(t5.is_repeat_activate is null,0,t5.is_repeat_activate) + IF(t6.is_repeat_activate is null,0,t6.is_repeat_activate) as is_repeat_activate
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df t1
    LEFT JOIN  ${dwd_dbname}.dwd_dtk_emp_info_df e
    ON IF(t1.owner_name like '%--%',split(t1.owner_name,'-')[0],t1.owner_name) = e.emp_name AND e.d = DATE_ADD(CURRENT_DATE(), -1) AND e.org_company_name = '上海快仓智能科技有限公司'
    --工单响应关闭信息
    LEFT JOIN
    (
      SELECT tmp.*,e1.emp_position as respond_user_position,e2.emp_position as close_user_position
      FROM
      (
        SELECT ticket_id,
               MAX(case when new_change_value = '已响应' then modify_user end) as respond_user,
               MAX(case when new_change_value = '已响应' then updated_time end) as respond_time,
               MAX(case when new_change_value = '已关闭' then modify_user end) as close_user,
               MAX(case when new_change_value = '已关闭' then updated_time end) as close_time,
               MAX(case when new_change_value = '转研发' then updated_time end) as to_rb_time
        FROM 
        (
          SELECT ticket_id,
                 modify_user,
                 updated_time,
                 old_change_value,
                 new_change_value,
                 ROW_NUMBER() over (partition by ticket_id,new_change_value order by updated_time desc) rk
          FROM ${dwd_dbname}.dwd_ones_work_order_change_record_df
          WHERE 1 = 1
            AND d = DATE_ADD(CURRENT_DATE(), -1) 
            AND order_change_type = '案列状态'
          ORDER BY ticket_id,updated_time
        ) t
        WHERE t.rk = 1
        GROUP BY ticket_id
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e1
      ON IF(tmp.respond_user like '%--%',split(tmp.respond_user,'-')[0],tmp.respond_user) = e1.emp_name AND e1.d = DATE_ADD(CURRENT_DATE(), -1) AND e1.org_company_name = '上海快仓智能科技有限公司'
      LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e2
      ON IF(tmp.close_user like '%--%',split(tmp.close_user,'-')[0],tmp.close_user) = e2.emp_name AND e2.d = DATE_ADD(CURRENT_DATE(), -1) AND e2.org_company_name = '上海快仓智能科技有限公司'
    ) t2 
    ON t2.ticket_id = t1.ticket_id
    --工单流转节点数量
    LEFT JOIN
    (
      SELECT ticket_id,
             COUNT(DISTINCT modify_user) as person_charge_num
      FROM ${dwd_dbname}.dwd_ones_work_order_change_record_df
      WHERE 1 = 1
        AND d = DATE_ADD(CURRENT_DATE(), -1)
        AND order_change_type = '案列状态'
      GROUP BY ticket_id
    ) t3 
    ON t3.ticket_id = t1.ticket_id
    --ones工单流转节点数量
    LEFT JOIN
    (
      SELECT t.field_value,
             COUNT(DISTINCT t.person) as person_charge_num
      FROM 
      (
        SELECT v.task_uuid,
               v.field_value,
               c.task_process_user as person
        FROM ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
        LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
        ON v.task_uuid = c.task_uuid
        WHERE 1 = 1 AND v.field_uuid = 'S993wZTA' AND c.task_process_field = 'field005' AND v.status = '1' -- 状态修改人
        UNION ALL 
        SELECT v.task_uuid,
               v.field_value,
               c.old_task_field_value as person
        FROM ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
        LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
        ON v.task_uuid = c.task_uuid
        WHERE 1 = 1 AND v.field_uuid = 'S993wZTA' AND c.task_process_field = 'field004' AND v.status = '1' -- 原负责人
        UNION ALL 
        SELECT v.task_uuid,
               v.field_value,
               c.new_task_field_value as person
        FROM ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
        LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
        ON v.task_uuid = c.task_uuid
        WHERE 1 = 1 AND v.field_uuid = 'S993wZTA' AND c.task_process_field = 'field004' AND v.status = '1'  -- 现负责人
      ) t 
      GROUP BY t.field_value
    ) t4 
    ON t4.field_value = t1.ticket_id
    --工单是否二次激活
    LEFT JOIN
    (
      SELECT DISTINCT ticket_id,
                      1 as is_repeat_activate
      FROM ${dwd_dbname}.dwd_ones_work_order_change_record_df
      WHERE d = DATE_ADD(CURRENT_DATE(), -1) AND order_change_type = '案列状态' AND old_change_value = '已关闭'
    ) t5 
    ON t5.ticket_id = t1.ticket_id
    --ones工单是否二次激活
    LEFT JOIN
    (
      SELECT v.field_value,
             1 as is_repeat_activate
      FROM ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
      LEFT JOIN ${dwd_dbname}.dwd_one_task_process_change_info_his c
      ON v.task_uuid = c.task_uuid
      WHERE 1 = 1 
        AND v.field_uuid = 'S993wZTA' AND c.task_process_field = 'field005' AND v.status = '1' AND c.old_task_field_value = '已关闭'
      GROUP BY v.field_value
    ) t6
    ON t6.field_value = t1.ticket_id    
    WHERE 1 = 1 
      AND t1.d = DATE_ADD(CURRENT_DATE(), -1) AND t1.project_code is not null AND t1.work_order_status != '已驳回' AND lower(t1.project_code) not regexp 'test|tese'
  ) t
  --项目所属产品线
  LEFT JOIN
  (
    SELECT tmp.*
    FROM 
    (
      SELECT s.project_code as true_project_code,
             s.project_sale_code as true_project_sale_code,
             s.project_name,
             s.project_attr_ft as project_ft,
             s.project_operation_state_group,
             s.project_product_name as product_name,
             s.project_current_version as current_version,
             IF(s.project_code LIKE 'C%' AND s.project_type_id = 8 AND s.project_area_place is null,'销售',s.project_area_place) as project_area_place,
             s.project_priority,
             row_number()over(PARTITION by s.project_sale_code order by h.start_time desc)rn   
      FROM ${dwd_dbname}.dwd_share_project_base_info_df s
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1
      WHERE s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code LIKE 'FH-%' OR s.project_code LIKE 'A%' OR s.project_code LIKE 'C%') AND s.project_type_id IN (0,1,4,7,8,9) AND (s.is_business_project = 0 OR (s.is_business_project = 1 AND s.is_pre_project = 1)) 
    )tmp
    WHERE (tmp.true_project_sale_code IS NULL OR tmp.rn = 1)
  ) tpf 
  ON tpf.true_project_code = t.project_code OR tpf.true_project_sale_code = t.project_code
  --工单对应的ones信息
  LEFT JOIN
  (
    SELECT tmp2.*
    FROM
    (
      SELECT tmp1.*,
             IF(t3.new_org_name_2 is null,'UNKNOWN',t3.new_org_name_2) as ones_close_user_dept,
             row_number()over(PARTITION by tmp1.uuid order by t3.org_change_date desc) rn2
      FROM
      (
        SELECT t1.*,
               IF(t2.new_org_name_2 is null,'UNKNOWN',t2.new_org_name_2) as ones_solver_user_dept,
               row_number()over(PARTITION by t1.uuid order by t2.org_change_date desc) rn1
        FROM
        (
          SELECT t2.field_value as work_order_id,
                 t1.uuid,
                 t1.task_no,
                 t1.summary,
                 t1.issue_type_cname,
                 t1.ones_create_time,
                 t1.ones_assign_user_dept,
                 t1.ones_assign_user,
                 t1.ones_assign_user_position,
                 t1.ones_solver_user,
                 t1.ones_solver_user_position,
                 IF(tt.ones_solve_time is null,'UNKNOWN',tt.ones_solve_time) as ones_solve_time,
                 t1.ones_close_user,
                 t1.ones_close_user_position,
                 tt.ones_close_time,
                 t1.first_class,
                 t1.second_class,
                 t1.third_class
          FROM 
          (
            SELECT t.uuid,
                   t.\`number\` as task_no,
                   t.summary,
                   t.issue_type_cname,
                   t.task_create_time as ones_create_time,
                   c.new_org_name_2 as ones_assign_user_dept,
                   t.task_assign_cname as ones_assign_user ,
                   IF(e1.emp_position is null,'UNKNOWN',e1.emp_position) as ones_assign_user_position,
                   t.task_solver_cname as ones_solver_user,
                   IF(e2.emp_position is null,'UNKNOWN',e2.emp_position) as ones_solver_user_position,
                   t.task_close_cname as ones_close_user,
                   IF(e3.emp_position is null,'UNKNOWN',e3.emp_position) as ones_close_user_position,
                   t.property_value_map['first_category'] as first_class,
                   t.property_value_map['second_category'] as second_class,
                   t.property_value_map['third_category'] as third_class,
                   row_number()over(PARTITION by t.uuid order by c.org_change_date desc) rn
            FROM ${dwd_dbname}.dwd_ones_task_info_ful t
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_change_info_df c
            ON t.task_assign_cname = c.emp_name AND c.d = DATE_ADD(CURRENT_DATE(), -1) AND c.org_change_date <= date(t.server_update_time)
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e1
            ON t.task_assign_email = e1.email AND e1.d = DATE_ADD(CURRENT_DATE(), -1) AND e1.org_company_name = '上海快仓智能科技有限公司'
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e2
            ON t.task_solver_email = e2.email AND e2.d = DATE_ADD(CURRENT_DATE(), -1) AND e2.org_company_name = '上海快仓智能科技有限公司'
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e3
            ON t.task_close_email = e3.email AND e3.d = DATE_ADD(CURRENT_DATE(), -1) AND e3.org_company_name = '上海快仓智能科技有限公司'
          )t1
          LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful t2
          ON t2.task_uuid = t1.uuid AND t2.field_uuid = 'S993wZTA' AND t2.field_value is not null --工单号属性:field_uuid ='S993wZTA'
          --缺陷工单匹配中间解决者
          LEFT JOIN
          (
            SELECT pc.task_uuid,
                   MAX(case when pc.new_task_field_value = '已修复' OR pc.new_task_field_value = '非Bug' OR pc.new_task_field_value = '无法重现' OR pc.new_task_field_value = '重复Bug' then task_process_time end) as ones_solve_time,
                   MAX(case when pc.new_task_field_value = '已关闭' OR pc.new_task_field_value = '已完成' then task_process_time end) as ones_close_time
            FROM ${dwd_dbname}.dwd_one_task_process_change_info_his pc 
            WHERE pc.task_process_field = 'field005' 
            GROUP BY pc.task_uuid
          )tt
          ON tt.task_uuid = t1.uuid
          WHERE t1.rn = 1
        ) t1 
        -- 解决人关联钉钉组织架构
        LEFT JOIN
        (
          SELECT emp_id,emp_name,old_org_name_2,new_org_name_2,org_change_date
          FROM ${dwd_dbname}.dwd_dtk_emp_org_change_info_df 
          WHERE d = DATE_ADD(CURRENT_DATE(), -1)
        ) t2 
        ON t1.ones_solver_user = t2.emp_name AND t2.org_change_date <= date(t1.ones_solve_time)
      )tmp1
      -- 关闭人关联钉钉组织架构
      LEFT JOIN 
      (
        SELECT emp_id,emp_name,old_org_name_2,new_org_name_2,org_change_date
        FROM ${dwd_dbname}.dwd_dtk_emp_org_change_info_df
        WHERE d = DATE_ADD(CURRENT_DATE(), -1)
      ) t3 
      ON tmp1.ones_close_user = t3.emp_name AND t3.org_change_date <= date(tmp1.ones_close_time)
      WHERE tmp1.rn1 = 1
    )tmp2
    WHERE tmp2.rn2 = 1
  )t1
  ON t1.work_order_id = t.ticket_id
)t 
ON t.project_ft = ftw.project_ft;