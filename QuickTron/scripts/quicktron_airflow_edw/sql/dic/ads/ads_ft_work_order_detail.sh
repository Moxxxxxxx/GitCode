#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-01-04 更新项目区域逻辑，新增项目状态阶段相关字段
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
	   t.project_area_group, -- 项目区域组（国内|国外）
       t.pms_project_operation_state, -- pms项目运营状态
       t.pms_project_status, -- 项目状态
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
  SELECT nvl(tpf.project_ft,'未知') as project_ft,
         nvl(tpf.project_code,t.project_code) as project_code,
         tpf.project_sale_code,
         tpf.project_name,
         nvl(tpf.project_operation_state_group,'未知') as project_operation_state,
         nvl(tpf.product_name,'未知') as product_name,
         nvl(tpf.current_version,'其它') as current_version,
         nvl(tpf.project_area_place,'未知') as project_area,
         nvl(tpf.project_priority,'未知') as project_priority,
		 nvl(tpf.project_area_group,'未知') as project_area_group,
         nvl(tpf.pms_project_operation_state,'未知') as pms_project_operation_state,
         nvl(tpf.pms_project_status,'未知') as pms_project_status,
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
    LEFT JOIN  
    (
      SELECT *,row_number()over(PARTITION by e.emp_name order by e.is_job desc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_info_df e
      WHERE e.d = '${pre1_date}' AND e.org_company_name = '上海快仓智能科技有限公司'
    )e
    ON IF(t1.owner_name like '%--%',split(t1.owner_name,'-')[0],t1.owner_name) = e.emp_name AND e.rn = 1
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
            AND d = '${pre1_date}'
            AND order_change_type = '案列状态'
          ORDER BY ticket_id,updated_time
        ) t
        WHERE t.rk = 1
        GROUP BY ticket_id
      )tmp
      LEFT JOIN  
      (
        SELECT *,row_number()over(PARTITION by e.emp_name order by e.is_job desc)rn
        FROM ${dwd_dbname}.dwd_dtk_emp_info_df e
        WHERE e.d = '${pre1_date}' AND e.org_company_name = '上海快仓智能科技有限公司'
      )e1
      ON IF(tmp.respond_user like '%--%',split(tmp.respond_user,'-')[0],tmp.respond_user) = e1.emp_name AND e1.rn = 1
      LEFT JOIN  
      (
        SELECT *,row_number()over(PARTITION by e.emp_name order by e.is_job desc)rn
        FROM ${dwd_dbname}.dwd_dtk_emp_info_df e
        WHERE e.d = '${pre1_date}' AND e.org_company_name = '上海快仓智能科技有限公司'
      )e2
      ON IF(tmp.close_user like '%--%',split(tmp.close_user,'-')[0],tmp.close_user) = e2.emp_name AND e2.rn = 1
    ) t2 
    ON t2.ticket_id = t1.ticket_id
    --工单流转节点数量
    LEFT JOIN
    (
      SELECT ticket_id,
             COUNT(DISTINCT modify_user) as person_charge_num
      FROM ${dwd_dbname}.dwd_ones_work_order_change_record_df
      WHERE 1 = 1
        AND d = '${pre1_date}'
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
      WHERE d = '${pre1_date}' AND order_change_type = '案列状态' AND old_change_value = '已关闭'
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
      AND t1.d = '${pre1_date}' AND t1.project_code is not null AND t1.work_order_status != '已驳回' AND lower(t1.project_code) not regexp 'test|tese'
  ) t
  --项目所属产品线
  LEFT JOIN
  (
    SELECT s.project_code,
           s.project_sale_code,
           s.project_name,
           nvl(s.project_attr_ft,'未知') as project_ft,
           NULL as project_operation_state_group,
           IF(s.project_product_name is null or s.project_product_name = 'UNKNOWN','未知',s.project_product_name) as product_name,
           IF(s.project_current_version is null or s.project_current_version = 'UNKNOWN','其它',s.project_current_version) as current_version,
           CASE WHEN s.data_source = 'BPM' AND s.project_code LIKE 'C%' AND s.project_type_id = 8 AND s.project_area_place is null THEN '销售'
                WHEN s.data_source = 'BPM' THEN nvl(s.project_area_place,'未知')
                WHEN s.data_source = 'PMS' THEN nvl(s.project_area,'未知') end as project_area_place,
           nvl(s.project_priority,'未知') as project_priority,
           s.project_area_type as project_area_group, -- 项目区域组（国内|国外）
           IF(s.data_source = 'BPM',NULL,s.project_operation_state) as pms_project_operation_state, -- pms项目运营状态
           IF(s.data_source = 'BPM',NULL,s.project_dispaly_state) as pms_project_status, -- 项目状态
           s.data_source
    FROM ${dwd_dbname}.dwd_pms_share_project_base_info_df s
    WHERE s.d = '${pre1_date}' 
  ) tpf 
  ON tpf.project_code = t.project_code OR tpf.project_sale_code = t.project_code
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
            ON t.task_assign_cname = c.emp_name AND c.d = '${pre1_date}' AND c.org_change_date <= date(t.server_update_time)
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e1
            ON t.task_assign_email = e1.email AND e1.d = '${pre1_date}' AND e1.org_company_name = '上海快仓智能科技有限公司'
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e2
            ON t.task_solver_email = e2.email AND e2.d = '${pre1_date}' AND e2.org_company_name = '上海快仓智能科技有限公司'
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df e3
            ON t.task_close_email = e3.email AND e3.d = '${pre1_date}' AND e3.org_company_name = '上海快仓智能科技有限公司'
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
          WHERE d = '${pre1_date}'
        ) t2 
        ON t1.ones_solver_user = t2.emp_name AND t2.org_change_date <= date(t1.ones_solve_time)
      )tmp1
      -- 关闭人关联钉钉组织架构
      LEFT JOIN 
      (
        SELECT emp_id,emp_name,old_org_name_2,new_org_name_2,org_change_date
        FROM ${dwd_dbname}.dwd_dtk_emp_org_change_info_df
        WHERE d = '${pre1_date}'
      ) t3 
      ON tmp1.ones_close_user = t3.emp_name AND t3.org_change_date <= date(tmp1.ones_close_time)
      WHERE tmp1.rn1 = 1
    )tmp2
    WHERE tmp2.rn2 = 1
  )t1
  ON t1.work_order_id = t.ticket_id
)t 
ON t.project_ft = ftw.project_ft;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"