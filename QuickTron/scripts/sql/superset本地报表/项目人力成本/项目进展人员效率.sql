--ads_project_member_effcive    --项目进展人员效率

-- project_view_detail 项目大表明细
-- pe_detail pe日志明细
with project_view_detail as 
(
SELECT case when b.project_code like 'A%' THEN 'A'
            when b.project_code like 'C%' THEN 'C'
            when b.project_code like 'FH-%' THEN 'FH'
            else '未知' end as project_code_class, -- 项目编码种类
       b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name, -- 项目名称
       CONCAT(b.project_code,'-',b.project_name) as project_info,
       IF(b.project_product_name is null,'未知',b.project_product_name) as project_product_name, -- 产品线
       IF(t2.project_code is not null,'历史项目','新项目') as project_type, -- 项目类型
       b.project_dispaly_state, -- 项目阶段
       b.project_dispaly_state_group, -- 项目阶段组
       IF(b.project_ft is null,'未知',b.project_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
       b.project_priority, -- 项目评级
       IF(b.project_current_version is null,'未知',b.project_current_version) as project_current_version, -- 版本号
       t1.sales_area_director, -- owner
       IF(b.project_area_place is null,'未知',b.project_area_place) as project_area, -- 区域-PM
       t1.pm_name, -- PM
       b.sap_counselor, -- 顾问
       t1.sales_person, -- 销售
       t1.pre_sales_consultant, -- 售前顾问
       IF(b.contract_signed_year is null,t2.contract_sign_year,b.contract_signed_year) as contract_signed_year, -- 合同日期
       IF(b.contract_signed_date is null,t2.contract_sign_date,b.contract_signed_date) as contract_signed_date, -- 合同日期
       IF(t4.fhsl is null and t3.cgsl is null,null,IF(CAST(t4.fhsl / t3.cgsl as decimal(10,4)) is null,0,CAST(t4.fhsl / t3.cgsl as decimal(10,4)))) as deliver_goods_achieving_rate, -- 发货完成率
       b.pre_project_approval_time, -- 前置申请完成时间
       b.project_handover_end_time, -- 交接审批完成时间
       b.expect_online_date, -- 预计上线时间
       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       date(CONCAT(IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month),'-01')) as online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已上线',IF(t2.project_code is not null,'已上线',b.is_online)) as is_online, -- 是否上线
       b.sap_entry_date, -- 实施入场时间
       b.online_times, -- 上线时长
       IF(t2.project_code is not null,NULL,b.no_online_times) as no_online_times, -- 持续未上线天数
       b.expect_final_inspection_date, -- 预计终验时间
       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.final_inspection_date) as final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.final_inspection_process_month) as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
       date(CONCAT(IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.final_inspection_process_month),'-01')) as final_inspection_process_month_begin, -- 终验单审批月初 => <终验报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已验收',IF(t2.project_code is not null,'已验收',b.is_final_inspection)) as is_final_inspection, -- 是否终验
       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,0,b.final_inspection_times) as final_inspection_times, -- 终验时长
       b.no_final_inspection_times, -- 持续未验收天数
       CASE when t2.project_code is not null AND b.project_dispaly_state_group != '项目结项' THEN '已验收未结项' -- 历史项目+项目未结项
            when t2.project_code is not null AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 历史项目+项目已结项
            when b.project_type_name = '纯硬件项目' AND t7.project_code is null then '未发货未上线' -- 纯硬件项目+设备到货审批完成
            when b.project_type_name = '纯硬件项目' AND t7.project_code is not null then '已结项' -- 纯硬件项目+设备到货审批未完成
            when b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl is null THEN '未发货未上线' -- 外部项目+未上线未验收+未发货
            when b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl is not null THEN '已发货未上线' -- 外部项目+未上线未验收+已发货
            when b.project_type_name != '纯硬件项目' AND b.is_online = '已上线' AND b.is_final_inspection = '未验收' THEN '已上线未验收' -- 外部项目+已上线未验收
            when b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '已验收未结项' -- 外部项目+已验收+项目未结项
            when b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       end as project_stage, -- 项目阶段
       CASE when t2.project_code is not null AND b.project_dispaly_state_group != '项目结项' THEN '结项阶段' -- 历史项目+项目未结项
            when t2.project_code is not null AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 历史项目+项目已结项
            when b.project_type_name = '纯硬件项目' AND t7.project_code is null then '发货阶段(硬件项目)' -- 纯硬件项目+设备到货审批完成
            when b.project_type_name = '纯硬件项目' AND t7.project_code is not null then '已结项(硬件项目)' -- 纯硬件项目+设备到货审批未完成
            when b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl is null THEN '发货阶段' -- 外部项目+未上线未验收+未发货
            when b.project_type_name != '纯硬件项目' AND b.is_online = '未上线' AND b.is_final_inspection = '未验收' AND t4.fhsl is not null THEN '上线阶段' -- 外部项目+未上线未验收+已发货
            when b.project_type_name != '纯硬件项目' AND b.is_online = '已上线' AND b.is_final_inspection = '未验收' THEN '验收阶段' -- 外部项目+已上线未验收
            when b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group != '项目结项' THEN '结项阶段' -- 外部项目+已验收+项目未结项
            when b.project_type_name != '纯硬件项目' AND b.is_final_inspection = '已验收' AND b.project_dispaly_state_group = '项目结项' THEN '已结项' -- 外部项目+已验收+项目已结项
       end as project_progress_stage -- 项目进度阶段
FROM 
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name,
         tt.project_type_name,
         tt.project_dispaly_state,
         tt.project_dispaly_state_group,
         tt.project_ft,
         tt.project_priority,
         tt.project_current_version,
         tt.sap_counselor,
         tt.contract_signed_year,
         tt.contract_signed_date,
         tt.expect_online_date,
         tt.online_date,
         tt.is_online,
         tt.sap_entry_date,
         tt.online_times,
         tt.no_online_times,
         tt.expect_final_inspection_date, 
         tt.final_inspection_date,
         tt.is_final_inspection,
         tt.final_inspection_times,
         tt.no_final_inspection_times,
         tt.project_product_name,
         tt.project_area_place,
         tt.online_process_month,
         tt.final_inspection_process_month,
         tt.project_handover_end_time,
         tt.project_handover_start_time,
         tt.pre_project_approval_time
  FROM 
  (
    SELECT b.project_code as true_project_code, -- 项目编码
           b.project_sale_code as true_project_sale_code, -- 售前编码
           b2.project_code,
           b2.project_sale_code,
           b.project_name, -- 项目名称
           case when b.project_type_id = 0 then '外部项目'
                when b.project_type_id = 1 then '公司外部项目'
                when b.project_type_id = 4 then '售前项目'
                when b.project_type_id = 7 then '硬件部项目'
                when b.project_type_id = 8 then '纯硬件项目'
                when b.project_type_id = 9 then '自营仓项目'
                end as project_type_name,
           IF(b.project_dispaly_state = 'UNKNOWN',NULL,b.project_dispaly_state) as project_dispaly_state, -- 项目阶段
           case when b.project_dispaly_state = '0.未开始' OR b.project_dispaly_state = '1.立项/启动阶段' OR b.project_dispaly_state = '2.需求确认/分解' OR b.project_dispaly_state = '3.设计开发/测试' then '需求确认/分解阶段'
                when b.project_dispaly_state = '4.采购/生产' OR b.project_dispaly_state = '5.发货/现场实施' then '发货阶段'
                when b.project_dispaly_state = '6.上线/初验/用户培训' then '上线实施阶段'
                when b.project_dispaly_state = '7.终验' then '验收阶段'
                when b.project_dispaly_state like '8.移交运维/转售后' then '售后移交阶段'
                when b.project_dispaly_state = '9.项目结项' then '项目结项'
                when b.project_dispaly_state = '10.项目暂停' then '项目暂停'
                when b.project_dispaly_state = '11.项目取消' then '项目取消'
                else NULL end as project_dispaly_state_group, -- 项目阶段组
           IF(nvl(b.project_attr_ft,'')='',NULL,b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
           b.project_priority, -- 项目评级
           IF(b.project_current_version = 'UNKNOWN',NULL,b.project_current_version) as project_current_version, -- 版本号
           b.sap_counselor, -- 顾问
           date_format(b.contract_signed_date,'yyyy') as contract_signed_year, -- 合同日期年份
           b.contract_signed_date, -- 合同日期
           b.expect_online_date, -- 预计上线时间
           b.online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
           IF(b.online_process_approval_time is null,'未上线','已上线') as is_online, -- 是否上线
           b.sap_entry_date, -- 实施入场时间
           datediff(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
           IF(b.sap_entry_date is not null AND b.online_date is null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.sap_entry_date),NULL) as no_online_times, -- 持续未上线天数
           b.expect_final_inspection_date, -- 预计终验时间
           b.final_inspection_date, -- 实际终验时间 => <终验报告里程碑>终验上线时间
           IF(b.final_inspection_process_approval_time is null,'未验收','已验收') as is_final_inspection, -- 是否终验
           datediff(b.final_inspection_date,b.online_date) as final_inspection_times, -- 终验时长
           IF(b.final_inspection_date is null AND b.online_date is not null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.online_date),NULL) as no_final_inspection_times, -- 持续未验收天数
           IF(b.project_product_name = 'UNKNOWN',NULL,b.project_product_name) as project_product_name, -- 产品线
           IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place) as project_area_place, -- 区域-PM
           date_format(b.online_process_approval_time,'yyyy-MM') as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
           date_format(b.final_inspection_process_approval_time,'yyyy-MM') as final_inspection_process_month, -- 终验单审批月份 => <终验报告里程碑>完成时间
           h.end_time as project_handover_end_time, -- 交接审批完成时间
           h.start_time as project_handover_start_time, -- 交接审批开始时间
           b.pre_project_approval_time, -- 前置申请完成时间
           row_number()over(PARTITION by b2.project_sale_code order by b2.project_code,h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_share_project_base_info_df b
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b2
    ON (b.project_code = b2.project_code or b.project_sale_code = b2.project_sale_code) AND b.d =b2.d 
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
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE b.d = DATE_ADD(CURRENT_DATE(), -1)   
      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
      AND b.project_type_id IN (0,1,4,7,8,9) -- 只保留外部项目/公司外部项目/售前项目/硬件部项目/纯硬件项目/自营仓项目
      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- 只保留不是商机或者是商机也是前置的项目
  )tt
  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
)b
-- 项目基本信息
LEFT JOIN 
(
  SELECT p.project_code,
         p.pm_name,
         p.sales_area_director,
         p.sales_person,
         p.pre_sales_consultant
  FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
  WHERE (p.project_code LIKE 'FH-%' OR p.project_code LIKE 'A%' OR p.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
    AND p.project_type IN ('外部项目','外部产品项目','售前项目','硬件部项目','纯硬件项目','自营仓项目')
) t1
ON b.project_code = t1.project_code
-- 历史项目基本信息
LEFT JOIN 
(
  SELECT f.project_code,
         f.contract_sign_date,
         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
) t2
ON b.project_code = t2.project_code
-- 物料采购数量
LEFT JOIN
(
  SELECT tt.true_project_code as project_code,
         SUM(tt.cgsl) as cgsl
  FROM
  (
    SELECT b.project_code as true_project_code,
           b.project_sale_code,
           tmp.project_code,
           h.start_time,
           sum(if(tmp.Number1 is null,0,tmp.Number1)) as cgsl,
           row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
    FROM
    (
      --采购申请
      SELECT a.project_code, -- 项目编码
             b.string22, -- 物料编码
             b.string23, -- 物料名称
             b.string24, -- 规格型号
             b.string26, -- 单位
             b.Number1 -- 采购数量
      FROM ${dwd_dbname}.dwd_bpm_materials_purchase_request_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 is not null
      WHERE a.end_time is not null AND a.subscribe_type != '材料采购申请'--流程已结束(审批完成，正常结束)--20220214 采购申请数量去除材料采购申请BY朱文
   
      UNION ALL 
      --采购申请变更
      SELECT a.project_code,
             b.string22,
             null,
             null,
             null,
             b.Number3
      FROM ${dwd_dbname}.dwd_bpm_purchase_request_change_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 is not null
      WHERE end_time is not null --流程已结束(审批完成，正常结束)
    )tmp
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
    ON b.d = DATE_ADD(CURRENT_DATE(), -1) and (b.project_code = tmp.project_code or b.project_sale_code = tmp.project_code)
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
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE tmp.string22 not like 'R1S9%' 
      AND tmp.string22 not like 'R2S9%'
      AND tmp.string22 not like 'R3S9%'
      AND tmp.string22 not like 'R4S9%'
      AND (tmp.string22 not like 'R5S9%' or tmp.string22 in('R5S90518','R5S90528')) --R5S90518:第三方外包
      AND (tmp.string22 not like 'R6S9%' or tmp.string22 in('R6S90077','R6S90078','R6S90080','R6S90058')) --国际物流费用
      AND tmp.string22 not like 'R7S9%'
      AND tmp.string22 not like 'R8S9%'
      AND tmp.string22 not like 'R9S9%'
    GROUP BY b.project_code,b.project_sale_code,tmp.project_code,h.start_time
    HAVING sum(if(tmp.Number1 is null,0,tmp.Number1)) > 0
  )tt
  WHERE tt.rn = 1
  GROUP BY tt.true_project_code
)t3
ON b.project_code = t3.project_code
-- 物料已发货数量
LEFT JOIN
(
  SELECT tt.true_project_code,
         SUM(tt.fhsl) as fhsl
  FROM 
  (
    SELECT a.project_code as string21,
           s.project_code as true_project_code,
           s.project_sale_code,
           h.start_time,
           sum(if(b.Number1 is null,0,b.Number1)) fhsl,
           row_number()over(PARTITION by a.project_code order by h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_project_delivery_approval_info_ful a
    LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b  
    ON a.flow_id = b.FlowID AND b.string14 is not null
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = a.project_code or s.project_sale_code = a.project_code)
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
    WHERE a.approve_status != '50' --进行自动终止
    GROUP BY a.project_code,h.start_time,s.project_code,s.project_sale_code
    )tt
  WHERE tt.rn = 1
  GROUP BY tt.true_project_code
)t4
ON b.project_code = t4.true_project_code
-- 纯硬件项目 设备到货确认里程碑审批流
LEFT JOIN
(
  SELECT tmp.project_code,
         tmp.equitment_arrival_date,
         tmp.end_time_month
  FROM 
  (
    SELECT e.project_code,
           date(e.equitment_arrival_date) as equitment_arrival_date, -- 设备到货签订日期
           date_format(e.end_time,'yyyy-MM') as end_time_month, -- 流程审批完成日期
           row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
    WHERE e.approve_status = 30 
  )tmp
  WHERE tmp.rn = 1
)t7
ON b.project_code = t7.project_code
WHERE t2.project_code is null -- 只保留新项目
),
pe_detail as 
(
    SELECT a.true_project_code,
           a.month_scope,
           a.emp_name
    FROM
    (
      SELECT tmp.*
      FROM 
      (
        SELECT tt.project_code,
               tt.emp_name,
               date_format(tt.log_date,'yyyy-MM') as month_scope,
               s.project_code as true_project_code,
               s.project_sale_code,
               row_number()over(PARTITION by tt.emp_name,tt.project_code,date_format(tt.log_date,'yyyy-MM') order by h.start_time desc)rn
        FROM 
        (
          SELECT tud.org_name_2,
                 tud.org_name_3,
                 tud.emp_name, -- 人员名称
                 tud.emp_position,
                 tud.is_job, -- 是否在职
                 tud.days, -- 日期
                 IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- 日期类型
                 IF(t1.log_date is not null or t12.leave_type is not null,'已打卡','未打卡') as ischeck, -- 是否打卡
                 t1.work_status, -- 出勤状态
                 t1.job_content, -- 工作内容
                 t1.log_date, -- 日志日期
                 t1.project_code, -- 项目编码
                 t1.project_name, -- 项目名称
                 t1.project_manage, -- 项目经理
                 t1.business_id, -- 审批编号
                 t1.create_time, -- 创建时间
                 t1.originator_user_name, -- 发起人
                 t1.approval_status, -- 审批状态
                 CASE when t1.log_date is not null and t1.work_status IN ('出差/On business trip','远程支持/Remote support') and i.project_code is null then '编码不存在' end as unusual_res -- 异常原因
          FROM 
          (
            SELECT tu.org_name_2,
                   tu.org_name_3,
                   tu.emp_id,
                   tu.emp_name,
                   tu.emp_position,
                   tu.is_job,
                   tu.hired_date,
                   tu.quit_date,
                   td.days,
                   CASE when td.day_type = 0 then '工作日'
                        when td.day_type = 1 then '周末'
                        when td.day_type = 2 then '节假日'
                        when td.day_type = 3 then '调休' end as day_type   
            FROM
            (
              SELECT tmp.org_name_2,
                     tmp.org_name_3,
                     tmp.emp_id,
                     tmp.emp_name,
                     tmp.emp_position,
                     tmp.is_job,
                     tmp.hired_date,
                     tmp.quit_date
              FROM
              (
                SELECT DISTINCT split(tg.org_path_name,'/')[1] as org_name_2,
                                split(tg.org_path_name,'/')[2] as org_name_3,
                                te.emp_id,
                                te.emp_name,
                                te.emp_position,
                                te.prg_path_name,
                                te.is_job,
                                date(te.hired_date) as hired_date,
                                date(te.quit_date) as quit_date,
                                row_number()over(PARTITION by te.emp_id order by split(tg.org_path_name,'/')[1] asc,split(tg.org_path_name,'/')[2] asc)rn
                FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
                LEFT JOIN 
                (
                  SELECT DISTINCT m.emp_id,
                                  m.emp_name,
                                  m.org_id,
                                  m.org_role_type,
                                  m.is_need_fill_manhour,
                                  m.org_start_date,
                                  m.org_end_date,
                                  m.is_job
                  FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
                  WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
                )tmp
                ON te.emp_id = tmp.emp_id
                LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
                ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
                WHERE 1 = 1
                  AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司' AND te.is_active = 1
                  -- 筛选PE全部人员
                  AND ((split(tg.org_path_name,'/')[1] IN ('营销中心','箱式FT') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('项目部') AND split(tg.org_path_name,'/')[2] IN ('箱式交付组','项目交付组') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('欧洲分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('海外交付工程师','PE','实习生','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('北美分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('东南亚及中国台湾区域') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('韩国子公司') AND te.emp_position IN ('项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('日本分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付组') AND te.emp_position IN ('维保技术员','项目工程师')))
              )tmp
              WHERE tmp.rn =1
            )tu  
            LEFT JOIN
            (
              SELECT DISTINCT days,
                              day_type
              FROM ${dim_dbname}.dim_day_date
              WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
            )td
            ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,DATE_ADD(CURRENT_DATE(), -1),tu.quit_date)
          )tud
          LEFT JOIN 
          (
            select *
            FROM ${dwd_dbname}.dwd_dtk_process_pe_log_info_df p
            WHERE d = DATE_ADD(CURRENT_DATE(), -1) AND p.approval_status != 'TERMINATED' AND p.create_time >= '2021-07-01 00:00:00'-- 审批状态剔除终止，起始时间从2021-07-01开始
          )t1
          ON t1.originator_user_id = tud.emp_id AND tud.days = t1.log_date 
          LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df i
          ON i.d = DATE_ADD(CURRENT_DATE(), -1) AND (i.project_code = t1.project_code or i.project_sale_code = t1.project_code)
          LEFT JOIN 
          (
            SELECT tud.days as stat_date,
                   l.originator_user_id,
		           te.emp_id,
		           CASE when (tud.days > l.start_date and tud.days < l.end_date)
                          or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then '全天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
                   end as leave_type
            FROM 
            (
              SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
                     l.originator_user_id,
                     IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
                     IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
                     IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
                     IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
                     IF(l.leave_type != '哺乳假','正常请假','哺乳假') as leave_type,
                     row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.create_time desc)rn
              FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
              LEFT JOIN ${dwd_dbname}.dwd_dtk_process_leave_info_df l1
              ON l.originator_user_id = l1.originator_user_id AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.process_result = l1.process_result AND l.process_status = l1.process_status AND l.is_valid =l1.is_valid
              WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
            )l
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
            ON l.originator_user_id = te.emp_id	
            LEFT JOIN ${dim_dbname}.dim_day_date tud
            on l.start_date <= tud.days and l.end_date >= tud.days
            WHERE l.rn = 1
            AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
          ) t12 
          ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
          where t1.project_code is not null 
        )tt
        LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
        ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tt.project_code or s.project_sale_code = tt.project_code) AND (s.project_code LIKE 'FH-%' OR s.project_code LIKE 'A%' OR s.project_code LIKE 'C%') AND s.project_type_id IN (0,1,4,7,8,9) AND (s.is_business_project = 0 OR (s.is_business_project = 1 AND s.is_pre_project = 1))
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
      WHERE tmp.rn = 1 AND tmp.true_project_code is not null
    )a
)

INSERT overwrite table ${ads_dbname}.ads_project_member_effcive
SELECT '' as id,
       t1.month_scope, -- 统计月份
       date(concat(t1.month_scope,'-01')) as month_scope_first_day, -- 统计月份首天
       t1.project_area, -- 大区
       t1.project_priority,
       t1.project_ft,
       t1.project_stage,
       t1.no_online_num_total, -- 累计未上线项目数量
       t1.online_num_month, -- 当月上线项目数量
       t1.no_final_inspection_num_total, -- 累计未验收项目数量
       t1.final_inspection_num_month, -- 当月验收项目数量
       IF(t2.suspend_num_month is null,0,t2.suspend_num_month) as suspend_num_month, -- 当月暂停项目数量
       IF(t3.pe_num is null,0,t3.pe_num) as pe_num, -- pe人员数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
-- 累计未上线/未验收项目总数 + 当月上线/验收项目数量
FROM
(
  SELECT a.project_code_class, -- 项目编码分类
         a.month_scope, -- 统计月份
         a.project_area, -- 大区
         a.project_priority,
         a.project_ft,
         a.project_stage,
         SUM(b.handover_num) as handover_num_total, -- 累计交接项目数量
         AVG(a.online_num) as online_num_month, -- 当月上线项目数量
         SUM(b.online_num) as online_num_total, -- 累计上线项目数量
         SUM(b.no_online_num) as no_online_num_total, -- 累计未上线项目数量 
         AVG(a.final_inspection_num) as final_inspection_num_month, -- 当月验收项目数量
         SUM(b.final_inspection_num) as final_inspection_num_total, -- 累计验收项目数量
         SUM(b.no_final_inspection_num) as no_final_inspection_num_total -- 累计未验收项目数量 
  FROM 
  (
    SELECT total.project_code_class,
           total.month_scope,
           total.project_area,
           total.project_priority,
           total.project_ft,
           total.project_stage,
           total.handover_num,
           IF(online.online_num is null,0,online.online_num) as online_num,
           total.handover_num - IF(online.online_num is null,0,online.online_num) as no_online_num,
           IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
           total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as no_final_inspection_num
    FROM 
    (
      SELECT i.project_code_class,
             td.month_scope,
             i.project_area,
             i.project_priority,
             i.project_ft,
             i.project_stage,
             SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num
      FROM 
      (
        SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
        FROM ${dim_dbname}.dim_day_date
        WHERE 1 = 1
          AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
      ) td
      LEFT JOIN
      (
	    SELECT pcc.project_code_class,
               pa.project_area,
               pp.project_priority,
               pf.project_ft,
               ps.project_stage
        FROM 
        (
          SELECT split('A',',') a,
                 split('华北,总部,华南,海外,华东,西南,华中,未知',',') b,
                 split('0级,1级,2级,3级,4级,5级',',') c,
                 split('未知,智能搬运FT,箱式FT',',') d,
                 split('未发货未上线,已发货未上线,已上线未验收,已验收未结项,已结项',',') e
        ) tmp
        lateral view explode(a) pcc as project_code_class 
        lateral view explode(b) pa as project_area
        lateral view explode(c) pp as project_priority
        lateral view explode(d) pf as project_ft
        lateral view explode(e) ps as project_stage
      ) i
      LEFT JOIN 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      ) tmp1
      ON i.project_code_class = tmp1.project_code_class AND td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM') AND i.project_area = tmp1.project_area AND i.project_priority = tmp1.project_priority AND i.project_ft = tmp1.project_ft AND i.project_stage = tmp1.project_stage
      GROUP BY i.project_code_class,td.month_scope,i.project_area,i.project_priority,i.project_ft,i.project_stage
    )total
    LEFT JOIN
    (
      SELECT tmp2.project_code_class,
             tmp2.online_process_month,
             tmp2.project_area,
             tmp2.project_priority,
             tmp2.project_ft,
             tmp2.project_stage,
             SUM(case when tmp2.is_online = '已上线' then 1 else 0 end) as online_num
      FROM 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      )tmp2
      WHERE tmp2.is_online = '已上线'
      GROUP BY tmp2.project_code_class,tmp2.online_process_month,tmp2.project_area,tmp2.project_priority,tmp2.project_ft,tmp2.project_stage
    )online
    ON total.project_code_class = online.project_code_class AND total.month_scope = online.online_process_month AND total.project_area = online.project_area AND total.project_priority = online.project_priority AND total.project_ft = online.project_ft AND total.project_stage = online.project_stage
    LEFT JOIN
    (
      SELECT tmp3.project_code_class,
             tmp3.final_inspection_process_month,
             tmp3.project_area,
             tmp3.project_priority,
             tmp3.project_ft,
             tmp3.project_stage,
             SUM(case when tmp3.is_final_inspection = '已验收' then 1 else 0 end) as final_inspection_num
      FROM 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      )tmp3
      WHERE tmp3.is_final_inspection = '已验收'
      GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month,tmp3.project_area,tmp3.project_priority,tmp3.project_ft,tmp3.project_stage
    )final_inspection
    ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month AND total.project_area = final_inspection.project_area AND total.project_priority = final_inspection.project_priority AND total.project_ft = final_inspection.project_ft AND total.project_stage = final_inspection.project_stage
  )a 
  LEFT JOIN 
  (
    SELECT total.project_code_class,
           total.month_scope,
           total.project_area,
           total.project_priority,
           total.project_ft,
           total.project_stage,
           total.handover_num,
           IF(online.online_num is null,0,online.online_num) as online_num,
           total.handover_num - IF(online.online_num is null,0,online.online_num) as no_online_num,
           IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
           total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as no_final_inspection_num
    FROM 
    (
      SELECT i.project_code_class,
             td.month_scope,
             i.project_area,
             i.project_priority,
             i.project_ft,
             i.project_stage,
             SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num
      FROM 
      (
        SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
        FROM ${dim_dbname}.dim_day_date
        WHERE 1 = 1
          AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
      ) td
      LEFT JOIN
      (
	    SELECT pcc.project_code_class,
               pa.project_area,
               pp.project_priority,
               pf.project_ft,
               ps.project_stage
        FROM 
        (
          SELECT split('A',',') a,
                 split('华北,总部,华南,海外,华东,西南,华中,未知',',') b,
                 split('0级,1级,2级,3级,4级,5级',',') c,
                 split('未知,智能搬运FT,箱式FT',',') d,
                 split('未发货未上线,已发货未上线,已上线未验收,已验收未结项,已结项',',') e
        ) tmp
        lateral view explode(a) pcc as project_code_class 
        lateral view explode(b) pa as project_area
        lateral view explode(c) pp as project_priority
        lateral view explode(d) pf as project_ft
        lateral view explode(e) ps as project_stage
      ) i
      LEFT JOIN 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      ) tmp1
      ON i.project_code_class = tmp1.project_code_class AND td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM') AND i.project_area = tmp1.project_area AND i.project_priority = tmp1.project_priority AND i.project_ft = tmp1.project_ft AND i.project_stage = tmp1.project_stage
      GROUP BY i.project_code_class,td.month_scope,i.project_area,i.project_priority,i.project_ft,i.project_stage
    )total
    LEFT JOIN
    (
      SELECT tmp2.project_code_class,
             tmp2.online_process_month,
             tmp2.project_area,
             tmp2.project_priority,
             tmp2.project_ft,
             tmp2.project_stage,
             SUM(case when tmp2.is_online = '已上线' then 1 else 0 end) as online_num
      FROM 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      )tmp2
      WHERE tmp2.is_online = '已上线'
      GROUP BY tmp2.project_code_class,tmp2.online_process_month,tmp2.project_area,tmp2.project_priority,tmp2.project_ft,tmp2.project_stage
    )online
    ON total.project_code_class = online.project_code_class AND total.month_scope = online.online_process_month AND total.project_area = online.project_area AND total.project_priority = online.project_priority AND total.project_ft = online.project_ft AND total.project_stage = online.project_stage
    LEFT JOIN
    (
      SELECT tmp3.project_code_class,
             tmp3.final_inspection_process_month,
             tmp3.project_area,
             tmp3.project_priority,
             tmp3.project_ft,
             tmp3.project_stage,
             SUM(case when tmp3.is_final_inspection = '已验收' then 1 else 0 end) as final_inspection_num
      FROM 
      (
	    SELECT *
	    FROM project_view_detail d
	    WHERE d.project_code_class = 'A'
      )tmp3
      WHERE tmp3.is_final_inspection = '已验收'
      GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month,tmp3.project_area,tmp3.project_priority,tmp3.project_ft,tmp3.project_stage
    )final_inspection
    ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month AND total.project_area = final_inspection.project_area AND total.project_priority = final_inspection.project_priority AND total.project_ft = final_inspection.project_ft AND total.project_stage = final_inspection.project_stage
  )b
  ON a.project_code_class = b.project_code_class AND a.project_area = b.project_area AND a.project_area = b.project_area AND a.project_priority = b.project_priority AND a.project_ft = b.project_ft AND a.project_stage = b.project_stage AND a.month_scope >= b.month_scope
  GROUP BY a.project_code_class,a.month_scope,a.project_area,a.project_priority,a.project_ft,a.project_stage
)t1
-- 当月暂停项目数量
LEFT JOIN 
(
  SELECT b.project_code_class,
         b.project_area,
         b.project_priority,
         b.project_ft,
         b.project_stage,
         date_format(a.end_time,'yyyy-MM') as month_scope,
         COUNT(DISTINCT b.project_code) as suspend_num_month
  FROM ${dwd_dbname}.dwd_bpm_project_suspend_apply_info_ful a
  LEFT JOIN 
  (
    SELECT *
    FROM project_view_detail d
	WHERE d.project_code_class = 'A'
  )b
  ON a.project_code = b.project_code
  WHERE a.approval_staus = 30 -- 审批完成
  GROUP BY b.project_code_class,b.project_area,date_format(a.end_time,'yyyy-MM'),b.project_priority,b.project_ft,b.project_stage
)t2
ON t1.project_code_class = t2.project_code_class AND t1.project_area = t2.project_area AND t1.month_scope = t2.month_scope AND t1.project_priority = t2.project_priority AND t1.project_ft = t2.project_ft AND t1.project_stage = t2.project_stage
-- PE人员数量
LEFT JOIN 
(
  SELECT d.project_code_class,
         d.project_area,
         d.project_priority,
         d.project_ft,
         d.project_stage,
         tmp1.month_scope,
         COUNT(DISTINCT tmp1.emp_name) as pe_num 
  FROM
  (
    SELECT *
    FROM pe_detail
  )tmp1
  LEFT JOIN project_view_detail d
  ON tmp1.true_project_code = d.project_code
  WHERE d.project_code is not null
  GROUP BY d.project_code_class,d.project_area,d.project_priority,d.project_ft,d.project_stage,tmp1.month_scope
)t3
ON t1.project_code_class = t3.project_code_class AND t1.project_area = t3.project_area AND t1.month_scope = t3.month_scope AND t1.project_priority = t3.project_priority AND t1.project_ft = t3.project_ft AND t1.project_stage = t3.project_stage;