--ads_project_general_view_detail    --项目概览大表

INSERT overwrite table ${ads_dbname}.ads_project_general_view_detail
SELECT '' as id, -- 主键
       case when b.project_code like 'A%' THEN 'A'
            when b.project_code like 'C%' THEN 'C'
            when b.project_code like 'FH-%' THEN 'FH'
            else '未知' end as project_code_class, -- 项目编码种类
       b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name, -- 项目名称
       CONCAT(b.project_code,'-',b.project_name) as project_info, -- 项目编码及名称
       IF(b.project_product_name is null,'未知',b.project_product_name) as project_product_name, -- 产品线
       IF(t2.project_code is not null,'历史项目','新项目') as project_type, -- 项目类型
       b.project_dispaly_state, -- 项目阶段
       b.project_dispaly_state_group, -- 项目阶段组
       IF(b.project_ft is null,'未知',b.project_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
       b.project_priority, -- 项目评级
       IF(b.project_current_version is null,'未知',b.project_current_version) as project_current_version , -- 版本号
       t1.sales_area_director, -- owner
       IF(b.project_area_place is null,'未知',b.project_area_place) as project_area, -- 区域-PM
       t1.pm_name, -- PM
       IF(t1.spm_name is null,t9.spm_name,t1.spm_name) as spm_name, -- spm
       b.sap_counselor, -- 顾问
       t1.sales_person, -- 销售
       t1.pre_sales_consultant, -- 售前顾问
       t6.contract_amount as amount, -- 合同金额（线下表）
       IF(b.contract_signed_year is null,t2.contract_sign_year,b.contract_signed_year) as contract_signed_year, -- 合同日期
       IF(b.contract_signed_date is null,t2.contract_sign_date,b.contract_signed_date) as contract_signed_date, -- 合同日期
       IF(t4.fhsl is null and t3.cgsl is null,null,IF(CAST(t4.fhsl / t3.cgsl as decimal(10,4)) is null,0,CAST(t4.fhsl / t3.cgsl as decimal(10,4)))) as deliver_goods_achieving_rate, -- 发货完成率
       b.pre_project_approval_time, -- 前置申请完成时间
       b.project_handover_end_time, -- 交接审批完成时间
       t8.plan_golive_date as expect_online_date, -- 预计上线时间
       IF(b.project_type_name = '纯硬件项目',t7.equitment_arrival_date,b.online_date) as online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
       IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month) as online_process_month, -- 上线单审批月份 => <上线报告里程碑>完成时间
       date(CONCAT(IF(b.project_type_name = '纯硬件项目',t7.end_time_month,b.online_process_month),'-01')) as online_process_month_begin, -- 上线单审批月初 => <上线报告里程碑>完成时间
       IF(b.project_type_name = '纯硬件项目' and t7.project_code is not null,'已上线',IF(t2.project_code is not null,'已上线',b.is_online)) as is_online, -- 是否上线
       b.sap_entry_date, -- 实施入场时间
       b.online_times, -- 上线时长
       IF(t2.project_code is not null,NULL,b.no_online_times) as no_online_times, -- 持续未上线天数
       t8.plan_acceptance_date as expect_final_inspection_date, -- 预计终验时间
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
       end as project_progress_stage, -- 项目进度阶段
       NULL as project_gm, -- 项目毛利率
       NULL as complain_num, -- 客户投诉次数
       IF(t5.zeroweek_work_num is null,0,t5.zeroweek_work_num) as zeroweek_work_num, -- 当前周
       IF(t5.oneweek_work_num is null,0,t5.oneweek_work_num) as oneweek_work_num, -- 当前周+前一周 
       IF(t5.twoweek_work_num is null,0,t5.twoweek_work_num) as twoweek_work_num, -- 当前周+前二周 
       IF(t5.threeweek_work_num is null,0,t5.threeweek_work_num) as threeweek_work_num, -- 当前周+前三周 
       IF(t5.fourweek_work_num is null,0,t5.fourweek_work_num) as fourweek_work_num, -- 当前周+前四周 
       IF(b.pe_members is null,'无',b.pe_members) as pe_members, -- 现场PE
       IF(b.charger_num is null,0,b.charger_num) as charger_num, -- 充电桩数量
       IF(b.station_num is null,0,b.station_num) as station_num, -- 工作站数量
       IF(b.agv_num is null,0,b.agv_num) as agv_num, -- 机器人数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
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
         tt.online_date,
         tt.is_online,
         tt.sap_entry_date,
         tt.online_times,
         tt.no_online_times,
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
         tt.pre_project_approval_time,
         tt.pe_members,
         tt.charger_num,
         tt.station_num,
         tt.agv_num
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
           b.online_date, -- 实际上线时间 => <上线报告里程碑>上线时间
           IF(b.online_process_approval_time is null,'未上线','已上线') as is_online, -- 是否上线
           b.sap_entry_date, -- 实施入场时间
           datediff(b.online_date,b.sap_entry_date) as online_times, -- 上线时长
           IF(b.sap_entry_date is not null AND b.online_date is null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.sap_entry_date),NULL) as no_online_times, -- 持续未上线天数
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
           b.pe_members, -- 现场PE
           b.charger_num, -- 充电桩数量
           b.station_num, -- 工作站数量
           b.agv_num, -- 小车数量
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
         p.spm_name,
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
    ON b.d = DATE_ADD(CURRENT_DATE(), -1) AND (b.project_code = tmp.project_code or b.project_sale_code = tmp.project_code) AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') AND b.project_type_id IN (0,1,4,7,8,9) AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1))
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
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = a.project_code or s.project_sale_code = a.project_code) AND (s.project_code LIKE 'FH-%' OR s.project_code LIKE 'A%' OR s.project_code LIKE 'C%') AND s.project_type_id IN (0,1,4,7,8,9) AND (s.is_business_project = 0 OR (s.is_business_project = 1 AND s.is_pre_project = 1))
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
LEFT JOIN 
(
  SELECT tmp.true_project_code,
         SUM(tmp.zeroweek_work_num) as zeroweek_work_num,
         SUM(tmp.oneweek_work_num) as oneweek_work_num,
         SUM(tmp.twoweek_work_num) as twoweek_work_num,
         SUM(tmp.threeweek_work_num) as threeweek_work_num,
         SUM(tmp.fourweek_work_num) as fourweek_work_num
  FROM 
  (
    SELECT tt.*,
           IF(s.project_code is null,tt.project_code,s.project_code) as true_project_code,
           s.project_sale_code,
           h.project_code as external_project_code,
           h.pre_sale_code,
           row_number()over(PARTITION by s.project_sale_code order by h.start_time desc)rn
    FROM
    (
      SELECT w.project_code,
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) THEN 1 ELSE 0 END) as zeroweek_work_num, -- 当前周
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 1 THEN 1 ELSE 0 END) as oneweek_work_num, -- 当前周+前一周 
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 2 THEN 1 ELSE 0 END) as twoweek_work_num, -- 当前周+前两周
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 3 THEN 1 ELSE 0 END) as threeweek_work_num, -- 当前周+前三周
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 4 THEN 1 ELSE 0 END) as fourweek_work_num -- 当前周+前四周
      FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones工单系统
      WHERE w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '已驳回' and lower(w.project_code) not regexp 'test|tese'    
      GROUP BY w.project_code
    )tt
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tt.project_code or s.project_sale_code = tt.project_code) AND (s.project_code LIKE 'FH-%' OR s.project_code LIKE 'A%' OR s.project_code LIKE 'C%') AND s.project_type_id IN (0,1,4,7,8,9) AND (s.is_business_project = 0 OR (s.is_business_project = 1 AND s.is_pre_project = 1))
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
  )tmp
  WHERE tmp.project_sale_code is NULL OR tmp.rn = 1 OR tmp.external_project_code is not null 
  GROUP BY tmp.true_project_code
)t5
ON b.project_code = t5.true_project_code
-- 项目收入
LEFT JOIN
(
  SELECT a.project_code,
         a.contract_amount
  FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
)t6
ON b.project_code = t6.project_code
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
-- 计划时间节点线下表
LEFT JOIN
(
  SELECT p.project_code,
         p.plan_golive_date, -- 计划上线日期
         p.plan_acceptance_date -- 计划验收日期
  FROM ${dwd_dbname}.dwd_pmo_project_plan_offline_info_df p
  WHERE p.d = DATE_ADD(CURRENT_DATE(), -1)
)t8
ON b.project_code = t8.project_code
-- 补充spm
LEFT JOIN
(
  SELECT DISTINCT kf.string31 as project_code,
                  kf.string35 as spm_name
  FROM ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful kf
  LEFT JOIN ${dwd_dbname}.dwd_bpm_es_flow_info_ful ef 
  ON kf.flowid = ef.flow_id
  WHERE oflowmodelid= '81687' and string31 is not null AND ef.flow_status = '30'
)t9
ON b.project_code = t9.project_code;