#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--ads_bpm_znby_ft_project_detail    --智能搬运FT项目信息明细
INSERT overwrite table ${ads_dbname}.ads_bpm_znby_ft_project_detail
SELECT '' as id, -- 主键
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_sale_code, -- 售前编码
       case when b.is_pre_project = 1 then '是'
            when b.is_pre_project = 0 then '否'
       end as is_pre_project, -- <外部项目启动前置申请>是否前置 注：FH没前置、商机 (1:是 0：否)
       b.project_ft, -- <技术方案评审>ft
       b.project_attr_ft, -- <QT项目立项信息中心>归属ft
       case when b.is_business_project = 1 then '是'
            when b.is_business_project = 0 then '否'
       end as is_business_project, -- 是否商机(1:是 0：否 -1：未知)
       b.technical_end_time, -- <技术方案评审>完成时间
       b.contract_signed_date, -- <外部项目交接>合同签订日期
       b.field_conduct_end_time, -- <现场实施完成评审单>完成时间
       b.online_date, -- <上线报告里程碑>完成时间
       b.final_inspection_date, -- <终验报告里程碑>完成时间
       IF(b.is_pre_project = 1,IF(b.technical_end_time is null,'待技术评审',IF(b.contract_signed_date is null,'技术评审','已签合同')),b.project_operation_state) as project_state, -- 项目状态
       b.expect_online_date, -- 预计上线
       b.expect_final_inspection_date,-- 预计终验
       IF(b.project_code like 'FH%',t2.amount,t1.amount) as contract_amount, -- 合同金额
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_share_project_base_info_df b
--合同评审项目
LEFT JOIN 
(
  SELECT tmp.final_proejct_code,
         SUM(tmp.amount) as amount
  FROM
  (
    SELECT if(i.pre_sale_code is null or length(i.pre_sale_code) = 0 or i.pre_sale_code = h.pre_sale_code,h.project_code,i.pre_sale_code) as final_proejct_code,
           i.contract_code,
           i.amount + if(c.amount is null,0,c.amount)  as amount
    FROM 
    (
      SELECT i.pre_sale_code,
             i.contract_code,
             i.start_time,
             cast(replace(nvl(i.levied_total,0),',','') as decimal(18,2)) as amount,
             row_number()over(PARTITION by i.contract_code order by i.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_contract_review_info_ful i
      WHERE i.end_time is not null AND i.approval_staus = 30 AND length(i.contract_code) != 0 AND i.contract_code is not null
    )i
    LEFT JOIN 
    (
      SELECT c.principal_contract_code,
             sum(cast(nvl(c.rmb_includ_tax_change_amount,0) as decimal(10,2))) as amount
      FROM ${dwd_dbname}.dwd_bpm_supplementary_contract_review_info_ful c
      WHERE c.end_time is not null AND c.approval_staus = 30 
      GROUP BY c.principal_contract_code
    )c
    ON i.contract_code = c.principal_contract_code 
    LEFT JOIN
    (
      SELECT h.contract_code,
             h.pre_sale_code,
             h.project_code,
             row_number()over(PARTITION by h.contract_code order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.end_time is not null AND h.contract_type = '主合同' AND h.approval_staus = 30 AND length(h.contract_code) != 0
    )h
    ON i.contract_code = h.contract_code 
    WHERE i.rn = 1 and h.rn = 1 
  )tmp 
  GROUP BY tmp.final_proejct_code
) t1
ON b.project_code = t1.final_proejct_code
--未合同评审项目
LEFT JOIN 
(
  SELECT a.pre_sale_code,a.amount
  FROM ${dwd_dbname}.dwd_bpm_external_project_pre_apply_info_ful a
  WHERE a.current_status IN ('已有PO单','已有合同') AND a.end_time is not null AND a.approve_status = 30
)t2
ON b.project_code = t2.pre_sale_code
WHERE b.d = DATE_ADD(CURRENT_DATE(), -1) 
  AND (b.project_ft LIKE '%智能搬运FT%' OR b.project_attr_ft = '智能搬运FT') -- <技术方案评审>ft&<QT项目立项信息中心>归属ft为智能搬运FT的
  AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- 只保留FH/A/C开头的项目
  AND b.project_type_id IN (0,1,4,7,8,9)
  AND b.is_fh_pre_transform = 0 -- 去掉已转正式的fh前置项目
ORDER BY b.project_code DESC;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"