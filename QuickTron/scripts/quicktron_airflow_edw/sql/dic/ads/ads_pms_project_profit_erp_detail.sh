#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-01-06 创建
#-- 2 wangyingying 2023-03-01 增加项目信息、物料名称、数量字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    

echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ads_pms_project_profit_erp_detail    --pms项目利润erp明细表

INSERT overwrite table ${ads_dbname}.ads_pms_project_profit_erp_detail
SELECT '' AS id, -- 主键
       b.project_code, -- 项目编码
       b.project_sale_code, -- 前置项目编码
       b.project_name, -- 项目名称
       b.project_info, -- 项目信息
       b.project_area, -- 项目区域
	   b.project_area_group, -- 项目区域组（国内|国外）
	   t.bill_project_code, -- 单据项目编码
	   t.cost_type, -- 费用类型
       t.bill_no, -- 费用单据编码
       t.material_id, -- 物料id
       t.material_number, -- 物料号
       t.material_name, -- 物料名称
       t.real_qty, -- 数量
       t.cost, -- 最终价格
       nvl(t.is_valid,0) AS is_valid, -- 是否有效
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN 
(
  SELECT b.project_code, -- 最终统计项目编码
         tmp.project_code AS bill_project_code, -- 单据项目编码
         tmp.cost_type, -- 费用类型
         tmp.bill_no, -- 费用单据编码
         tmp.material_id, -- 物料id
         tmp.material_number, -- 物料号
         tmp.material_name, -- 物料名称
         tmp.real_qty, -- 数量
         tmp.finance_cost_amount_lc AS cost, -- 最终价格
         tmp.is_valid -- 是否有效
  FROM 
  (
    -- agv费用
    SELECT so.project_code, -- 项目编码
           'agv费用' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           so.real_qty, -- 数量
           IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
    ON m.material_id = so.material_id AND so.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_info_df si 
    ON so.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
      AND m.paez_checkbox  = 1 -- 物料属性为agv
      AND m.document_status = 'C' -- 数据状态：完成
      AND so.project_code IS NOT NULL
    
    UNION ALL 
    
    -- agv费用退货
    SELECT sr.project_code, -- 项目编码
           'agv费用-退货' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           sr.real_qty, -- 数量
           IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
    ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_info_df si
    ON sr.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
      AND m.paez_checkbox  = 1 -- 物料属性为agv
      AND m.document_status = 'C' -- 数据状态：完成
      AND sr.project_code IS NOT NULL
      
    UNION ALL 
    
    -- 货架费用
    SELECT so.project_code, -- 项目编码
           '货架费用' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           so.real_qty, -- 数量
           IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
    ON m.material_id = so.material_id AND so.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_info_df si 
    ON so.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
      AND m.material_number LIKE 'RT04%' -- 货架
      AND so.project_code IS NOT NULL
    
    UNION ALL 
    
    -- 货架费用退货
    SELECT sr.project_code, -- 项目编码
           '货架费用-退货' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           sr.real_qty, -- 数量
           IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
    ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_info_df si
    ON sr.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
      AND m.material_number LIKE 'RT04%' -- 货架
      AND sr.project_code IS NOT NULL
      
    UNION ALL 
    
    -- 充电桩费用
    SELECT so.project_code, -- 项目编码
           '充电桩费用' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           so.real_qty, -- 数量
           IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
    ON m.material_id = so.material_id AND so.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_info_df si 
    ON so.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
      AND m.material_number LIKE 'RT03%' -- 充电桩
      AND so.project_code IS NOT NULL

    UNION ALL 
    
    -- 充电桩费用退货 
    SELECT sr.project_code, -- 项目编码
           '充电桩费用-退货' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           sr.real_qty, -- 数量
           IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty)) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
    ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_info_df si
    ON sr.id = si.id AND si.d = '${pre1_date}'
    LEFT JOIN 
    (
      SELECT b.material_id, -- 物料内码
             SUM(b.end_period_number) AS end_period_number, -- 期末数量
             SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
             nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
      FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
      WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
      GROUP BY b.material_id
    )b
    ON m.material_id = b.material_id
    WHERE m.d = '${pre1_date}'
     AND m.material_number LIKE 'RT03%' -- 充电桩
      AND sr.project_code IS NOT NULL
      
    UNION ALL 
    
    -- 出口包装费
    SELECT po.project_code, -- 项目编码
           '出口包装费' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
    LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
    ON g.id = m.material_group AND m.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE g.materia_number ='P' -- 包装
      AND m.document_status = 'C' -- 数据状态：完成
      AND po.project_code IS NOT NULL
      
    UNION ALL 
    
    -- 出口包装费退货
    SELECT pm.project_code, -- 项目编码
           '出口包装费-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
    LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
    ON g.id = m.material_group AND m.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE g.materia_number ='P' -- 包装
      AND m.document_status = 'C' -- 数据状态：完成
      AND pm.project_code IS NOT NULL  
      
    UNION ALL 
    
    -- 运输费
    SELECT po.project_code, -- 项目编码
           '运输费' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND po.project_code IS NOT NULL  
    
    UNION ALL 
    
    -- 运输费退货
    SELECT pm.project_code, -- 项目编码
           '运输费-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND pm.project_code IS NOT NULL  
      
    UNION ALL 
    
    -- 外包软件费用
    SELECT po.project_code, -- 项目编码
           '外包软件费用' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND po.project_code IS NOT NULL  

    UNION ALL 
    
    -- 外包软件费用退货
    SELECT pm.project_code, -- 项目编码
           '外包软件费用-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND pm.project_code IS NOT NULL   

    UNION ALL 
    
    -- 其他物料费用
    SELECT so.project_code, -- 项目编码
           '其他物料费用' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           so.real_qty, -- 数量
           nvl(so.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
    ON m.material_id = so.material_id AND so.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_info_df si 
    ON so.id = si.id AND si.d = '${pre1_date}'
    WHERE m.d = '${pre1_date}'
      AND m.paez_checkbox != 1 -- 物料属性不为agv 
      AND m.material_number not like 'RT04%' -- 排除货架
      AND m.material_number not like 'RT03%' -- 排除充电桩
      AND (m.material_group not in ('111370','111373') OR m.material_group is null) -- 排除物料分组为P（包装）、S（软件）
      AND m.material_number not like 'R5S%'
      AND m.material_number not like 'R6S%'
      AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
      AND m.document_status = 'C' -- 数据状态已完成
      AND so.project_code IS NOT NULL 
      
    UNION ALL 
    
    -- 其他物料费用退货
    SELECT sr.project_code, -- 项目编码
           '其他物料费用-退货' AS cost_type, -- 费用类型
           si.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           sr.real_qty, -- 数量
           nvl(sr.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
    ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_info_df si
    ON sr.id = si.id AND si.d = '${pre1_date}'
    WHERE m.d = '${pre1_date}'
      AND m.paez_checkbox != 1 -- 物料属性不为agv 
      AND m.material_number not like 'RT04%' -- 排除货架
      AND m.material_number not like 'RT03%' -- 排除充电桩
      AND (m.material_group not in ('111370','111373') OR m.material_group is null) -- 排除物料分组为P（包装）、S（软件）
      AND m.material_number not like 'R5S%'
      AND m.material_number not like 'R6S%'
      AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
      AND m.document_status = 'C' -- 数据状态已完成
      AND sr.project_code IS NOT NULL

    UNION ALL 
    
    -- 外包硬件费用
    SELECT po.project_code, -- 项目编码
           '外包硬件费用' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
    LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
    ON g.id = m.material_group AND m.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010','S99000046K010','S99L04660K010')) OR m.material_number IN ('R5S90518','R5S90527','R5S90041'))
      AND m.document_status = 'C' -- 数据状态：完成
      AND po.project_code IS NOT NULL  

    UNION ALL 
    
    -- 外包硬件费用退货
    SELECT pm.project_code, -- 项目编码
           '外包硬件费用-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           1 AS is_valid -- 是否有效
    FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
    LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
    ON g.id = m.material_group AND m.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010','S99000046K010','S99L04660K010')) OR m.material_number IN ('R5S90518','R5S90527','R5S90041'))
      AND m.document_status = 'C' -- 数据状态：完成
      AND pm.project_code IS NOT NULL    
      
    UNION ALL 
    
    -- 外包劳务-运维费用
    SELECT po.project_code, -- 项目编码
           '外包运维劳务费用' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           IF(pi.bill_date <= '2021-12-31',1,0) AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
      AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
      AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
  	  AND m.material_number not in ('R5S90041') -- 外包硬件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND po.project_code IS NOT NULL   

    UNION ALL 
    
    -- 外包劳务-运维费用退货
    SELECT pm.project_code, -- 项目编码
           '外包运维劳务费用-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           IF(pi.bill_date <= '2021-12-31',1,0) AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
      AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
      AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
      AND m.material_number not in ('R5S90041') -- 外包硬件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND pm.project_code is not NULL  
      
    UNION ALL 
    
    -- 外包劳务-实施费用
    SELECT po.project_code, -- 项目编码
           '外包实施劳务费用' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           po.quantity AS real_qty, -- 数量
           nvl(po.finance_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           IF(pi.bill_date <= '2021-12-31',1,0) AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
    ON m.material_id = po.material_id AND po.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df pi
    ON po.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
      AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
      AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
  	  AND m.material_number not in ('R5S90041') -- 外包硬件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND po.project_code IS NOT NULL   

    UNION ALL 
    
    -- 外包劳务-实施费用退货
    SELECT pm.project_code, -- 项目编码
           '外包实施劳务费用-退货' AS cost_type, -- 费用类型
           pi.bill_no, -- 费用单据编码
           m.material_id, -- 物料id
           m.material_number, -- 物料号
           m.material_name, -- 物料名称
           pm.rm_real_qty AS real_qty, -- 数量
           nvl(pm.finance_cost_amount_lc,0) AS finance_cost_amount_lc, -- 最终价格
           IF(pi.bill_date <= '2021-12-31',1,0) AS is_valid -- 是否有效
    FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
    ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
    LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df pi
    ON pm.id = pi.id AND pi.d = '${pre1_date}'
    WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
      AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
      AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
      AND m.material_number not in ('R5S90041') -- 外包硬件
      AND m.document_status = 'C' -- 数据状态：完成
      AND m.d = '${pre1_date}'
      AND pm.project_code IS NOT NULL  
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
)t
ON b.project_code = t.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"