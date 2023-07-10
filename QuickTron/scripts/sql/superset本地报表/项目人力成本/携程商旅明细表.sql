--携程商旅明细表 ads_project_ctrip_travel_detail

with project_view_detail as 
(
SELECT b.project_code, -- 项目编码
       b.project_sale_code, -- 售前编码
       b.project_name -- 项目名称
FROM 
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name
  FROM 
  (
    SELECT b.project_code as true_project_code, -- 项目编码
           b.project_sale_code as true_project_sale_code, -- 售前编码
           b.project_name, -- 项目名称
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
-- 历史项目基本信息
LEFT JOIN 
(
  SELECT f.project_code,
         f.contract_sign_date,
         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
) t2
ON b.project_code = t2.project_code
WHERE t2.project_code is null -- 只保留新项目
)

INSERT overwrite table ${ads_dbname}.ads_project_ctrip_travel_detail
SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       pvd.project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       c.dept_name, -- 一级部门
       c.team_org_name, -- 二级部门
       c.passenger_name as travel_user, -- 商旅人员
       '用车' as travel_type, -- 商旅类型
       date_format(c.start_time, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(c.end_time, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       '国内' as travel_scope, -- 商旅范围
       CONCAT(c.start_address,'-',c.end_address) as travel_path, -- 商旅路径
       NULL as travel_detail, -- 商旅详细信息
       c.real_amount_haspost as amount, -- 费用金额
       IF(c.real_amount_haspost >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_car_account_check_info_di c
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON c.d = td.days
LEFT JOIN project_view_detail pvd
ON c.project_code = pvd.project_code OR c.project_code = pvd.project_sale_code
WHERE (td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)) AND pvd.project_code is not null

UNION ALL 

SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       pvd.project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       f.dept_name, -- 一级部门
       f.team_org_name, -- 二级部门
       f.passenger_name as travel_user, -- 商旅人员
       '飞机' as travel_type, -- 商旅类型
       date_format(f.takeoff_time, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(f.arrival_time, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       f.flight_class as travel_scope, -- 商旅范围
       CONCAT(f.departure_city,'-',f.purpose_city) as travel_path, -- 商旅路径
       f.flight_no as travel_detail, -- 商旅详细信息
       f.real_amount as amount, -- 费用金额
       IF(f.real_amount >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di f
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON f.d = td.days
LEFT JOIN project_view_detail pvd
ON f.project_code = pvd.project_code OR f.project_code = pvd.project_sale_code
WHERE (td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)) AND pvd.project_code is not null


UNION ALL 

SELECT '' as id, -- 主键
       pvd.project_code, -- 项目编码
       pvd.project_sale_code, -- 前置项目编码
       pvd.project_name, -- 项目名称
       CONCAT(pvd.project_code,'-',pvd.project_name) as project_info, -- 项目信息
       h.dept_name, -- 一级部门
       h.team_org_name, -- 二级部门
       h.check_in_name as travel_user, -- 商旅人员
       '酒店' as travel_type, -- 商旅类型
       date_format(h.check_in_date, 'yyyy-MM-dd HH:mm:ss') as start_time, -- 开始时间
       date_format(h.out_date, 'yyyy-MM-dd HH:mm:ss') as end_time, -- 结束时间
       h.hotel_class as travel_scope, -- 商旅范围
       h.hotel_city as travel_path, -- 商旅路径
       h.hotel_name as travel_detail, -- 商旅详细信息
       h.amount as amount, -- 费用金额
       IF(h.amount >= 0,'购票','退票') as order_type, -- 订单类型
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di h
LEFT JOIN ${dim_dbname}.dim_day_date td 
ON h.d = td.days
LEFT JOIN project_view_detail pvd
ON h.project_code = pvd.project_code OR h.project_code = pvd.project_sale_code
WHERE (td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)) AND pvd.project_code is not null;