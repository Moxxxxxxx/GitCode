#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-7 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--项目工单日趋势指标统计表 ads_project_work_order_daily 

INSERT overwrite table ${ads_dbname}.ads_project_work_order_daily
SELECT '' as id,
       '${pre1_date}' as cur_date, 
       tmp.ft_name,
       tmp.project_code,
       tmp.project_name,
       tmp.project_operation_state,
       tmp.product_name,
       tmp.current_version,
       tmp.work_order_type,
       SUM(tmp.noclose_over_tendays_inthirtydays) as noclose_over_tendays_inthirtydays, -- 近30天工单生存时长超10日工单数量
       SUM(tmp.noclose_over_tendays_inninetydays) as noclose_over_tendays_inninetydays, -- 近90天工单生存时长超10日工单数量
       CAST(nvl(SUM(tmp.noclose_over_tendays_inthirtydays)/SUM(tmp.total_thirtydays),0) as decimal(10,2)) as noclose_over_tendays_inthirtydays_rate, -- 近30天工单生存时长超10日的工单占比
       CAST(nvl(SUM(tmp.noclose_over_tendays_inninetydays)/SUM(tmp.total_ninetydays),0) as decimal(10,2)) as noclose_over_tendays_inninetydays_rate, -- 近90天工单生存时长超10日的工单占比
       SUM(tmp.closed_sevendays) as closed_sevendays_num, -- 近7日工单关闭数量
       SUM(tmp.total_sevendays) as total_sevendays, -- 近7日累计的工单总数
       CAST(nvl(SUM(tmp.closed_sevendays)/SUM(tmp.total_sevendays),0) as decimal(10,2)) as closed_sevendays_rate, --近7日工单关闭率
       SUM(tmp.closed_fourteendays) as closed_fourteendays_num, -- 近14日工单关闭数量
       SUM(tmp.total_fourteendays) as total_fourteendays, -- 近14日累计的工单总数
       CAST(nvl(SUM(tmp.closed_fourteendays)/SUM(tmp.total_fourteendays),0) as decimal(10,2)) as closed_fourteendays_rate, -- 近14日工单关闭率
       SUM(tmp.closed_thirtydays) as closed_thirtydays_num, -- 近30日工单关闭数量
       SUM(tmp.total_thirtydays) as total_thirtydays, -- 近30日累计的工单总数
       CAST(nvl(SUM(tmp.closed_thirtydays)/SUM(tmp.total_thirtydays),0) as decimal(10,2)) as closed_thirtydays_rate, -- 近30日工单关闭率
       SUM(tmp.closed_ninetydays) as closed_ninetydays_num, -- 近90日工单关闭数量
       SUM(tmp.total_ninetydays) as total_ninetydays, -- 近90日累计的工单总数
       CAST(nvl(SUM(tmp.closed_ninetydays)/SUM(tmp.total_ninetydays),0) as decimal(10,2)) as closed_ninetydays_rate, -- 近90日工单关闭率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
( 
  SELECT ftw.ft_name, -- 所属产品线
         t.project_code, -- 项目编码
         t.project_name, -- 项目名称
         t.project_operation_state, -- 项目运营阶段
         t.product_name, -- 场景（即产品名称）
         t.current_version, -- 产品版本
         case when t.close_name = '售后' then '售后工单'
              when t.close_name = '技术支持' or (t.close_name = '研发' and t.issue_type_cname = '任务') then '恢复工单'
              when t.close_name = '研发' and t.issue_type_cname = '缺陷' then '缺陷工单'
              when t.close_name = '实施' then '实施工单'
              when t.close_name = '硬件自动化' then '硬件工单'
              else '其他工单' end as work_order_type, -- 工单类型（恢复工单，缺陷工单，售后工单，实施工单，硬件工单，其他工单）
         IF(t.case_status != '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) >= 86400*10,1,0) as noclose_over_tendays, -- 超10日未关闭工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*30 and ((t.case_status = '6-已关闭' and unix_timestamp(t.status_time) - unix_timestamp(t.created_time) >= 86400*10) or (t.case_status != '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) >= 86400*10)),1,0) as noclose_over_tendays_inthirtydays, -- 近30日工单生存时长超10日工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*90 and ((t.case_status = '6-已关闭' and unix_timestamp(t.status_time) - unix_timestamp(t.created_time) >= 86400*10) or (t.case_status != '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) >= 86400*10)),1,0) as noclose_over_tendays_inninetydays, -- 近90日工单生存时长超10日工单
         IF(t.case_status = '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*7 and unix_timestamp('${pre1_date}') - unix_timestamp(t.status_time) <= 86400*7,1,0) as closed_sevendays, -- 近7日已关闭工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*7,1,0) as total_sevendays, -- 近7日累计工单
         IF(t.case_status = '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*14 and unix_timestamp('${pre1_date}') - unix_timestamp(t.status_time) <= 86400*14,1,0) as closed_fourteendays, -- 近14日已关闭工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*14,1,0) as total_fourteendays, -- 近14日累计工单
         IF(t.case_status = '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*30 and unix_timestamp('${pre1_date}') - unix_timestamp(t.status_time) <= 86400*30,1,0) as closed_thirtydays, -- 近30日已关闭工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*30,1,0) as total_thirtydays, -- 近30日累计工单
         IF(t.case_status = '6-已关闭' and unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*90 and unix_timestamp('${pre1_date}') - unix_timestamp(t.status_time) <= 86400*90,1,0) as closed_ninetydays, -- 近90日已关闭工单
         IF(unix_timestamp('${pre1_date}') - unix_timestamp(t.created_time) <= 86400*90,1,0) as total_ninetydays -- 近90日累计工单    
  FROM 
  (
    SELECT DISTINCT tft.ft_id,
                    tft.ft_name
    FROM ${dim_dbname}.dim_ft_team_info_offline tft
  ) ftw
  LEFT JOIN
  (
    SELECT t1.ticket_id,
           t1.project_code,
           t2.project_name,
           t2.ft_name,
           t2.project_operation_state,
           t2.product_name,
           t2.current_version,
           t1.case_status,
           t1.created_time,
           t1.status_time,
           t1.close_name,
           t3.issue_type_cname
    FROM 
    (
      SELECT w.ticket_id,
             w.project_code,
             w.case_status,
             w.status_time,
             w.created_time,
             w.close_name
      FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
      WHERE w.d = '${pre1_date}' AND w.project_code is not null AND w.work_order_status != '已驳回' AND lower(w.project_code) not regexp 'test|tese'
    ) t1 
    LEFT JOIN
    (
      SELECT v.project_code,
             v.project_name,
             nvl(v.project_attr_ft,'未知') as ft_name,
             if(v.project_operation_state = 'UNKNOWN' OR v.project_operation_state is null,'未知',v.project_operation_state) as project_operation_state,
             if(v.project_product_name = 'UNKNOWN' OR v.project_product_name is null,'未知',v.project_product_name) as product_name,
             if(v.project_current_version = 'UNKNOWN' OR v.project_current_version is null,'其它',v.project_current_version) as current_version
      FROM ${dwd_dbname}.dwd_pms_share_project_base_info_df v
      WHERE v.d = '${pre1_date}'
    )t2 
    ON t1.project_code = t2.project_code
    LEFT JOIN
    (
      SELECT t2.field_value,
             t1.issue_type_cname
      FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
      LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful t2 
      ON t2.task_uuid = t1.uuid
      WHERE t2.field_uuid = 'S993wZTA' AND t2.field_value is not null --工单号属性:field_uuid ='S993wZTA'
    )t3 
    ON t1.ticket_id = t3.field_value
    WHERE 1 = 1
  ) t 
  ON t.ft_name = ftw.ft_name 
)tmp
GROUP BY tmp.ft_name,tmp.project_code,tmp.project_name,tmp.project_operation_state,tmp.product_name,tmp.current_version,tmp.work_order_type;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"