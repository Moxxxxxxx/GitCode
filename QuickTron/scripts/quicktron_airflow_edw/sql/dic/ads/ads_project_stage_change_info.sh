#!/bin/bash
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
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ads_project_stage_change_info    --项目阶段变化表

with a as
(
  SELECT total.project_code_class,
         total.project_area,
         total.project_priority,
         total.project_ft,
         total.month_scope,
         total.pre_num,
         total.handover_num,
         nvl(total.amount,0) as total_amount,
         nvl(online.online_num,0) as online_num,
         nvl(online.amount,0) as online_amount,
         IF(total.project_code_class IN ('A','C'),total.handover_num - nvl(online.online_num,0),total.pre_num - nvl(online.online_num,0)) as no_online_num,
         nvl(total.amount,0) - nvl(online.amount,0) as no_online_amount,
         nvl(final_inspection.final_inspection_num,0) as final_inspection_num,
         nvl(final_inspection.amount,0) as final_inspection_amount,
         IF(total.project_code_class IN ('A','C'),total.handover_num - nvl(final_inspection.final_inspection_num,0),total.pre_num - nvl(final_inspection.final_inspection_num,0)) as no_final_inspection_num,
         nvl(total.amount,0) - nvl(final_inspection.amount,0) as no_final_inspection_amount
  FROM 
  (
    SELECT b.project_code_class,
           b.project_area,
           b.project_priority,
           b.project_ft,
           td.month_scope,
           SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
           SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
           SUM(nvl(tmp1.amount,0)) as amount
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
             pf.project_ft
      FROM 
      (
        SELECT split(project_code_class,',') as a,
               split(nvl(project_area,'未知'),',') as b,
               split(nvl(project_priority,'未知'),',') as c,
               split(nvl(project_ft,'未知'),',') as d
        FROM ${tmp_dbname}.tmp_pms_project_general_view_detail
        GROUP BY project_code_class,project_area,project_priority,project_ft
      ) tmp
      lateral view explode(a) pcc as project_code_class 
      lateral view explode(b) pa as project_area
      lateral view explode(c) pp as project_priority
      lateral view explode(d) pf as project_ft
    ) b
    LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail tmp1
    ON nvl(b.project_code_class,'unknown1') = nvl(tmp1.project_code_class,'unknown2') AND nvl(b.project_area,'unknown1') = nvl(tmp1.project_area,'unknown2') AND nvl(b.project_priority,'unknown1') = nvl(tmp1.project_priority,'unknown2') AND nvl(b.project_ft,'unknown1') = nvl(tmp1.project_ft,'unknown2') AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
    GROUP BY b.project_code_class,b.project_area,b.project_priority,b.project_ft,td.month_scope
  )total
  LEFT JOIN
  (
    SELECT tmp2.project_code_class,
           tmp2.project_area,
           tmp2.project_priority,
           tmp2.project_ft,
           tmp2.online_process_month,
           SUM(case when tmp2.is_online = '已上线' then 1 else 0 end) as online_num,
           SUM(tmp2.amount) as amount
    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail tmp2
    WHERE tmp2.is_online = '已上线' 
    GROUP BY tmp2.project_code_class,tmp2.project_area,tmp2.project_priority,tmp2.project_ft,tmp2.online_process_month
  )online
  ON nvl(total.project_code_class,'unknown1') = nvl(online.project_code_class,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(online.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(online.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(online.project_ft,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(online.online_process_month,'unknown2')
  LEFT JOIN
  (
    SELECT tmp3.project_code_class,
           tmp3.project_area,
           tmp3.project_priority,
           tmp3.project_ft,
           tmp3.final_inspection_process_month,
           SUM(case when tmp3.is_final_inspection = '已验收' then 1 else 0 end) as final_inspection_num,
           SUM(tmp3.amount) as amount
    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail tmp3
    WHERE tmp3.is_final_inspection = '已验收'
    GROUP BY tmp3.project_code_class,tmp3.project_area,tmp3.project_priority,tmp3.project_ft,tmp3.final_inspection_process_month
  )final_inspection
  ON nvl(total.project_code_class,'unknown1') = nvl(final_inspection.project_code_class,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(final_inspection.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(final_inspection.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(final_inspection.project_ft,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(final_inspection.final_inspection_process_month,'unknown2')
)

INSERT overwrite table ${ads_dbname}.ads_project_stage_change_info
SELECT '' as id,
       a.project_code_class,
       a.project_area,
       a.project_priority,
       a.project_ft,
       a.month_scope,
       SUM(b.pre_num) as pre_num,
       SUM(b.handover_num) as handover_num,
       SUM(b.total_amount) as total_amount,
       SUM(b.online_num) as online_num,
       SUM(b.online_amount) as online_amount,
       SUM(b.no_online_num) as no_online_num,
       SUM(b.no_online_amount) as no_online_amount,
       SUM(b.final_inspection_num) as final_inspection_num,
       SUM(b.final_inspection_amount) as final_inspection_amount,
       SUM(b.no_final_inspection_num) as no_final_inspection_num,
       SUM(b.no_final_inspection_amount) as no_final_inspection_amount,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM a
LEFT JOIN a b
ON nvl(a.project_code_class,'unknown1') = nvl(b.project_code_class,'unknown2') AND nvl(a.project_area,'unknown1') = nvl(b.project_area,'unknown2') AND nvl(a.project_priority,'unknown1') = nvl(b.project_priority,'unknown2') AND nvl(a.project_ft,'unknown1') = nvl(b.project_ft,'unknown2') AND nvl(a.month_scope,'unknown1') >= nvl(b.month_scope,'unknown2')
GROUP BY a.project_code_class,a.project_area,a.project_priority,a.project_ft,a.month_scope;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"