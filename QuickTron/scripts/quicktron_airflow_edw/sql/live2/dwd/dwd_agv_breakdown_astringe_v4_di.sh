#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 小车故障收敛规则后信息表v4
#-- 注意 ： 每天增量分区，按照天和项目分区
#-- 输入表 : dwd.dwd_agv_breakdown_detail_incre_dt
#-- 输出表 ：dwd.dwd_agv_breakdown_astringe_v4_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-13 CREATE 
#-- 2 wangziming 2023-01-12 modify 回流七天的数据

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"




sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table dwd.dwd_agv_breakdown_astringe_v4_di partition (d, pt) 
select agv_code,
       error_code,
       error_name,
       error_display_name,
       error_level,
       breakdown_id,
       speed,
       bucket_id,
       warehouse_id,
       mileage,
       point_codes,
       point_x,
       point_y,
       breakdown_log_time,
       breakdown_collect_time,
       agv_type_id,
       agv_type_code,
       agv_type_name,
       project_code,
       project_name,
       first_classification,
     error_code_list,
     error_code_position,
     error_code_0_position_list,
       to_date(breakdown_log_time) as d,
       project_code                as pt
from (select t2.*,
                   case
                       when error_code <> LAG(error_code, 1)
                                              over (PARTITION by project_code,agv_code,to_date(breakdown_log_time) order by breakdown_log_time asc) or
                            LAG(error_code, 1)
                                over (PARTITION by project_code,agv_code,to_date(breakdown_log_time) order by breakdown_log_time asc) is null
                           then 1
                       else 0 end first_breakdown_flag,  --判断当前error_code与前一个是否相同，相同则标记为0
             
             case
                 when LAG(breakdown_log_time, 1)
                          over (PARTITION BY project_code,agv_code,to_date(breakdown_log_time) order by breakdown_log_time asc) is null or
                      unix_timestamp(breakdown_log_time) - unix_timestamp(LAG(breakdown_log_time, 1)
                                                                                 over (PARTITION BY project_code,agv_code,to_date(breakdown_log_time) order by breakdown_log_time asc)) >
                      60 then 1
                 else 0 end as error_flag --判断当前error_code与前一个时间间隔，间隔小于等于1分钟则标记为0           
            from (select *
                  from (select t.*,
                               ROW_NUMBER()
                                       over (PARTITION by d,pt,project_code,agv_code,breakdown_id order by error_code_position asc) as rk
                        from dwd.dwd_agv_breakdown_detail_incre_dt t
                        where 1 = 1
                          and d >= date_sub('${pre1_date}',7)
                          and error_code not in ('0')
                          and error_level >= 3) t1
                  where t1.rk = 1) t2) t3
 where t3.error_flag=1;
"

# t3.first_breakdown_flag|
printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

