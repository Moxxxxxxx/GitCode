--  insert overwrite table dwd.dwd_agv_breakdown_astringe_v4_di partition (d, pt) 
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
                          and d = '${pre1_date}'
                          and error_code not in ('0')
                          and error_level >= 3) t1
                  where t1.rk = 1) t2) t3
 where t3.first_breakdown_flag|t3.error_flag=1;