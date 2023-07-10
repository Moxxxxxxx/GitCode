--select * from ads.ads_phx_project_view_lite_charge_pile 
--select * from ads.ads_phx_project_view_lite_carry_order_count
--select * from ads.ads_phx_project_view_lite_amr_status

--select * from ads.ads_project_view_lite_charge_pile where pt='A53333'
--select * from ads.ads_project_view_lite_carry_order_count where pt='A53333'
--select * from ads.ads_project_view_lite_amr_status where pt='A53333'
--select * from ads.ads_carry_order_agv_type  where project_code='A53333'
-- select * from ads.ads_carry_order_point where project_code='A53333'



--select * from ads.ads_phx_carry_work_analyse_count
--select * from ads.ads_phx_amr_breakdown_detail
select * from ads.ads_phx_lite_amr_breakdown
select * from ads.ads_phx_amr_breakdown
select * from tmp.tmp_phx_error_mtbf_add







case when ts.stat_next_create_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(ts.stat_next_create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(ts.create_time),'yyyy-MM-dd HH:mm:ss')),0)*1000+nvl(cast(SUBSTRING(rpad(ts.stat_next_create_time,23,'0'),21,3) as int),0)-nvl(cast(SUBSTRING(rpad(ts.create_time,23,'0'),21,3) as int),0))/1000 end as state_duration  -- 状态持续时长（秒）



tmp_phx_error_mtbf_add:dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_phx_basic_robot_base_info_df&dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful&dwd_phx_rms_robot_state_daily_info_di&dwd_phx_rms_robot_state_info_di

ads_phx_amr_breakdown:dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_phx_basic_robot_base_info_df&dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful&dwd_phx_rms_robot_state_info_di&dwd_phx_rss_transport_order_info_di&dwd_phx_rss_transport_order_carrier_job_info_di&tmp_phx_error_mtbf_add