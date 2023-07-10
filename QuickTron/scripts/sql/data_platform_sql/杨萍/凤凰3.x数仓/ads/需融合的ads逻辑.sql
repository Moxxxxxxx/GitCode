ads_amr_breakdown_detail
ads_amr_breakdown
ads_lite_amr_breakdown
ads_project_view_lite_amr_status
ads_project_view_lite_carry_order_count
ads_project_view_lite_charge_pile
ads_carry_work_analyse_count

ads_carry_work_analyse_detail -- 不做



reflow_ads_single_project_abc_count_info
reflow_ads_single_project_agv_fix_deatail
reflow_ads_single_project_agv_type_info
reflow_ads_single_project_classify_target
reflow_ads_single_project_equipment_detail
reflow_ads_single_project_intelligent_handling
reflow_ads_single_project_order_statistics
reflow_ads_single_project_synthesis_target


SELECT * FROM ${dim_dbname}.dim_collection_project_record_ful WHERE project_version like '3.%'


and project_version like '2.%'


tmp_phx_error_mtbf_add
ads_phx_amr_breakdown_detail
ads_phx_amr_breakdown
ads_phx_lite_amr_breakdown
ads_phx_carry_work_analyse_count

ads_phx_project_view_lite_amr_status
ads_phx_project_view_lite_carry_order_count
ads_phx_project_view_lite_charge_pile


select count(0) from ads.ads_phx_amr_breakdown_detail
select count(0) from ads.ads_phx_amr_breakdown
select count(0) from ads.ads_phx_lite_amr_breakdown
select count(0) from ads.ads_phx_carry_work_analyse_count
-- select count(0) from ads.ads_phx_project_view_lite_amr_status where pt='A53333' and d='2023-02-23'
select count(0) from ads.ads_project_view_lite_amr_status where pt='A53333' and d='2023-02-23'
select count(0) from ads.ads_project_view_lite_carry_order_count where pt='A53333'
select count(0) from ads.ads_project_view_lite_charge_pile where pt='A53333'
select * from ads.ads_carry_order_agv_type where project_code='A53333'


select * from evo_wds_base.ads_project_view_lite_amr_status where project_code='A53333'
select * from evo_wds_base.ads_project_view_lite_carry_order_count where project_code='A53333'
select * from evo_wds_base.ads_project_view_lite_charge_pile where project_code='A53333'
select * from evo_wds_base.ads_carry_order_agv_type where project_code='A53333'





tmp_phx_error_mtbf_add:dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful&dwd_phx_basic_robot_base_info_df&dwd_phx_rms_robot_state_info_di&dim_collection_project_record_ful&dim_day_date&dim_day_of_hour
ads_phx_amr_breakdown_detail:dim_collection_project_record_ful&dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful
ads_phx_amr_breakdown:dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful&dwd_phx_rss_transport_order_info_di&dwd_phx_rss_transport_order_carrier_job_info_di&dwd_phx_basic_robot_base_info_df&dim_day_date&dim_day_of_hour&dim_collection_project_record_ful&dwd_phx_rms_robot_state_info_di
ads_phx_lite_amr_breakdown:dwd_phx_basic_robot_base_info_df&dim_collection_project_record_ful&dwd_phx_rss_transport_order_info_di&dwd_phx_rss_transport_order_carrier_job_info_di&dwd_phx_basic_robot_base_info_df&dwd_phx_robot_breakdown_astringe_v1_di&dim_phx_basic_error_info_ful&dwd_phx_rms_robot_state_info_di&dwd_phx_rms_job_history_info_di
ads_phx_carry_work_analyse_count:dim_collection_project_record_ful&dwd_phx_rss_transport_order_carrier_cost_info_di&dwd_phx_rss_transport_order_info_di&dwd_phx_rss_transport_order_carrier_job_info_di&dwd_phx_basic_robot_base_info_df
ads_phx_project_view_lite_amr_status:dim_collection_project_record_ful&dwd_phx_basic_robot_base_info_df&dwd_phx_rms_robot_state_info_di
ads_phx_project_view_lite_carry_order_count:dim_collection_project_record_ful&dwd_phx_rss_transport_order_info_di
ads_phx_project_view_lite_charge_pile:dim_collection_project_record_ful&dwd_phx_basic_charger_info_df&dwd_phx_basic_map_info_df&dwd_phx_rms_robot_charging_info_di
reflow_ads_phx_amr_breakdown_detail:ads_phx_amr_breakdown_detail
reflow_ads_phx_lite_amr_breakdown:ads_phx_lite_amr_breakdown
reflow_ads_phx_amr_breakdown:ads_phx_amr_breakdown
reflow_ads_phx_carry_work_analyse_count:




---------------------------------------------------------------------
case when ts.stat_next_create_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(ts.stat_next_create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(ts.create_time),'yyyy-MM-dd HH:mm:ss')),0)*1000+nvl(cast(SUBSTRING(rpad(ts.stat_next_create_time,23,'0'),21,3) as int),0)-nvl(cast(SUBSTRING(rpad(ts.create_time,23,'0'),21,3) as int),0))/1000 end as state_duration