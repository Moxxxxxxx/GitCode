-- python manage.py makemigrations
-- python manage.py migrate







-- 所有的orm表
select * from qt_smartreport.qtr_dim_hour;                          
select * from qt_smartreport.qtr_dim_hour_seconds_sequence;         
select * from qt_smartreport.qtr_hour_robot_charge_stat_his;  
select * from qt_smartreport.qtr_hour_charger_charge_stat_his;   
select * from qt_smartreport.qtr_hour_robot_state_duration_his; 
select * from qt_smartreport.qtr_day_robot_error_detail_his;  
select * from qt_smartreport.qtr_hour_robot_error_time_detail_his; 
select * from qt_smartreport.qtr_day_robot_end_error_detail_his;
select * from qt_smartreport.qtr_hour_robot_error_list_his; 
select * from qt_smartreport.qtr_hour_robot_error_mtbf_his; 
select * from qt_smartreport.qtr_day_robot_error_list_his; 
select * from qt_smartreport.qtr_day_robot_error_mtbf_his;  
select * from qt_smartreport.qtr_week_robot_error_list_his;
select * from qt_smartreport.qtr_week_robot_error_mtbf_his;  
select * from qt_smartreport.qtr_day_sys_error_detail_his;    
select * from qt_smartreport.qtr_day_sys_end_error_detail_his; 
select * from qt_smartreport.qtr_hour_sys_error_duration_his; 
select * from qt_smartreport.qtr_hour_sys_error_list_his;
select * from qt_smartreport.qtr_hour_sys_error_mtbf_his; 
select * from qt_smartreport.qtr_day_sys_error_list_his;  
select * from qt_smartreport.qtr_day_sys_error_mtbf_his; 
select * from qt_smartreport.qtr_week_sys_error_list_his;  
select * from qt_smartreport.qtr_week_sys_error_mtbf_his; 
select * from qt_smartreport.qtr_hour_action_traffic_control_stat_his;
select * from qt_smartreport.qtr_hour_action_liftup_operation_his; 
select * from qt_smartreport.qtr_hour_action_putdown_operation_his; 
select * from qt_smartreport.qtr_hour_action_terminal_guide_operation_his; 
select * from qt_smartreport.qtr_hour_transport_order_stat_his;
select * from qt_smartreport.qtr_hour_transport_upstream_order_stat_his;


-- 所有的orm表(sqlserver)
select * from qt_smartreport.dbo.qtr_dim_hour;                          
select * from qt_smartreport.dbo.qtr_dim_hour_seconds_sequence;         
select * from qt_smartreport.dbo.qtr_hour_robot_charge_stat_his;  
select * from qt_smartreport.dbo.qtr_hour_charger_charge_stat_his;   
select * from qt_smartreport.dbo.qtr_hour_robot_state_duration_his; 
select * from qt_smartreport.dbo.qtr_day_robot_error_detail_his;  
select * from qt_smartreport.dbo.qtr_hour_robot_error_time_detail_his; 
select * from qt_smartreport.dbo.qtr_day_robot_end_error_detail_his;
select * from qt_smartreport.dbo.qtr_hour_robot_error_list_his; 
select * from qt_smartreport.dbo.qtr_hour_robot_error_mtbf_his; 
select * from qt_smartreport.dbo.qtr_day_robot_error_list_his; 
select * from qt_smartreport.dbo.qtr_day_robot_error_mtbf_his;  
select * from qt_smartreport.dbo.qtr_week_robot_error_list_his;
select * from qt_smartreport.dbo.qtr_week_robot_error_mtbf_his;  
select * from qt_smartreport.dbo.qtr_day_sys_error_detail_his;    
select * from qt_smartreport.dbo.qtr_day_sys_end_error_detail_his; 
select * from qt_smartreport.dbo.qtr_hour_sys_error_duration_his; 
select * from qt_smartreport.dbo.qtr_hour_sys_error_list_his;
select * from qt_smartreport.dbo.qtr_hour_sys_error_mtbf_his; 
select * from qt_smartreport.dbo.qtr_day_sys_error_list_his;  
select * from qt_smartreport.dbo.qtr_day_sys_error_mtbf_his; 
select * from qt_smartreport.dbo.qtr_week_sys_error_list_his;  
select * from qt_smartreport.dbo.qtr_week_sys_error_mtbf_his; 
select * from qt_smartreport.dbo.qtr_hour_action_traffic_control_stat_his;
select * from qt_smartreport.dbo.qtr_hour_action_liftup_operation_his; 
select * from qt_smartreport.dbo.qtr_hour_action_putdown_operation_his; 
select * from qt_smartreport.dbo.qtr_hour_action_terminal_guide_operation_his; 
select * from qt_smartreport.dbo.qtr_hour_transport_order_stat_his;
select * from qt_smartreport.dbo.qtr_hour_transport_upstream_order_stat_his;






-- 2、删除django_migrations

select * from  qt_smartreport.django_migrations where app='task_table';



-- 3、删除.py文件

