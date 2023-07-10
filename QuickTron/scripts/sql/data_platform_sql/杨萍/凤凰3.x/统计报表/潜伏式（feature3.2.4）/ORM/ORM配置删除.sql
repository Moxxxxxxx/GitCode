-- python manage.py makemigrations
-- python manage.py migrate



select * from qt_smartreport.qtr_dim_hour;                          
select * from qt_smartreport.qtr_dim_hour_seconds_sequence;         
select * from qt_smartreport.qtr_hour_robot_charge_stat_his;        
select * from qt_smartreport.qtr_hour_charger_charge_stat_his;     
select * from qt_smartreport.qtr_day_transport_order_link_detail_stat_his; 
select * from qt_smartreport.qtr_transport_order_detail_stat_his;    
select * from qt_smartreport.qtr_transport_upstream_order_detail_stat_his;   
select * from qt_smartreport.qtr_hour_transport_order_link_detail_his;   
select * from qt_smartreport.qtr_hour_transport_order_detail_his;   
select * from qt_smartreport.qtr_hour_transport_upstream_order_detail_his;   
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




-- 1、删除表
drop table qt_smartreport.qtr_dim_hour;
drop table qt_smartreport.qtr_dim_hour_seconds_sequence;
drop table qt_smartreport.qtr_hour_robot_charge_stat_his;
drop table qt_smartreport.qtr_hour_charger_charge_stat_his;
drop table qt_smartreport.qtr_day_transport_order_link_detail_stat_his;
drop table qt_smartreport.qtr_transport_order_detail_stat_his;
drop table qt_smartreport.qtr_transport_upstream_order_detail_stat_his;
drop table qt_smartreport.qtr_hour_transport_order_link_detail_his;
drop table qt_smartreport.qtr_hour_transport_order_detail_his;
drop table qt_smartreport.qtr_hour_transport_upstream_order_detail_his;
drop table qt_smartreport.qtr_hour_robot_state_duration_his;
drop table qt_smartreport.qtr_day_robot_error_detail_his;
drop table qt_smartreport.qtr_hour_robot_error_time_detail_his;
drop table qt_smartreport.qtr_day_robot_end_error_detail_his;
drop table qt_smartreport.qtr_hour_robot_error_list_his;
drop table qt_smartreport.qtr_hour_robot_error_mtbf_his;
drop table qt_smartreport.qtr_day_robot_error_list_his;
drop table qt_smartreport.qtr_day_robot_error_mtbf_his;
drop table qt_smartreport.qtr_week_robot_error_list_his;
drop table qt_smartreport.qtr_week_robot_error_mtbf_his;
drop table qt_smartreport.qtr_day_sys_error_detail_his;
drop table qt_smartreport.qtr_day_sys_end_error_detail_his;
drop table qt_smartreport.qtr_hour_sys_error_duration_his;
drop table qt_smartreport.qtr_hour_sys_error_list_his;
drop table qt_smartreport.qtr_hour_sys_error_mtbf_his;
drop table qt_smartreport.qtr_day_sys_error_list_his;
drop table qt_smartreport.qtr_day_sys_error_mtbf_his;
drop table qt_smartreport.qtr_week_sys_error_list_his;
drop table qt_smartreport.qtr_week_sys_error_mtbf_his;




-- 2、删除django_migrations

select * from  qt_smartreport.django_migrations where app='task_table';
delete from  qt_smartreport.django_migrations where app='task_table';
select * from  qt_smartreport.django_migrations where app='task_table';


-- 3、删除.py文件

