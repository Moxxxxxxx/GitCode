-- 项目信息表
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_name,project_code,project_product_name,project_current_version,project_ft,project_priority,project_area,pe_members,project_progress_stage,deliver_goods_achieving_rate,pm_name 
								             from ads_project_general_view_detail 
											 where project_code = 'A51118'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_name","project_code","project_type","project_version","ft","project_level","to_distinct","pe","pro_stage","completion_rate","pm"],
                        "preSql": ["truncate table ads_project_view_project_info"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_project_info"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 项目交付进度表
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_progress_stage,project_code,project_stage,deliver_goods_achieving_rate,date(pre_project_approval_time),contract_signed_date,date(project_handover_end_time),date(sap_entry_date),
          	                                        expect_online_date,online_date,online_overdue_days,expect_final_inspection_date,final_inspection_date,final_inspection_overdue_days,deliver_goods_desc 
											 from ads_project_general_view_detail 
											 where project_code = 'A51118'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["processStageGroup","project_code","process_stage","delivery_completed_rate","lead_time","contract_date","handover_time","entrance_time",
						           "planned_launch_time","actual_launch_time","launch_out_time","planned_accept_time","actual_accept_time","accept_out_time","delivery_status"], 
						"preSql": ["truncate table ads_project_view_project_process"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_project_process"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 机器人故障统计
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select agv_type_code,project_code,breakndown_num,order_breakndown_rate,order_num,move_job_breakndown_rate,move_job_num,OEE,MTBF,MTTR
											 from ads_single_project_agv_type_info 
											 where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["amr_type","project_code","breakdown_num","order_breakdown_rate","order_num","task_breakdown_rate","task_num","oee","mtbf","mttr"], 
						"preSql": ["truncate table ads_project_view_breakdown_count"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_breakdown_count"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 机器人故障Top5
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,classify_value,num_of_times
											 from ads_single_project_classify_target 
											 where project_code = 'A51118' and cur_date = $'{pre1_date}' and classify = '机器人故障'
											 order by sort asc"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","amr_code","breakdown_num"], 
						"preSql": ["truncate table ads_project_view_breakdown_top5"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_breakdown_top5"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 机器人故障码Top5
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,classify_value,num_of_times
											 from ads_single_project_classify_target 
											 where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '机器人故障码'
											 order by sort asc"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","err_des","breakdown_num"], 
						"preSql": ["truncate table ads_project_view_error_code_top5"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_error_code_top5"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 设备信息
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select null as warehouse_area,project_code,agv_num,charger_num
											 from ads_project_general_view_detail 
											 where project_code = 'A51118'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["warehouse_area","project_code","amr_num","charging_num"], 
						"preSql": ["truncate table ads_project_view_project_equipment"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_project_equipment"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 库存统计
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,bucket_num_total,slot_num_total,slot_num_actual,slot_using_rate,sku_num_total,sku_num_actual,quantity_total,inventory_depth
											 from ads_single_project_synthesis_target 
											 where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","shelvesNum","totalGoodsNum","useGoodsNum","goodsRate","totalGoodsSku","onlineGoodsSku","totalStock","avgStock"], 
						"preSql": ["truncate table ads_project_view_stock_count"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_stock_count"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 库存统计明细
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,class_type,sku_num,sku_rate,sku_picking_quantity,picking_quantity_rate,sku_inventory_quantity,inventory_quantity_rate
											 from ads_single_project_abc_count_info 
											 where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","skuType","saleStockNum","saleStockRate","outStockNum","outStockRate","stockNum","stockRate"], 
						"preSql": ["truncate table ads_project_view_stock_count_detail"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_stock_count_detail"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 出入库单量
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select t1.project_code,t1.order_num,t1.orderline_num,t1.quantity_num,t1.quantity_order_rate,t1.quantity_orderline_rate,t1.orderline_order_rate,
								                    t2.order_num,t2.orderline_num,t2.quantity_num,t2.quantity_order_rate,t2.quantity_orderline_rate,t2.orderline_order_rate
                                             from ads_single_project_order_statistics t1
                                             left join ads_single_project_order_statistics t2
                                             on t1.cur_date = t2.cur_date and t1.project_code = t2.project_code and t1.run_type = t2.run_type and t1.order_type != t2.order_type
                                             where t1.project_code = 'A51118' and t1.cur_date = '${pre1_date}' and t1.run_type = '日' and t1.order_type = '入库'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","in_order_num","in_order_line_num","in_total_num","in_piece_order","in_piece_line","in_line_order","out_order_num","out_order_line_num","out_total_num","out_piece_order","out_piece_line","out_line_order"], 
						"preSql": ["truncate table ads_project_view_warehousing_order_num"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_warehousing_order_num"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 出入库单量分时
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select t1.project_code,t1.order_type,t1.cur_hour,t1.in_num,t2.out_num
                                             from
                                             (
                                               select t1.project_code,'nums' as order_type,t1.cur_hour,t1.order_num as in_num
                                               from ads_single_project_order_statistics t1
                                               where t1.project_code = 'A51118' and t1.cur_date = '${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                               union all
                                               select t1.project_code,'lines' as order_type,t1.cur_hour,t1.orderline_num as in_num
                                               from ads_single_project_order_statistics t1
                                               where t1.project_code = 'A51118' and t1.cur_date = '${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                               union all
                                               select t1.project_code,'pieces' as order_type,t1.cur_hour,t1.quantity_num as in_num
                                               from ads_single_project_order_statistics t1
                                               where t1.project_code = 'A51118' and t1.cur_date = '${pre1_date}' and t1.run_type = '小时' and t1.order_type = '入库'
                                             )t1
                                             left join 
                                             (
                                               select t2.project_code,'nums' as order_type,t2.cur_hour,t2.order_num as out_num
                                               from ads_single_project_order_statistics t2
                                               where t2.project_code = 'A51118' and t2.cur_date = '${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                               union all
                                               select t2.project_code,'lines' as order_type,t2.cur_hour,t2.orderline_num as out_num
                                               from ads_single_project_order_statistics t2
                                               where t2.project_code = 'A51118' and t2.cur_date = '${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                               union all
                                               select t2.project_code,'pieces' as order_type,t2.cur_hour,t2.quantity_num as out_num
                                               from ads_single_project_order_statistics t2
                                               where t2.project_code = 'A51118' and t2.cur_date = '${pre1_date}' and t2.run_type = '小时' and t2.order_type = '出库'
                                             )t2
                                             on t1.project_code = t2.project_code and t1.order_type = t2.order_type and t1.cur_hour = t2.cur_hour
                                             order by t1.project_code,t1.order_type,t1.cur_hour"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","order_type","time_sharing","in_num","out_num"], 
						"preSql": ["truncate table ads_project_view_warehousing_order_num_today"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_warehousing_order_num_today"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 拣选效率
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,station_num,station_num_total,station_free_rate,picking_orderline_efficiency,picking_quantity_efficiency,once_instation_duration,once_win_open_times,once_picking_orderline,once_picking_quantity,once_station_interval
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","work_station_on","work_station_num","work_station_free_rate","pick_efficiency_line","pick_efficiency_piece","single_duration","single_popup_time","single_hit_line","pick_order_num","`interval`"], 
						"preSql": ["truncate table ads_project_view_pick_efficiency"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_pick_efficiency"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 系统工单转化统计
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,sys_order_num,sys_order_rate,trans_dev_order_num,trans_dev_order_rate,dev_trouble_num,dev_trouble_rate,order_num,order_rate
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","sys_order_num","sys_order_rate","trans_dev_order_num","trans_dev_order_rate","dev_trouble_num","dev_trouble_rate","scene_order","scene_order_rate"], 
						"preSql": ["truncate table ads_project_view_trans_sys_order_count"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_trans_sys_order_count"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 机器人类型
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select agv_type_code,project_code,agv_num,offline_maintain_num
                                             from ads_single_project_agv_type_info
                                             where project_code = 'A51118' and cur_date = '${pre1_date}' and agv_type_code != 'all'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["amr_type","project_code","total_num","unlinefix"], 
						"preSql": ["truncate table ads_project_view_amr_type_detail"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_amr_type_detail"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 系统故障统计排行
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,classify_value,num_of_times
                                             from ads_single_project_classify_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '系统故障'
											 order by sort asc"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","order_type","breakdown_num"], 
						"preSql": ["truncate table ads_project_view_sys_order_ansys_rank"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_sys_order_ansys_rank"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 系统工单分析统计
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,classify_value,num_of_times
                                             from ads_single_project_classify_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '系统工单'
											 order by sort asc"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","order_type","breakdown_num"], 
						"preSql": ["truncate table ads_project_view_sys_order_ansys"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_sys_order_ansys"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 累计维修情况
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select total_fix_time,project_code,total_fix_num,avg_fix_duration
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["total_fix_time","project_code","total_fix_num","avg_fix_duration"], 
						"preSql": ["truncate table ads_project_view_amr_repair"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_amr_repair"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 自恢复情况
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,total,fail_time,succes_time,avg_recover_duration
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","total","fail_time","succes_time","avg_recover_duration"], 
						"preSql": ["truncate table ads_project_view_manual_recovery_count"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_manual_recovery_count"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 死锁情况
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,dead_lock_num,avg_duration,manul_reduce
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","dead_lock_num","avg_duration","manul_reduce"], 
						"preSql": ["truncate table ads_project_view_dead_lock_num"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_dead_lock_num"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 拥堵情况
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,traffic_jam_num,avgtraffic_duration,car_num
                                             from ads_single_project_synthesis_target
                                             where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","traffic_jam_num","avgtraffic_duration","car_num"], 
						"preSql": ["truncate table ads_project_view_traffic_jam_num"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_traffic_jam_num"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 人工介入分布
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,classify_value,num_of_times,null as rate
											 from ads_single_project_classify_target 
											 where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '人工介入'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","recoverMode","num","rate"], 
						"preSql": ["truncate table ads_project_view_manual_recovery"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_manual_recovery"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 死锁分布
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,num_of_times,classify_value
											 from ads_single_project_classify_target 
											 where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '死锁'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","dead_lock_num_detail","dead_lock_section"], 
						"preSql": ["truncate table ads_project_view_dead_lock_num_dis"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_dead_lock_num_dis"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 拥堵分布
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,num_of_times,classify_value
											 from ads_single_project_classify_target 
											 where project_code = 'A51118' and cur_date = '${pre1_date}' and classify = '拥堵'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","trafficJamNumDetail","trafficJamSection"], 
						"preSql": ["truncate table ads_project_view_traffic_jam_num_dis"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_traffic_jam_num_dis"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 维修明细
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select agv_code,project_code,agv_type_code,start_fix_time,fix_duration,fix_reason
											 from ads_single_project_agv_fix_deatail
											 where project_code = 'A51118' and cur_date = '${pre1_date}'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads?mapreduce.job.queuename=hive"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["amr_code","project_code","amr_type","start_fix_time","fix_duration","fix_reason"], 
						"preSql": ["truncate table ads_project_view_amr_fix_duration"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_amr_fix_duration"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

-- 设备清单
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select project_code,equiqment_name,equiqment_num
											 from ads_single_project_equipment_detail
											 where project_code = 'A51118'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads?mapreduce.job.queuename=hive"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["project_code","equiqment_name","equiqment_num"], 
						"preSql": ["truncate table ads_project_view_project_equipment_detail"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/pvw", 
                                "table": ["ads_project_view_project_equipment_detail"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}

{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [
            {
                 "reader": {
                    "name": "rdbmsreader",
                    "parameter": {
                        "username": "wangyingying",
                        "password": "wangyingying4",
                        "connection": [
                            {
								"querySql":["select agv_code,project_code,agv_type_code,agv_type_name,start_fix_time,fix_duration,end_fix_time,fix_reason
											 from ads_single_project_agv_fix_deatail
											 where project_code = 'A51118'"],
                                "jdbcUrl": ["jdbc:hive2://003.bg.qkt:10000/ads?mapreduce.job.queuename=hive"]
                            }
                        ]
                    }
                },
				"writer": {
                    "name": "mysqlwriter", 
                    "parameter": {
                        "column": ["amr_code","project_code","amr_type","amr_type_name","start_fix_time","fix_duration","end_fix_time","fix_reason"], 
						"preSql": ["truncate table ads_project_view_amr_fix_duration"],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://007.bg.qkt:3306/evo_wds_base", 
                                "table": ["ads_project_view_amr_fix_duration"]
                            }
                        ], 
                        "password": "quicktron123456", 
                        "username": "root"
                    }
                }
            }
        ]
    }
}
