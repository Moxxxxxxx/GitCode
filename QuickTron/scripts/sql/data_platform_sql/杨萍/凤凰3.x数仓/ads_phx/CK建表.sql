-- 表1：local_ads_phx_carry_work_analyse_count

DROP table ads.ads_phx_carry_work_analyse_count ON CLUSTER quicktron_31;
DROP table ads.local_ads_phx_carry_work_analyse_count ON CLUSTER quicktron_31;



CREATE TABLE ads.local_ads_phx_carry_work_analyse_count ON CLUSTER quicktron_31
(
    `data_time` DateTime COMMENT '数据产生时间',
    `upstream_work_id` Nullable(String) COMMENT '上游作业单',
    `work_id` String COMMENT '搬运作业单',
    `work_path` String COMMENT '路径',
    `stage` Nullable(String) COMMENT '阶段（暂定）',
    `start_point` Nullable(String) COMMENT '起始点',
    `target_point` Nullable(String) COMMENT '结束点',
    `work_state` Nullable(String) COMMENT '作业单状态',
    `first_classification` Nullable(String) COMMENT '机器人类型',
    `first_classification_desc` String COMMENT '机器人类型中文描述',
    `agv_type_code` Nullable(String) COMMENT '机器人类型编码',
    `agv_code` Nullable(String) COMMENT '机器人编码',
    `robot_num` Nullable(Int32) COMMENT '分配机器人数量',
    `wotk_duration_total` Float64 COMMENT '总耗时',
    `robot_assign_duration` Float64 COMMENT '分车耗时',
    `robot_move_duration` Float64 COMMENT '搬运耗时',
    `station_executor_duration` Float64 COMMENT '进站实操耗时',
    `work_create_time` String COMMENT '作业单创建时间',
    `work_complete_time` String COMMENT '作业单完成时间',
    `project_code` String COMMENT '项目编码',
    `create_time` DateTime DEFAULT now() COMMENT '数据创建时间',
    `update_time` DateTime DEFAULT now() COMMENT '数据更新时间'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(data_time)
PRIMARY KEY (data_time, project_code, work_id)
ORDER BY (data_time, project_code, work_id);


CREATE TABLE ads.ads_phx_carry_work_analyse_count ON CLUSTER quicktron_31 AS ads.local_ads_phx_carry_work_analyse_count
    ENGINE = Distributed(quicktron_31, ads, local_ads_phx_carry_work_analyse_count, halfMD5(data_time));   


-- 表2：local_ads_phx_amr_breakdown_detail


DROP table ads.ads_phx_amr_breakdown_detail ON CLUSTER quicktron_31;
DROP table ads.local_ads_phx_amr_breakdown_detail ON CLUSTER quicktron_31;
 


CREATE TABLE ads.local_ads_phx_amr_breakdown_detail ON CLUSTER quicktron_31
(
    `error_id` String COMMENT '故障id',
    `data_time` DateTime COMMENT '数据产生时间',
    `happen_time` String COMMENT '故障发生时间',
    `amr_type` String COMMENT '机器人类型',
    `carry_type_des` String COMMENT '搬运机器人类型描述',
    `carry_type` String COMMENT '搬运机器人类型',
    `amr_type_des` String COMMENT '机器人类型描述',
    `amr_code` String COMMENT '机器人编码',
    `error_level` Int32 COMMENT '错误等级',
    `error_des` String COMMENT '错误描述',
    `error_code` String COMMENT '错误编码',
    `error_module` String COMMENT '故障模块',
    `end_time` String COMMENT '故障结束时间',
    `error_duration` Float64 COMMENT '故障时长',
    `project_code` String COMMENT '项目编码',
    `create_time` DateTime DEFAULT now() COMMENT '数据创建时间',
    `update_time` DateTime DEFAULT now() COMMENT '数据更新时间'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(data_time)
PRIMARY KEY (error_id, data_time, happen_time, amr_code, error_code, project_code)
ORDER BY (error_id, data_time, happen_time, amr_code, error_code, project_code);



CREATE TABLE ads.ads_phx_amr_breakdown_detail ON CLUSTER quicktron_31 AS ads.local_ads_phx_amr_breakdown_detail
    ENGINE = Distributed(quicktron_31, ads, local_ads_phx_amr_breakdown_detail, halfMD5(data_time));   


-- 表3：local_ads_phx_amr_breakdown

DROP table ads.ads_phx_amr_breakdown ON CLUSTER quicktron_31;
DROP table ads.local_ads_phx_amr_breakdown ON CLUSTER quicktron_31;


CREATE TABLE ads.local_ads_phx_amr_breakdown ON CLUSTER quicktron_31
(
    `data_time` DateTime COMMENT '数据产生时间（业务无关）',
    `breakdown_id` String COMMENT '错误次数',
    `carry_order_num` Int32 COMMENT '搬运作业单数',
    `carry_task_num` Int32 COMMENT '搬运任务量',
    `amr_type` String COMMENT '机器人类型',
    `amr_type_des` String COMMENT '机器人类型描述',
    `mttr_error_num` Int32 COMMENT 'mttr错误次数',
    `amr_code` String COMMENT '机器人编码',
    `theory_time` Float64 COMMENT '无故障时长',
    `error_duration` Float64 COMMENT 'oee故障时长',
    `mttr_error_duration` Float64 COMMENT 'mttr故障时长',
    `add_mtbf` Float64 COMMENT '累计mtbf',
    `type_class` String COMMENT '筛选类型（单个：single,\n类型：part，全部：all）',
    `project_code` String COMMENT '项目编码',
    `happen_time` String COMMENT '数据实际产生时间(含小时)',
    `create_time` DateTime DEFAULT now() COMMENT '数据创建时间',
    `update_time` DateTime DEFAULT now() COMMENT '数据更新时间',
    `add_breakdown_id` String COMMENT '新增故障id'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(data_time)
PRIMARY KEY (data_time, amr_code, amr_type, happen_time, project_code)
ORDER BY (data_time, amr_code, amr_type, happen_time, project_code);


CREATE TABLE ads.ads_phx_amr_breakdown ON CLUSTER quicktron_31 AS ads.local_ads_phx_amr_breakdown
    ENGINE = Distributed(quicktron_31, ads, local_ads_phx_amr_breakdown, halfMD5(data_time));
   


-- 表4：local_ads_phx_lite_amr_breakdown

DROP table ads.ads_phx_lite_amr_breakdown ON CLUSTER quicktron_31;
DROP table ads.local_ads_phx_lite_amr_breakdown ON CLUSTER quicktron_31;



CREATE TABLE ads.local_ads_phx_lite_amr_breakdown  ON CLUSTER quicktron_31
(
    `data_time` DateTime COMMENT '数据产生时间（业务无关）',
    `breakdown_id` String COMMENT '错误次数',
    `amr_code` String COMMENT '机器人编码',
    `amr_type` String COMMENT '机器人类型',
    `carry_order_num` Int32 COMMENT '搬运作业单数',
    `right_order_num` Int32 COMMENT '正常搬运作业单数',
    `amr_task` Int32 COMMENT '机器人任务数',
    `total_charge` Int32 COMMENT '充电总次数',
    `exc_charge` Int32 COMMENT '充电异常次数',
    `error_duration` Float64 COMMENT '故障时长',
    `mttr_error_duration` Float64 COMMENT 'mttr故障时长',
    `mttr_error_num` Int32 COMMENT 'mttr故障次数',
    `start_time` String COMMENT '运行时段-开始时间',
    `end_time` String COMMENT '运行时段-结束时间',
    `actual_duration` Float64 COMMENT '实际运行时段',
    `project_code` String COMMENT '项目编码',
    `happen_time` String COMMENT '数据实际产生时间',
    `create_time` DateTime DEFAULT now() COMMENT '数据创建时间',
    `update_time` DateTime DEFAULT now() COMMENT '数据更新时间',
    `add_breakdown_id` String COMMENT '新增故障id'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(data_time)
PRIMARY KEY (data_time, project_code, amr_code, happen_time)
ORDER BY (data_time, project_code, amr_code, happen_time);


CREATE TABLE ads.ads_phx_lite_amr_breakdown ON CLUSTER quicktron_31 AS ads.local_ads_phx_lite_amr_breakdown
    ENGINE = Distributed(quicktron_31, ads, local_ads_phx_lite_amr_breakdown, halfMD5(data_time));   