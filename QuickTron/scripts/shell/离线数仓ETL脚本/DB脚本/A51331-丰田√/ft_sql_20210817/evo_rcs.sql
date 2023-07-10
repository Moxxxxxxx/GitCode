/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_rcs

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:27
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for agv_charger_bind
-- ----------------------------
DROP TABLE IF EXISTS `agv_charger_bind`;
CREATE TABLE `agv_charger_bind`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `charger_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `charger_port_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `charger_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `charging_agv` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `reserved_agv` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `stop_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_charger_code`(`charger_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job
-- ----------------------------
DROP TABLE IF EXISTS `agv_job`;
CREATE TABLE `agv_job`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `action_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `action_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `bucket_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `can_interrupt` bit(1) DEFAULT NULL,
  `dest_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `is_let_down` bit(1) DEFAULT NULL,
  `is_report_event` bit(1) DEFAULT NULL,
  `job_context` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_mark` bit(1) DEFAULT NULL,
  `job_priority` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `own_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `src_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `top_face_list` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL,
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 23545 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job_cache
-- ----------------------------
DROP TABLE IF EXISTS `agv_job_cache`;
CREATE TABLE `agv_job_cache`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `action_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `action_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `bucket_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `can_interrupt` bit(1) DEFAULT NULL,
  `dest_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `is_let_down` bit(1) DEFAULT NULL,
  `is_report_event` bit(1) DEFAULT NULL,
  `job_context` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_mark` bit(1) DEFAULT NULL,
  `job_priority` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `own_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `src_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `top_face_list` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL,
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 17877 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job_command_record
-- ----------------------------
DROP TABLE IF EXISTS `agv_job_command_record`;
CREATE TABLE `agv_job_command_record`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `cmd_final_content` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `cmd_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `cmd_raw_content` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `cmd_response` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `cmd_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `is_cmd_sent` bit(1) NOT NULL,
  `is_sent_success` bit(1) NOT NULL,
  `op_content` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `op_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `op_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `sub_job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_create_time`(`create_time`) USING BTREE,
  INDEX `idx_sub_job_id`(`sub_job_id`) USING BTREE,
  INDEX `idx_op_id`(`op_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job_event_notification
-- ----------------------------
DROP TABLE IF EXISTS `agv_job_event_notification`;
CREATE TABLE `agv_job_event_notification`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `event_content` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `event_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `is_sent_success` bit(1) DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `message_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_create_time`(`create_time`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_message_id`(`message_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 115288 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job_history
-- ----------------------------
DROP TABLE IF EXISTS `agv_job_history`;
CREATE TABLE `agv_job_history`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `action_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `action_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `bucket_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `can_interrupt` bit(1) DEFAULT NULL,
  `dest_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `is_let_down` bit(1) DEFAULT NULL,
  `is_report_event` bit(1) DEFAULT NULL,
  `job_context` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_mark` bit(1) DEFAULT NULL,
  `job_priority` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `own_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `src_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `top_face_list` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL,
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_create_time`(`create_time`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 23754 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_job_sub
-- ----------------------------
DROP TABLE IF EXISTS `agv_job_sub`;
CREATE TABLE `agv_job_sub`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `class_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `content` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `is_last` bit(1) NOT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `sequence` int(11) NOT NULL,
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `sub_job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `update_by` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_create_time`(`create_time`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_sub_job_id`(`sub_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 80181 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_pd_status
-- ----------------------------
DROP TABLE IF EXISTS `agv_pd_status`;
CREATE TABLE `agv_pd_status`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_mac_address` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ap_mac_address` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ap_radio_id` int(11) DEFAULT NULL,
  `ap_service_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `battery_temperature` int(11) DEFAULT NULL,
  `bucket_heading` int(11) DEFAULT NULL,
  `bucket_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `direction` double DEFAULT NULL,
  `disk_space_percent` int(11) DEFAULT NULL,
  `exception_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ground_code_bias` int(11) DEFAULT NULL,
  `ground_decoded` int(11) DEFAULT NULL,
  `is_barrier` bit(1) DEFAULT NULL,
  `is_return_home` bit(1) DEFAULT NULL,
  `liftup_number` int(11) DEFAULT NULL,
  `load_mileage` int(11) DEFAULT NULL,
  `loading_bucket` int(11) DEFAULT NULL,
  `no_load_mileage` int(11) DEFAULT NULL,
  `over_all_mileage` int(11) DEFAULT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `power` float DEFAULT NULL,
  `robot_state` int(11) DEFAULT NULL,
  `signal_strength` int(11) DEFAULT NULL,
  `speed` int(11) DEFAULT NULL,
  `warehouse_id` bigint(20) DEFAULT NULL,
  `x` int(11) DEFAULT NULL,
  `y` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3454 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for agv_properties
-- ----------------------------
DROP TABLE IF EXISTS `agv_properties`;
CREATE TABLE `agv_properties`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_type_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_type_id` bigint(20) DEFAULT NULL,
  `dispaly_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_enum` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `unique_index_1`(`agv_type_id`, `name`, `agv_type_code`) USING BTREE,
  INDEX `index_1`(`agv_type_id`, `name`, `value`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_agv
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv`;
CREATE TABLE `basic_agv`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '小车编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `zone_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '机器人当前库区',
  `zone_collection` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '机器人作业库区集合',
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '小车类型',
  `agv_frame_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '车架号',
  `drive_unit_version` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ip` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '小车ip',
  `dsp_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'dsp版本',
  `battery_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `radar_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `camera_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '摄像头版本',
  `os` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作系统',
  `command_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '指令集版本',
  `product_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '产品线版本',
  `dbox_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'dbox版本',
  `iot_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'iot版本',
  `disk_space_percent` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '磁盘空间',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `bucket_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '虚拟货架编码',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_appearance
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_appearance`;
CREATE TABLE `basic_agv_appearance`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_appearance_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '机器人外形编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_appearance_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_part
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_part`;
CREATE TABLE `basic_agv_part`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_part_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人类型Id',
  `agv_part_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_part_layer` int(11) DEFAULT NULL COMMENT '部件层',
  `rotation_radius` int(11) DEFAULT NULL COMMENT '旋转半径',
  `offset_off_center_x` int(11) DEFAULT NULL COMMENT '距中心偏移X',
  `offset_off_center_y` int(11) DEFAULT NULL COMMENT '距中心偏移Y',
  `safe_length` int(11) DEFAULT NULL COMMENT '长安全距离',
  `length` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高',
  `safe_width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽安全距离',
  `safe_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高安全距离',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `roller_parts` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '小车配备的辊筒参数信息',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_type_id`, `agv_part_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 71 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_type
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_type`;
CREATE TABLE `basic_agv_type`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '类型编码',
  `agv_type_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '机器人名称',
  `agv_image` int(11) DEFAULT NULL COMMENT '机器人缩略图',
  `first_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级分类',
  `second_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级分类',
  `size_information` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '尺寸信息',
  `self_weight` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '自重',
  `specified_load` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '额定负载',
  `jacking_height` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '顶升高度',
  `no_load_rated_speed` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '空载额定速度',
  `full_load_rated_speed` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '满载额定速度',
  `navigation_method` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '导航方式',
  `positioning_accuracy` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '定位精度',
  `stop_accuracy` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '停止精度',
  `stop_angle_accuracy` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '停止角精度',
  `battery_type` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池类型',
  `battery_capacity` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池容量',
  `rated_battery_life` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '额定续航',
  `charging_time` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '充电时间',
  `battery_life` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池寿命',
  `ditch_capacity` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '过沟能力',
  `crossing_slope_capacity` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '过坡能力',
  `crossing_hom_capacity` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '过坎能力',
  `operating_temperature` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '使用温度',
  `noise` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '噪声',
  `charger_port_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电口类型',
  `walk_face` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行走面',
  `agv_camera_distance` int(11) NOT NULL COMMENT '车载摄像距离',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `slam_bucket_guide_front_wide_detection_added_value` int(11) DEFAULT NULL COMMENT 'slam货架引导--正面宽探测附加值',
  `slam_bucket_guide_deep_detection_added_value` int(11) DEFAULT NULL COMMENT 'slam货架引导--深度探测附加值',
  `speed` bigint(20) DEFAULT NULL COMMENT ' Robot速度，单位:mm/s',
  `acceleration` bigint(20) DEFAULT NULL COMMENT ' Robot加速度,单位:mm/s^2',
  `angular_speed` bigint(20) DEFAULT NULL COMMENT 'Robot角速度,单位:0.01度/s',
  `angular_acceleration` bigint(20) DEFAULT NULL COMMENT ' Robot角加速度,单位:0.01度/s^2',
  `reflector_guide_base_width` int(11) DEFAULT NULL COMMENT 'slam反光板引导--正面宽基础探测值',
  `reflector_guide_base_depth` int(11) DEFAULT NULL COMMENT 'slam反光板引导--正面深基础探测值',
  `camera_guide_base_width` int(11) DEFAULT NULL COMMENT 'slam相机板引导--正面宽基础探测值',
  `camera_guide_base_height` int(11) DEFAULT NULL COMMENT 'slam相机板引导--正面高基础探测值',
  `leave_guide_distance_off_main_line` int(11) DEFAULT NULL COMMENT '退出导引离开主线路？距离时生效',
  `vertical_distance_off_main_line` int(11) DEFAULT NULL COMMENT '垂足偏离 主线的最小距离  大于 去垂足 小于去下一个点',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `agv_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 48 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_ancillary_point
-- ----------------------------
DROP TABLE IF EXISTS `basic_ancillary_point`;
CREATE TABLE `basic_ancillary_point`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `ancillary_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '更改后的点条码',
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更换时的地图编码',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更换时的地图版本',
  `map_bar_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图上的点条码',
  `old_bar_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上一次更换的点条码',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_p_code`(`point_code`, `warehouse_id`) USING BTREE,
  UNIQUE INDEX `uk_a_code`(`warehouse_id`, `ancillary_point_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_area
-- ----------------------------
DROP TABLE IF EXISTS `basic_area`;
CREATE TABLE `basic_area`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `area_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区域编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `point_code` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码集合',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区ID',
  `area_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区域名称',
  `area_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区域类型',
  `super_area_id` bigint(20) DEFAULT NULL COMMENT '上级区域ID',
  `json_data` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'JSON数据',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'invalid' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'agv三级类型编码结合',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`area_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_auto_discharge_cargo
-- ----------------------------
DROP TABLE IF EXISTS `basic_auto_discharge_cargo`;
CREATE TABLE `basic_auto_discharge_cargo`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `equipment_id` bigint(20) NOT NULL COMMENT '目标设备',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码',
  `agv_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '机器人直接驶入点编码',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_automated_path
-- ----------------------------
DROP TABLE IF EXISTS `basic_automated_path`;
CREATE TABLE `basic_automated_path`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `automated_path_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '自动化路径编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `enabled` tinyint(1) NOT NULL COMMENT '启用标志',
  `start_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起始点编码',
  `start_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起点作业区分',
  `start_point_command` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起点指令集',
  `start_point_rollback_mode` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起点异常回滚策略',
  `start_point_equipment_id` bigint(20) DEFAULT NULL COMMENT '起点设备id',
  `end_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点点编码',
  `end_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '终点作业区分',
  `end_point_command` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '终点指令集',
  `end_point_rollback_mode` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '终点异常回滚策略',
  `end_point_equipment_id` bigint(20) DEFAULT NULL COMMENT '终点设备id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_charger
-- ----------------------------
DROP TABLE IF EXISTS `basic_charger`;
CREATE TABLE `basic_charger`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `charger_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电桩编码',
  `charger_port_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电口类型',
  `charger_mode` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '充电模式',
  `charger_type` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否启用',
  `map_code_and_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图版本和编号',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `ip` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '充电桩ip',
  `agv_heading` int(11) DEFAULT NULL COMMENT 'AGV进入充电桩相对方向',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `charger_code`, `map_code_and_version`) USING BTREE,
  INDEX `ix_code`(`warehouse_id`, `charger_code`, `state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_equipment
-- ----------------------------
DROP TABLE IF EXISTS `basic_equipment`;
CREATE TABLE `basic_equipment`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `equipment_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备编码',
  `equipment_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备区分',
  `equipment_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `enabled` tinyint(1) NOT NULL COMMENT '启用状态',
  `out_in_warehouse_mode` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出入库模式',
  `carrier_capacity` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '运载能力',
  `skip_command` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '跳转指令码集合',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`warehouse_id`, `equipment_code`, `equipment_classification`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_external_point
-- ----------------------------
DROP TABLE IF EXISTS `basic_external_point`;
CREATE TABLE `basic_external_point`  (
  `id` bigint(20) NOT NULL,
  `external_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '外设交互点关系码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库号',
  `enabled` tinyint(1) NOT NULL COMMENT '启用标志',
  `start_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起始点编码',
  `start_point_equipment_id` bigint(20) DEFAULT NULL COMMENT '起始设备id',
  `start_point_is_interactive` tinyint(1) NOT NULL COMMENT '起点是否交互',
  `start_point_is_parking` tinyint(1) NOT NULL COMMENT '起点是否停车',
  `end_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点编码',
  `end_point_equipment_id` bigint(20) DEFAULT NULL COMMENT '终点设备id',
  `end_point_is_interactive` tinyint(1) NOT NULL COMMENT '终点是否交互',
  `end_point_is_parking` tinyint(1) NOT NULL COMMENT '终点是否停车',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`warehouse_id`, `external_point_code`) USING BTREE
) ENGINE = MyISAM CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_map
-- ----------------------------
DROP TABLE IF EXISTS `basic_map`;
CREATE TABLE `basic_map`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `map_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图名称',
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图编号',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图版本',
  `base_map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '原地图版本',
  `file_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '文件名称',
  `map_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图上线状态',
  `json_data` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'JSON数据',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `map_code`, `map_version`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 67 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '地图表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_module_confirm
-- ----------------------------
DROP TABLE IF EXISTS `basic_module_confirm`;
CREATE TABLE `basic_module_confirm`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '更换时的地图编码',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '更换时的地图版本',
  `map_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `wes_result` tinyint(1) DEFAULT NULL COMMENT 'wes确认结果(0为失败，1为成功，下同)',
  `wcs_result` tinyint(1) DEFAULT NULL COMMENT 'wcs确认结果',
  `rcs_result` tinyint(1) DEFAULT NULL COMMENT 'rcs确认结果',
  `confirm_result` tinyint(1) DEFAULT NULL COMMENT '最终确认结果',
  `message` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `result` tinyint(1) DEFAULT NULL COMMENT '地图下线结果',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_operation_scope
-- ----------------------------
DROP TABLE IF EXISTS `basic_operation_scope`;
CREATE TABLE `basic_operation_scope`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `operation_scope_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业范围编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `zone_collection` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区集合',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `operation_scope_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_relationship
-- ----------------------------
DROP TABLE IF EXISTS `basic_relationship`;
CREATE TABLE `basic_relationship`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `table_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '表名',
  `relational_table` varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '相关表',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用名称',
  `created_time` datetime(3) NOT NULL COMMENT '创建时间',
  `last_updated_user` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新APP',
  `last_updated_time` datetime(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_reShip_code`(`table_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 16 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_roller_part
-- ----------------------------
DROP TABLE IF EXISTS `basic_roller_part`;
CREATE TABLE `basic_roller_part`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(6) DEFAULT NULL,
  `created_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `delta_x` int(11) DEFAULT NULL,
  `delta_y` int(11) DEFAULT NULL,
  `install_angle` int(11) DEFAULT NULL,
  `last_updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(6) DEFAULT NULL,
  `last_updated_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `roller_layer` int(11) DEFAULT NULL,
  `roller_no` int(11) DEFAULT NULL,
  `roller_team` int(11) DEFAULT NULL,
  `work_direction` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_slot_pos_relation
-- ----------------------------
DROP TABLE IF EXISTS `basic_slot_pos_relation`;
CREATE TABLE `basic_slot_pos_relation`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `area_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `bucket_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `bucket_layer` int(11) NOT NULL,
  `front_left_slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `front_right_slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `front_slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `left_slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `right_slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `slot_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `slot_digital_code` bigint(20) NOT NULL,
  `slot_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `unique_slot_code`(`slot_code`) USING BTREE,
  UNIQUE INDEX `unique_slot_digital_code`(`slot_digital_code`) USING BTREE,
  INDEX `idx_area_code`(`area_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_transport_entity
-- ----------------------------
DROP TABLE IF EXISTS `basic_transport_entity`;
CREATE TABLE `basic_transport_entity`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区ID',
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人类型ID',
  `transport_entity` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '搬送对象区分',
  `transport_entity_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '搬送对象类型Id',
  `checkout_bar_code_flag` tinyint(1) UNSIGNED NOT NULL COMMENT '是否检验条码',
  `support_transfer_job_to_rbt` tinyint(1) NOT NULL DEFAULT 0,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`zone_id`, `agv_type_id`, `transport_entity`, `transport_entity_type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for dsp_system_config
-- ----------------------------
DROP TABLE IF EXISTS `dsp_system_config`;
CREATE TABLE `dsp_system_config`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `config_group` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'GLOBAL',
  `warehouse_id` bigint(20) UNSIGNED DEFAULT NULL,
  `group_instance_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `os` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `hierarchy` int(2) NOT NULL DEFAULT 1,
  `directory` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `sequence` int(11) NOT NULL DEFAULT 1 COMMENT '功能模块下的展示顺序',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `display_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_enum` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `non_real_time` tinyint(1) DEFAULT NULL COMMENT '非实时标记',
  `remark` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '该配置的说明备注',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_system_config_createdDate`(`created_time`) USING BTREE,
  INDEX `ix_system_config_lastUpdatedDate`(`last_updated_time`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 28312 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for flyway_schema_history
-- ----------------------------
DROP TABLE IF EXISTS `flyway_schema_history`;
CREATE TABLE `flyway_schema_history`  (
  `installed_rank` int(11) NOT NULL,
  `version` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `description` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `script` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `checksum` int(11) DEFAULT NULL,
  `installed_by` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `installed_on` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `execution_time` int(11) NOT NULL,
  `success` tinyint(1) NOT NULL,
  PRIMARY KEY (`installed_rank`) USING BTREE,
  INDEX `flyway_schema_history_s_idx`(`success`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_bucket
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_bucket`;
CREATE TABLE `rcs_agv_bucket`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_modified` datetime(0) DEFAULT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `hard_bind_bucket` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `soft_bind_bucket` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_4knl8f00gbtsvaug3cln210h7`(`agv_id`) USING BTREE,
  INDEX `uk_agv_id`(`agv_id`) USING BTREE,
  INDEX `idx_agv_id`(`agv_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 132 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_error_dict
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_error_dict`;
CREATE TABLE `rcs_agv_error_dict`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `error_domain` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `content` varchar(2047) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error_level` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `solution` varchar(2047) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`error_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 41 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_exception
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_exception`;
CREATE TABLE `rcs_agv_exception`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `exception` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_create` datetime(0) NOT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `gmt_modified` datetime(0) NOT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `mark` bit(1) NOT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `x` int(11) DEFAULT NULL,
  `y` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_innc93l9nll0ukhxjea2ns8ww`(`agv_id`) USING BTREE,
  INDEX `idx_agv_id`(`agv_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_multi_floor_move_job
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_multi_floor_move_job`;
CREATE TABLE `rcs_agv_multi_floor_move_job`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `update_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_job_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `create_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `cur_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `cur_zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `failed_reason` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `final_get_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `tar_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tar_zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `update_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_genc9p4oq9ndx4cv05xdybiy2`(`agv_job_id`) USING BTREE,
  INDEX `uk_agv_id`(`agv_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_parcel
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_parcel`;
CREATE TABLE `rcs_agv_parcel`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `belt_state` tinyint(4) DEFAULT NULL,
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_modified` datetime(0) DEFAULT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `parcels` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_agv_id`(`agv_id`) USING BTREE,
  INDEX `uk_agv_id`(`agv_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_parts
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_parts`;
CREATE TABLE `rcs_agv_parts`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `direction_down` int(11) DEFAULT NULL,
  `gmt_create` datetime(0) NOT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `gmt_modified` datetime(0) NOT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `direction_layer` int(11) DEFAULT NULL,
  `direction_left` int(11) DEFAULT NULL,
  `direction_right` int(11) DEFAULT NULL,
  `direction_rotate` int(11) DEFAULT NULL,
  `direction_up` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UKs0byys3589t6m7gyqs2i2xstw`(`agv_type`, `direction_layer`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 37 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_agv_path_plan
-- ----------------------------
DROP TABLE IF EXISTS `rcs_agv_path_plan`;
CREATE TABLE `rcs_agv_path_plan`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime(6) NOT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `desc_pos` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `direction_way_points` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `error_msg` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `is_last_push` bit(1) DEFAULT NULL,
  `is_success` bit(1) NOT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `next_points` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `special_map` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `start_pos` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `sub_job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `way_points` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_create_time`(`create_time`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_sub_job_id`(`sub_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 230219 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_cmd_list
-- ----------------------------
DROP TABLE IF EXISTS `rcs_cmd_list`;
CREATE TABLE `rcs_cmd_list`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `cmd_version` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '指令集版本号',
  `cmd_list` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '指令集文本',
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_modified` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_dsp_error_dict
-- ----------------------------
DROP TABLE IF EXISTS `rcs_dsp_error_dict`;
CREATE TABLE `rcs_dsp_error_dict`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `created_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_date` datetime(0) DEFAULT NULL,
  `level` tinyint(4) DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `error_level` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_nofxw9ynhpj2nxdp4w6d44q66`(`code`) USING BTREE,
  UNIQUE INDEX `UK_gu33tfw8mwur15xxkyex2fmis`(`name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_modbus_coils
-- ----------------------------
DROP TABLE IF EXISTS `rcs_modbus_coils`;
CREATE TABLE `rcs_modbus_coils`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '空满检测点位',
  `slave_ip` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'modbusTcp模块从站ip',
  `bit_pos` int(11) DEFAULT NULL COMMENT '线圈在从站中的地址',
  `gmt_create` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_scan_code_record
-- ----------------------------
DROP TABLE IF EXISTS `rcs_scan_code_record`;
CREATE TABLE `rcs_scan_code_record`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `barcode_decoded` int(11) NOT NULL,
  `barcode_in_map` int(11) NOT NULL,
  `bias_type` int(11) NOT NULL,
  `date_created` datetime(6) NOT NULL,
  `direction` int(11) NOT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `point_x` int(11) NOT NULL,
  `point_y` int(11) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_simu_boot_info
-- ----------------------------
DROP TABLE IF EXISTS `rcs_simu_boot_info`;
CREATE TABLE `rcs_simu_boot_info`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `charge_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `container_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `coordinate` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `direction` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `ip` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_end_point` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `power` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `speed` float NOT NULL,
  `system_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `work_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `lost_power` bit(1) DEFAULT NULL,
  `simu_boot_position` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'random',
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for rcs_storeforkbin_bucket
-- ----------------------------
DROP TABLE IF EXISTS `rcs_storeforkbin_bucket`;
CREATE TABLE `rcs_storeforkbin_bucket`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `agv_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_create` datetime(0) DEFAULT NULL,
  `gmt_create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `gmt_modified` datetime(0) DEFAULT NULL,
  `gmt_modified_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `self_carrying_bucket` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer6` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer7` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer8` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `layer9` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `UK_tdimj52v1bh1m2fn6n6hd8i3e`(`agv_id`) USING BTREE,
  INDEX `uk_agv_id`(`agv_id`) USING BTREE,
  INDEX `idx_agv_id`(`agv_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
