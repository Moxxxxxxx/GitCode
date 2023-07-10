/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_station

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:36
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

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
-- Table structure for station
-- ----------------------------
DROP TABLE IF EXISTS `station`;
CREATE TABLE `station`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `primary_zone_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '所属主分区',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `station_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站类型',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务模式',
  `state` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'OFFLINE' COMMENT '状态',
  `accept_new` tinyint(1) NOT NULL DEFAULT 1 COMMENT '接单标记',
  `master` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否为主站',
  `master_station_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '主站ID',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站设备标识',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `logical` tinyint(1) NOT NULL DEFAULT 0,
  `station_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站名称',
  `current_operator` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '当前操作用户',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_stationCode`(`warehouse_id`, `station_code`) USING BTREE,
  UNIQUE INDEX `uidx_station_deviceCode`(`warehouse_id`, `device_code`) USING BTREE,
  INDEX `idx_station_state`(`warehouse_id`, `state`) USING BTREE,
  INDEX `idx_station_primaryZoneCode`(`warehouse_id`, `primary_zone_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 12 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_action_config
-- ----------------------------
DROP TABLE IF EXISTS `station_action_config`;
CREATE TABLE `station_action_config`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `page_id` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '页面Id',
  `page_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '页面名称',
  `action_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '操作ID',
  `action_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '操作名称',
  `visiable` tinyint(1) NOT NULL DEFAULT 1 COMMENT '设备编码',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_action_map`(`warehouse_id`, `page_id`, `action_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1451 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站按钮配置' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_biz_type_scope
-- ----------------------------
DROP TABLE IF EXISTS `station_biz_type_scope`;
CREATE TABLE `station_biz_type_scope`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务模式',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_biz_type_scope`(`warehouse_id`, `station_id`, `biz_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 744 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站与业务模式映射' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_entry
-- ----------------------------
DROP TABLE IF EXISTS `station_entry`;
CREATE TABLE `station_entry`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `station_point_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站停靠点',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'AGV编码',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架编码',
  `entry_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '进入时间',
  `exit_time` timestamp(3) DEFAULT NULL COMMENT '离开时间',
  `idempotent_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '幂等健',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_entry_bucket`(`warehouse_id`, `delete_flag`, `station_code`, `station_point_code`, `agv_code`, `bucket_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 13670 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站出入站记录' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_event
-- ----------------------------
DROP TABLE IF EXISTS `station_event`;
CREATE TABLE `station_event`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `event_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '事件类型',
  `event_content` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '事件正文',
  `processed` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否处理',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_system_event_processed`(`warehouse_id`, `processed`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5707 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站系统事件表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_login
-- ----------------------------
DROP TABLE IF EXISTS `station_login`;
CREATE TABLE `station_login`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务模式',
  `user_account` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '登录人',
  `login_time` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '登入时间',
  `logout_time` timestamp(3) DEFAULT NULL COMMENT '登出时间',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_login_stationCode`(`warehouse_id`, `delete_flag`, `station_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1784 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站登录记录' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_order_filter
-- ----------------------------
DROP TABLE IF EXISTS `station_order_filter`;
CREATE TABLE `station_order_filter`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) NOT NULL,
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业类型  PICK:拣货 REPLENISH:上架',
  `filter` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '筛选条件JSON',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `udx_station_id_type`(`warehouse_id`, `station_id`, `type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_point
-- ----------------------------
DROP TABLE IF EXISTS `station_point`;
CREATE TABLE `station_point`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '所属工作站',
  `point_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '停靠点编码',
  `point_short_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '简码',
  `ptl_code` int(11) DEFAULT NULL COMMENT '电子标签Code',
  `point_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '停靠点类型',
  `acceptable_job_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '可接收的任务类型',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_point_pointCode`(`warehouse_id`, `point_code`) USING BTREE,
  INDEX `idx_station_poin_stationId`(`warehouse_id`, `station_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 120 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站停靠点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_point_job_mapping
-- ----------------------------
DROP TABLE IF EXISTS `station_point_job_mapping`;
CREATE TABLE `station_point_job_mapping`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `station_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站类型',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务模式',
  `station_point_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '停靠点类型',
  `acceptable_job_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '可接收的任务类型',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_point_job_mapping_bizPointType`(`biz_type`, `station_point_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 55 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '停靠点任务映射关系' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_running_device
-- ----------------------------
DROP TABLE IF EXISTS `station_running_device`;
CREATE TABLE `station_running_device`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备编码',
  `operator` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '操作用户',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_running_device_deviceCode`(`warehouse_id`, `device_code`) USING BTREE,
  INDEX `idx_station_running_device_stationId`(`warehouse_id`, `station_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站当前运行设备' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_running_work
-- ----------------------------
DROP TABLE IF EXISTS `station_running_work`;
CREATE TABLE `station_running_work`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务模式',
  `order_id` bigint(20) DEFAULT NULL,
  `work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '设备编码',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_running_work_workId`(`warehouse_id`, `station_id`, `biz_type`, `work_id`, `device_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7854 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站当前运行作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_slot
-- ----------------------------
DROP TABLE IF EXISTS `station_slot`;
CREATE TABLE `station_slot`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '所属工作站',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '分播墙编码',
  `slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '槽位编码',
  `slot_short_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '槽位简码',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '优先级',
  `capacity` int(11) NOT NULL DEFAULT 1 COMMENT '槽位容量',
  `slot_number` int(11) DEFAULT 1,
  `back_ptl_code` int(11) DEFAULT NULL COMMENT '槽位背面电子标签',
  `slot_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '槽位类型',
  `container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器号',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `full_flag` tinyint(1) NOT NULL DEFAULT 0,
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_slot_stationid_slotcode`(`warehouse_id`, `station_id`, `slot_short_code`) USING BTREE,
  UNIQUE INDEX `uidx_station_slot_slotcode`(`warehouse_id`, `slot_code`) USING BTREE,
  UNIQUE INDEX `uidx_station_slot_containerCode`(`warehouse_id`, `container_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 495 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站槽位' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for station_slot_binding
-- ----------------------------
DROP TABLE IF EXISTS `station_slot_binding`;
CREATE TABLE `station_slot_binding`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_slot_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站槽位Code',
  `work_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '单据类型',
  `work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `order_id` bigint(20) NOT NULL COMMENT '业务单ID',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_slot_binding_slot_work`(`warehouse_id`, `station_slot_code`, `work_type`, `work_id`) USING BTREE,
  INDEX `idx_station_slot_binding_workId`(`warehouse_id`, `work_type`, `work_id`) USING BTREE,
  INDEX `idx_station_slot_binding_orderId`(`warehouse_id`, `work_type`, `order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4443 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站槽位绑定' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_slot_binding_history
-- ----------------------------
DROP TABLE IF EXISTS `station_slot_binding_history`;
CREATE TABLE `station_slot_binding_history`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_slot_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站槽位Code',
  `work_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '单据类型',
  `work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `order_id` bigint(20) NOT NULL COMMENT '业务单ID',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_slot_binding_history_workId`(`warehouse_id`, `work_type`, `work_id`) USING BTREE,
  INDEX `idx_station_slot_binding_history_orderId`(`warehouse_id`, `work_type`, `order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8179 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站槽位绑定历史表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_slot_sku
-- ----------------------------
DROP TABLE IF EXISTS `station_slot_sku`;
CREATE TABLE `station_slot_sku`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_slot_id` bigint(20) NOT NULL COMMENT '工作站槽位ID',
  `work_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单类型',
  `work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `work_detail_id` bigint(20) NOT NULL COMMENT '作业单明细ID',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `lot_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '批次ID',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  `pack_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '包装规格ID',
  `job_id` bigint(20) NOT NULL COMMENT 'WCS任务ID',
  `quantity` int(11) NOT NULL COMMENT '商品数量',
  `container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器编号',
  `packed_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '封箱标记',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_station_slot_sku_stationSlotId`(`warehouse_id`, `station_slot_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25531 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站槽位商品' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_task
-- ----------------------------
DROP TABLE IF EXISTS `station_task`;
CREATE TABLE `station_task`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_no` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务编号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `task_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务类型',
  `task_level` int(11) NOT NULL DEFAULT 0 COMMENT '任务优先级',
  `hierarchy` tinyint(3) NOT NULL DEFAULT 1 COMMENT '任务嵌套层级',
  `parent_task_id` bigint(20) DEFAULT NULL COMMENT '上级任务ID',
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `station_point_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站停靠点',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'AGV编码',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_face` int(4) DEFAULT NULL COMMENT '货架面',
  `work_id` bigint(20) DEFAULT NULL COMMENT '作业单ID',
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `channel` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '渠道',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_task_taskNo`(`warehouse_id`, `task_no`) USING BTREE,
  INDEX `idx_station_task_taskid`(`warehouse_id`, `task_no`) USING BTREE,
  INDEX `idx_station_task_stationid`(`warehouse_id`, `station_id`, `state`, `channel`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 28253 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_task_message
-- ----------------------------
DROP TABLE IF EXISTS `station_task_message`;
CREATE TABLE `station_task_message`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_task_id` bigint(20) NOT NULL COMMENT '任务ID',
  `message_type` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '消息类型',
  `message` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '消息正文',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_task_message_taskid`(`warehouse_id`, `station_task_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 56505 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站任务数据' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_task_state_change
-- ----------------------------
DROP TABLE IF EXISTS `station_task_state_change`;
CREATE TABLE `station_task_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_task_id` bigint(20) NOT NULL COMMENT '任务ID',
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_task_state_change_taskid`(`warehouse_id`, `station_task_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 85284 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站任务状态变更' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_zone
-- ----------------------------
DROP TABLE IF EXISTS `station_zone`;
CREATE TABLE `station_zone`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `station_id` bigint(20) NOT NULL COMMENT '工作站ID',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '分区编码',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_station_zone_stationZone`(`warehouse_id`, `station_id`, `zone_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 164 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站分区映射' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
