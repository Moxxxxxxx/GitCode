/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wcs_g2p

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:41
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for action_state_change
-- ----------------------------
DROP TABLE IF EXISTS `action_state_change`;
CREATE TABLE `action_state_change`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `action_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'action id',
  `action_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'action类型',
  `device_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '设备code',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'action状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_action_state_change_action_id`(`action_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'Action状态变更记录' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_agv_point
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_point`;
CREATE TABLE `basic_agv_point`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库Id',
  `slot_id` bigint(20) NOT NULL COMMENT '库位Id',
  `slot_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库位编码',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '类型编码',
  `point_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业点编码',
  `bar_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业点条码',
  `slot_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库位点编码',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`slot_id`, `agv_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '机器人作业点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_area
-- ----------------------------
DROP TABLE IF EXISTS `basic_area`;
CREATE TABLE `basic_area`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `area_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区域编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码集合',
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
  `last_updated_time` datetime(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`area_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_convey_detail
-- ----------------------------
DROP TABLE IF EXISTS `bucket_convey_detail`;
CREATE TABLE `bucket_convey_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架所需作业面',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度明细表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_convey_job
-- ----------------------------
DROP TABLE IF EXISTS `bucket_convey_job`;
CREATE TABLE `bucket_convey_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `detail_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '详单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架初始路点',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架目标路点',
  `bucket_face_num` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架所需作业面',
  `bucket_init_face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架当前正北面',
  `bucket_target_face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架目标正北面',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度job表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_convey_work
-- ----------------------------
DROP TABLE IF EXISTS `bucket_convey_work`;
CREATE TABLE `bucket_convey_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架入库单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度work表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_move_action
-- ----------------------------
DROP TABLE IF EXISTS `bucket_move_action`;
CREATE TABLE `bucket_move_action`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区code',
  `action_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `action_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'action类型',
  `biz_tag` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'action业务标记',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'action状态',
  `action_error` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'action失败原因',
  `device_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '设备编码',
  `lock_mark` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务执行完成的锁定类型,UNLOCK/AGV/BUCKET',
  `priority` int(11) NOT NULL COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `bucket_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架编码',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架点Code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架点Code',
  `top_face` tinyint(2) DEFAULT NULL COMMENT '货架朝上(北)货柜面',
  `target_face` tinyint(2) DEFAULT NULL COMMENT '目标作业面',
  `put_down` tinyint(1) NOT NULL DEFAULT 0 COMMENT '到目标点是否需要放下货架，0否，1是，默认为0',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_action_id`(`action_id`) USING BTREE,
  INDEX `idx_biz_tag`(`biz_tag`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_device_code`(`device_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架移位Action' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for bucket_move_job
-- ----------------------------
DROP TABLE IF EXISTS `bucket_move_job`;
CREATE TABLE `bucket_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `lift_group_id` bigint(64) DEFAULT NULL COMMENT '电梯任务组id',
  `busi_group_id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `left_group_id` bigint(64) DEFAULT NULL COMMENT '电梯任务组id',
  `floor` int(11) DEFAULT NULL COMMENT '楼层',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `source` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务来源',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'FORCE OR COMMON',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `bucket_move_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架移动类型',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `station_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '初始点位code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标点位code',
  `top_face` tinyint(16) DEFAULT NULL,
  `top_faces` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '多作业面',
  `sequence` int(11) DEFAULT NULL COMMENT '序号',
  `bucket_face_num` tinyint(2) DEFAULT NULL,
  `put_down` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否放下货架，0否，1是',
  `need_operation` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `agv_end_point` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `lock_flag` tinyint(16) DEFAULT NULL COMMENT '是否锁定agv',
  `bucket_type_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架类型编码',
  `check_code` tinyint(16) NOT NULL DEFAULT 1 COMMENT '是否校验编码',
  `stand_by_flag` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `flag` int(11) DEFAULT NULL COMMENT '标志位',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 25441 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架移动任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_point
-- ----------------------------
DROP TABLE IF EXISTS `bucket_point`;
CREATE TABLE `bucket_point`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库Id',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `original_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架所属点',
  `remark` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 86 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架对应点位表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_robot_job
-- ----------------------------
DROP TABLE IF EXISTS `bucket_robot_job`;
CREATE TABLE `bucket_robot_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库Id',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `robot_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上游业务指定的ID',
  `priority_type` tinyint(16) NOT NULL DEFAULT 0 COMMENT '优先级类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务状态',
  `source` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务来源',
  `work_mode` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '作业模式',
  `push_flag` tinyint(16) NOT NULL DEFAULT 0 COMMENT '推送标识',
  `bucket_slot_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编号',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位编码',
  `start_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架起始点',
  `work_face` tinyint(16) DEFAULT NULL,
  `work_faces` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '多作业面',
  `end_area` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标区域',
  `target_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标点',
  `agv_end_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv目标点',
  `put_down` tinyint(16) NOT NULL DEFAULT 1 COMMENT '是否放下agv,方向-1,不放下-0',
  `need_operation` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '是否需要实操',
  `need_reset` tinyint(16) DEFAULT NULL COMMENT '是否需要返库',
  `lock_flag` tinyint(16) DEFAULT NULL COMMENT '是否锁定agv',
  `bucket_type_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架类型编码',
  `need_out` tinyint(16) NOT NULL DEFAULT 0 COMMENT '是否出场',
  `check_code` tinyint(16) NOT NULL DEFAULT 1 COMMENT '是否校验编码',
  `stand_by_flag` tinyint(64) NOT NULL COMMENT 'stand_by',
  `job_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `dispatch_state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'NORMAL' COMMENT '调度状态',
  `remark` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv编码',
  `agv_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv类型',
  `business_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务类型',
  `device_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电梯编码',
  `cancel_strategy` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '取消策略',
  `deadline` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '截至时间',
  `busi_group_id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `robot_job_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务组id',
  `sequence` tinyint(64) DEFAULT NULL COMMENT '任务组序号',
  `flag` int(11) DEFAULT NULL COMMENT '按位与的一个标志字段',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `hds_group_id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'hds任务组ID',
  `bucket_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT ';分割的货架类型',
  `start_area` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起点区域',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_rjobid`(`robot_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '标准搬运任务' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for bucket_runtime
-- ----------------------------
DROP TABLE IF EXISTS `bucket_runtime`;
CREATE TABLE `bucket_runtime`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区code',
  `bucket_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架编码',
  `bucket_mobile_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架移动类型',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架状态',
  `current_job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '当前调度持有货架的业务job类型',
  `next_job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '预占后续调度持有货架的业务job类型',
  `logical_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架软绑定的点位',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_bucket_code`(`bucket_code`) USING BTREE,
  INDEX `idx_logical_point_code`(`logical_point_code`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'WCS维护的货架运行时信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for carry_task
-- ----------------------------
DROP TABLE IF EXISTS `carry_task`;
CREATE TABLE `carry_task`  (
  `id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `external_task_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `biz_type` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `priority` int(11) DEFAULT NULL,
  `target_type` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `target_code` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `target_dest_way_point_code` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `attachments` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `biz_ext` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `consume_state` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `process_state` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `create_time` datetime(0) NOT NULL,
  `update_time` datetime(0) NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_external_task_id`(`external_task_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for container_move_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `container_move_job_v2`;
CREATE TABLE `container_move_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器Code',
  `station_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `move_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器移位类型',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器移位业务类型',
  `lift_up` tinyint(2) DEFAULT NULL COMMENT '小车是否需要顶升',
  `put_down` tinyint(2) DEFAULT NULL COMMENT '小车到目标点是否需要降下',
  `source_put_down` tinyint(2) DEFAULT NULL COMMENT '小车移动起始点是否需要降下',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '原始货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '原始货位',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位',
  `source_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移位起始点',
  `target_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移位目标点',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_s_bucket_slot`(`source_bucket_slot_code`) USING BTREE,
  INDEX `idx_t_bucket_slot`(`target_bucket_slot_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '容器移位小朱雀搬运任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for container_take_down_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `container_take_down_job_v2`;
CREATE TABLE `container_take_down_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '容器Code',
  `station_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `station_stop_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点编码',
  `agv_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '小车code',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '空容器下架任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for container_transfer_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `container_transfer_job_v2`;
CREATE TABLE `container_transfer_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `move_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移动类型，出库还是入库',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器Code',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位',
  `source_road_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位对应作业点',
  `source_take_face` tinyint(2) DEFAULT NULL COMMENT '源货位对应作业方向',
  `source_take_height` int(64) DEFAULT NULL COMMENT '源货位对应作业高度',
  `target_put_face` tinyint(2) DEFAULT NULL COMMENT '目标货位对应作业方向',
  `target_put_height` int(64) DEFAULT NULL COMMENT '目标货位高度',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位',
  `target_road_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位对应作业点',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_source_bucket_code`(`source_bucket_code`) USING BTREE,
  INDEX `idx_target_bucket_code`(`target_bucket_code`) USING BTREE,
  INDEX `idx_s_bucket_slot`(`source_bucket_slot_code`) USING BTREE,
  INDEX `idx_t_bucket_slot`(`target_bucket_slot_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '容器转移料箱agv任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for countcheck_job
-- ----------------------------
DROP TABLE IF EXISTS `countcheck_job`;
CREATE TABLE `countcheck_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `detail_id` bigint(16) DEFAULT NULL COMMENT '详单ID',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '盘点调度单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架初始路点',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架目标路点',
  `bucket_face_num` tinyint(2) DEFAULT NULL COMMENT '货架所需作业面',
  `bucket_init_face` tinyint(2) DEFAULT NULL COMMENT '货架当前正北面',
  `bucket_target_face` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6546 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for countcheck_work
-- ----------------------------
DROP TABLE IF EXISTS `countcheck_work`;
CREATE TABLE `countcheck_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '盘点调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '盘点源单ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) DEFAULT NULL COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_work_id`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 106 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点申请单表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for countcheck_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `countcheck_work_detail`;
CREATE TABLE `countcheck_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `face` tinyint(2) DEFAULT NULL COMMENT '货架所需作业面',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6489 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点申请单明细表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for guided_put_away_job
-- ----------------------------
DROP TABLE IF EXISTS `guided_put_away_job`;
CREATE TABLE `guided_put_away_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) NOT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `quantity` int(11) DEFAULT NULL COMMENT '计划数量',
  `fullfill_quantity` int(11) DEFAULT NULL COMMENT '实捡数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺捡或多捡原因',
  `new_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '新货位Code（实操反馈）',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `bucket_slot_type_id` bigint(16) DEFAULT NULL COMMENT '货位类型ID',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架路点Code',
  `bucket_face_num` tinyint(2) DEFAULT NULL COMMENT '货架作业面',
  `target_face_num` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6007 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '推荐上架任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for guided_putaway_work
-- ----------------------------
DROP TABLE IF EXISTS `guided_putaway_work`;
CREATE TABLE `guided_putaway_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '推荐上架作业单号',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源单号',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `biz_class` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '业务分类',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '业务类型',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '存储类型，整存/散存',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '作业单优先级',
  `finished_date` datetime(3) DEFAULT NULL COMMENT '完成时间',
  `stop_date` datetime(3) DEFAULT NULL COMMENT '暂停时间',
  `cancel_date` datetime(3) DEFAULT NULL COMMENT '取消时间',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `opened` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0是FALSE,1为TRUE',
  `submit_times` int(4) NOT NULL DEFAULT 0 COMMENT '提交次数',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_work_id`(`work_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_biz_class`(`biz_class`) USING BTREE,
  INDEX `idx_biz_type`(`biz_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 312 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '推荐上架作业单主信息' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for guided_putaway_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `guided_putaway_work_detail`;
CREATE TABLE `guided_putaway_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单号',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) NOT NULL COMMENT 'SKU',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主',
  `lot_id` bigint(16) NOT NULL COMMENT '批次',
  `pack_id` bigint(16) NOT NULL COMMENT '包装规格',
  `original_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '原始计划上架数量',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '计划上架数量',
  `fulfill_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实际上架数量',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否冻结,0-否,1-是',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '工作站槽位三级库存ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_detail_id`(`detail_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5214 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '推荐上架作业单明细' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for hds_carry_task
-- ----------------------------
DROP TABLE IF EXISTS `hds_carry_task`;
CREATE TABLE `hds_carry_task`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `external_task_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运任务id',
  `biz_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `target_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运的目标类型',
  `target_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运的目标编码',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `target_face` tinyint(2) DEFAULT NULL COMMENT '货架所需作业面',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE COMMENT '每个job表的job_id都不会重复'
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货到人密集存储搬运任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for hds_job
-- ----------------------------
DROP TABLE IF EXISTS `hds_job`;
CREATE TABLE `hds_job`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `warehouse_id` int(11) DEFAULT NULL,
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `area_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `put_down` tinyint(1) DEFAULT NULL,
  `priority` int(11) DEFAULT NULL,
  `target_point` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `top_face` smallint(6) DEFAULT NULL,
  `priority_type` tinyint(1) DEFAULT NULL,
  `job_source_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_source_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `push_flag` tinyint(1) DEFAULT NULL,
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for hds_job_group
-- ----------------------------
DROP TABLE IF EXISTS `hds_job_group`;
CREATE TABLE `hds_job_group`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `warehouse_id` int(11) DEFAULT NULL,
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `priority` int(11) DEFAULT NULL,
  `area_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `top_face` int(11) DEFAULT NULL,
  `put_down` tinyint(1) DEFAULT NULL,
  `group_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `source_waypoint_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `target_waypoint_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for hds_reserve_point
-- ----------------------------
DROP TABLE IF EXISTS `hds_reserve_point`;
CREATE TABLE `hds_reserve_point`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `warehouse_id` int(11) NOT NULL,
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for heat_calculation_rules
-- ----------------------------
DROP TABLE IF EXISTS `heat_calculation_rules`;
CREATE TABLE `heat_calculation_rules`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库di',
  `heat_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '热度编码',
  `priority_order` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '计算顺序',
  `Proportion` bigint(64) DEFAULT NULL COMMENT '所占比例',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for heat_move_group
-- ----------------------------
DROP TABLE IF EXISTS `heat_move_group`;
CREATE TABLE `heat_move_group`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库di',
  `zone_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `task_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `group_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务组ID',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'task状态',
  `job_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for heat_move_job
-- ----------------------------
DROP TABLE IF EXISTS `heat_move_job`;
CREATE TABLE `heat_move_job`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库di',
  `zone_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `task_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务组ID',
  `group_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务组ID',
  `job_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务ID',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `start_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架起始点',
  `target_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标点',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'job状态',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv编码',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for heat_move_task
-- ----------------------------
DROP TABLE IF EXISTS `heat_move_task`;
CREATE TABLE `heat_move_task`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库di',
  `zone_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `task_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务组ID',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'task状态',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for idempotency_check
-- ----------------------------
DROP TABLE IF EXISTS `idempotency_check`;
CREATE TABLE `idempotency_check`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `idempotency_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '幂等性校验类型',
  `idempotency_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '幂等性校验值',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idempotency_unique`(`idempotency_code`, `idempotency_type`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1427269081316200451 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '幂等性校验公共信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for job_action
-- ----------------------------
DROP TABLE IF EXISTS `job_action`;
CREATE TABLE `job_action`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `action_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `action_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'action类型',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `job_priority` int(11) NOT NULL DEFAULT 0 COMMENT '任务优先级',
  `subscribe_event` bigint(16) NOT NULL DEFAULT 0 COMMENT 'job订阅的action事件标记位',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '当前记录是否有效的状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_action_id`(`action_id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'Action和job的映射关系' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for job_lifecycle_record
-- ----------------------------
DROP TABLE IF EXISTS `job_lifecycle_record`;
CREATE TABLE `job_lifecycle_record`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `create_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录日期时间',
  `robot_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '搬运任务id',
  `extend_ids` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '搬运任务扩展子任务属性id列表',
  `transfer_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '料箱车任务id',
  `move_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '小朱雀任务id',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '机器人编号',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '料箱编号',
  `record_level` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '记录等级',
  `description` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '记录描述',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '记录创建的代码点',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '任务生命周期记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for job_state_change
-- ----------------------------
DROP TABLE IF EXISTS `job_state_change`;
CREATE TABLE `job_state_change`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv code',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_job_state_change_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 586131 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '任务状态变更记录' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for lift_move_job
-- ----------------------------
DROP TABLE IF EXISTS `lift_move_job`;
CREATE TABLE `lift_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务Id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型,空车移位，在线搬运，离线搬运，电梯移动，电梯关门',
  `group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务组ID',
  `device_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备类型',
  `device_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备编码',
  `from_floor` decimal(11, 0) NOT NULL COMMENT '出发层',
  `target_floor` decimal(11, 0) NOT NULL COMMENT '到达层',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_device_code`(`device_code`, `state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '电梯任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for order_group
-- ----------------------------
DROP TABLE IF EXISTS `order_group`;
CREATE TABLE `order_group`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '订单组id',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '订单类型',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `take_effect` smallint(6) NOT NULL DEFAULT 0,
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单组状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `order_group_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_order_group_id`(`order_group_id`) USING BTREE,
  INDEX `idx_order_type`(`order_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5071 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '订单组信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_job
-- ----------------------------
DROP TABLE IF EXISTS `picking_job`;
CREATE TABLE `picking_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `picking_order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '拣货订单明细行ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `customer_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '三级库存ID',
  `quantity` int(11) DEFAULT NULL COMMENT '应拣数量',
  `actual_quantity` int(11) DEFAULT NULL COMMENT '实拣数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺拣原因',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架路点Code',
  `bucket_face_num` tinyint(2) DEFAULT NULL COMMENT '货架作业面',
  `target_face_num` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站槽位Code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `job_mode` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'job调度方式',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库单类型',
  `order_group_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '集合单类型',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_orderid`(`order_id`) USING BTREE,
  INDEX `idx_group_id`(`picking_order_group_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 27272 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拣货任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for picking_work
-- ----------------------------
DROP TABLE IF EXISTS `picking_work`;
CREATE TABLE `picking_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单类型',
  `picking_order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单ID',
  `wave_order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单类型',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站编号',
  `priority_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `priority_value` int(11) NOT NULL DEFAULT 0 COMMENT '优先级数值',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站槽位编号',
  `ship_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '截止发货时间',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定货架编号',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定货位编号',
  `work_station_match_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单工作站分配类型',
  `cross_zone` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否跨库区，0否，1是',
  `picking_available_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否有货先发，0否，1是',
  `match_station_field1` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段1',
  `match_station_field2` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段2',
  `match_station_field3` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段3',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `owner_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '平台货主',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE,
  INDEX `idx_orderid`(`order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7044 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拣货作业单明细表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for picking_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `picking_work_detail`;
CREATE TABLE `picking_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单明细ID',
  `picking_order_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '拣货订单明细行ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单ID',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '指定的三级库存ID',
  `sku_id` bigint(16) NOT NULL DEFAULT 0 COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT 0 COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '应拣数量',
  `fulfill_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实拣数量',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workdetailid`(`picking_work_detail_id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 26307 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拣货作业单明细表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for putaway_job
-- ----------------------------
DROP TABLE IF EXISTS `putaway_job`;
CREATE TABLE `putaway_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `detail_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '详单ID',
  `put_away_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架初始路点',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架目标路点',
  `bucket_face_num` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架所需作业面',
  `bucket_init_face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架当前正北面',
  `bucket_target_face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架目标正北面',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4438 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for putaway_work
-- ----------------------------
DROP TABLE IF EXISTS `putaway_work`;
CREATE TABLE `putaway_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架入库单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `buckets` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '所需货架以及面的集合',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 542 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架申请单表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for putaway_work_copy
-- ----------------------------
DROP TABLE IF EXISTS `putaway_work_copy`;
CREATE TABLE `putaway_work_copy`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架入库单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `buckets` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '所需货架以及面的集合',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) DEFAULT NULL COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架申请单表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for putaway_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `putaway_work_detail`;
CREATE TABLE `putaway_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架所需作业面',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4319 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架申请单明细表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for reprint_move_job
-- ----------------------------
DROP TABLE IF EXISTS `reprint_move_job`;
CREATE TABLE `reprint_move_job`  (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'bucket_robot_job表里的job_id',
  `warehouse_id` bigint(11) NOT NULL COMMENT '仓库id',
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `job_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务状态',
  `point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上料点或者下料点',
  `sub_job_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '子任务id',
  `lock_flag` tinyint(1) UNSIGNED ZEROFILL NOT NULL DEFAULT 0 COMMENT '是否锁定agv',
  `equipment_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '设备号',
  `interactive` tinyint(16) NOT NULL DEFAULT 0 COMMENT '是否交互 true：是 false：否',
  `sequence` int(5) UNSIGNED NOT NULL DEFAULT 0 COMMENT '序号',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv编码',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv类型',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `container_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器编号',
  `work_face` int(3) DEFAULT NULL COMMENT '工作面',
  `agv_end_point` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv投递后去的点',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '翻版车任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for robot_job_detail
-- ----------------------------
DROP TABLE IF EXISTS `robot_job_detail`;
CREATE TABLE `robot_job_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库Id',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `robot_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上游业务指定的ID',
  `job_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务状态',
  `des` text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '状态描述',
  `reason` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '原因',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv编码',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ux`(`job_id`, `state`, `reason`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '任务细节' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for roller_assign_record
-- ----------------------------
DROP TABLE IF EXISTS `roller_assign_record`;
CREATE TABLE `roller_assign_record`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区域',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv code',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '辊筒类型',
  `agv_capacity` int(11) DEFAULT NULL COMMENT '辊筒运力',
  `scroll_direction` int(11) DEFAULT NULL COMMENT '滚动方向',
  `point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '分配上料点',
  `load_nums` int(11) DEFAULT NULL COMMENT '上料点上料数量',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `created_date` datetime(0) DEFAULT NULL COMMENT '创建时间',
  `updated_date` datetime(0) DEFAULT NULL COMMENT '修改时间',
  `loaded_nums` int(16) DEFAULT NULL COMMENT '已经完成上料的任务数量',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_point_code`(`point_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '辊筒分车记录' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for roller_job_extends
-- ----------------------------
DROP TABLE IF EXISTS `roller_job_extends`;
CREATE TABLE `roller_job_extends`  (
  `job_id` bigint(16) NOT NULL COMMENT '作业单id',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器编码',
  `auto_load` tinyint(1) DEFAULT NULL COMMENT '上料方式 0-人工上料 1-自动上料',
  `auto_unload` tinyint(1) DEFAULT NULL COMMENT '下料方式 人工下料 1-自动下料',
  `auto_load_type` tinyint(2) DEFAULT NULL COMMENT '自动上料方式 1-光电上料 0-接口上料',
  `auto_unload_type` tinyint(2) DEFAULT NULL COMMENT '自动下料方式 1-光电上料 0-接口上料',
  `load_equipment_id` bigint(16) DEFAULT NULL COMMENT '上料上游设备id',
  `unload_equipment_id` bigint(16) DEFAULT NULL COMMENT '下料下游设备id',
  `load_interactive` tinyint(1) DEFAULT NULL COMMENT '记录上料时是否需要与上游交互',
  PRIMARY KEY (`job_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '辊筒作业单扩展字段' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for roller_move_job
-- ----------------------------
DROP TABLE IF EXISTS `roller_move_job`;
CREATE TABLE `roller_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库号',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区code',
  `busi_group_id` bigint(16) DEFAULT NULL,
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv类型',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'FORCE or COMMON',
  `priority` int(11) DEFAULT NULL COMMENT '任务优先级',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起始点位',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标点位',
  `work_face` int(11) DEFAULT NULL COMMENT '工作面-小车移动后的方向',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `created_date` datetime(0) DEFAULT NULL COMMENT '创建时间',
  `updated_date` datetime(0) DEFAULT NULL COMMENT '更新时间',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'unknown' COMMENT '创建入口',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'unknown' COMMENT '最后修改入口',
  `pre_id` bigint(32) DEFAULT NULL COMMENT '上一个任务的主键id',
  `job_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '记录辊筒作业任务的料口正对的点',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `unq_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '辊筒车任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for roller_sub_job
-- ----------------------------
DROP TABLE IF EXISTS `roller_sub_job`;
CREATE TABLE `roller_sub_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  `move_job_id` bigint(16) DEFAULT NULL COMMENT '关联move_job',
  `job_id` bigint(16) NOT NULL COMMENT '关联robot_job',
  `sub_job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '子任务类型 LOAD UNLOAD WAITING',
  `point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `scroll_direction` int(11) DEFAULT NULL COMMENT '滚动方向',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器编码',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `auto_feed` tinyint(1) DEFAULT NULL COMMENT '上下料方式 0-人工 1-自动',
  `feed_type` tinyint(1) DEFAULT NULL COMMENT '自动上料交互方式 0-光电 1-接口',
  `roller_loc_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '记录任务与辊筒盘的绑定关系',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_move_job_id`(`move_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '滚筒车上下料子任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for si_qp_extend
-- ----------------------------
DROP TABLE IF EXISTS `si_qp_extend`;
CREATE TABLE `si_qp_extend`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位',
  `source_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位对应作业点',
  `source_take_face` tinyint(2) DEFAULT NULL COMMENT '源货位对应作业方向',
  `source_take_height` int(64) DEFAULT NULL COMMENT '源货位对应作业高度',
  `target_put_face` tinyint(2) DEFAULT NULL COMMENT '目标货位对应作业方向',
  `target_put_height` int(64) DEFAULT NULL COMMENT '目标货位高度',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位',
  `target_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位对应作业点',
  `target_area` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业目标区域',
  `transfer_job_id` bigint(16) DEFAULT NULL COMMENT '容器转移任务id',
  `move_job_id` bigint(16) DEFAULT NULL COMMENT '容器搬运任务id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_transfer_job_id`(`transfer_job_id`) USING BTREE,
  INDEX `idx_move_job_id`(`move_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'QuickPick智能搬运任务扩展表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for si_qp_move_job
-- ----------------------------
DROP TABLE IF EXISTS `si_qp_move_job`;
CREATE TABLE `si_qp_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器Code',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器移位业务类型',
  `move_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器移位类型',
  `source_height` int(64) DEFAULT NULL COMMENT '起始位置需要的高度',
  `target_height` int(64) DEFAULT NULL COMMENT '目标位置需要的高度',
  `target_area` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '作业目标区域',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '原始货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '原始货位',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位',
  `source_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '移位起始点',
  `target_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '移位目标点',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_s_bucket_slot_code`(`source_bucket_slot_code`) USING BTREE,
  INDEX `idx_t_bucket_slot_code`(`target_bucket_slot_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'QuickPick智能搬运容器移位小朱雀搬运任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for si_qp_transfer_job
-- ----------------------------
DROP TABLE IF EXISTS `si_qp_transfer_job`;
CREATE TABLE `si_qp_transfer_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `move_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移动类型，出库还是入库',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器Code',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货位',
  `source_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货位对应作业点',
  `source_take_face` tinyint(2) DEFAULT NULL COMMENT '源货位对应作业方向',
  `source_take_height` int(64) DEFAULT NULL COMMENT '源货位对应作业高度',
  `target_put_face` tinyint(2) DEFAULT NULL COMMENT '目标货位对应作业方向',
  `target_put_height` int(64) DEFAULT NULL COMMENT '目标货位高度',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位',
  `target_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位对应作业点',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_s_bucket_slot_code`(`source_bucket_slot_code`) USING BTREE,
  INDEX `idx_t_bucket_slot_code`(`target_bucket_slot_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'QuickPick智能搬运转移料箱agv任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for smallparcel_move_job
-- ----------------------------
DROP TABLE IF EXISTS `smallparcel_move_job`;
CREATE TABLE `smallparcel_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `bucket_move_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架移动类型',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `book_num` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '图书编号',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '初始点位code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标点位code',
  `top_face` tinyint(2) NOT NULL DEFAULT 1 COMMENT '货架目标正北面',
  `put_down` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否放下货架，0否，1是',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '小皮带投递任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for station_confirm_task
-- ----------------------------
DROP TABLE IF EXISTS `station_confirm_task`;
CREATE TABLE `station_confirm_task`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'zoneCode',
  `task_no` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '确认任务实操taskNo',
  `work_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业类型',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_face` tinyint(2) DEFAULT NULL COMMENT '货架作业面',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `state` varchar(24) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'DONE' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_task_no`(`task_no`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '在线工作站货架离站确认任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for station_task_group
-- ----------------------------
DROP TABLE IF EXISTS `station_task_group`;
CREATE TABLE `station_task_group`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `group_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务组ID',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务ID',
  `job_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_group_id`(`group_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 27349 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '工作站任务和job关系表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for tally_picking_job
-- ----------------------------
DROP TABLE IF EXISTS `tally_picking_job`;
CREATE TABLE `tally_picking_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `tally_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `tally_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `customer_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '三级库存ID',
  `quantity` int(11) DEFAULT NULL COMMENT '应拣数量',
  `actual_quantity` int(11) DEFAULT NULL COMMENT '实拣数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺拣原因',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架路点Code',
  `bucket_face_num` tinyint(2) DEFAULT NULL COMMENT '货架作业面',
  `target_face_num` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `stop_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货下架任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_putaway_job
-- ----------------------------
DROP TABLE IF EXISTS `tally_putaway_job`;
CREATE TABLE `tally_putaway_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `quantity` int(11) DEFAULT NULL COMMENT '计划数量',
  `fullfill_quantity` int(11) DEFAULT NULL COMMENT '实上数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '少上或超上原因',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `bucket_slot_type_id` bigint(16) DEFAULT NULL COMMENT '货位类型ID',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架路点Code',
  `bucket_face_num` tinyint(2) DEFAULT NULL COMMENT '货架作业面',
  `target_face_num` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `bucket_move_job_id` bigint(16) DEFAULT NULL COMMENT '搬运任务ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货上架任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work
-- ----------------------------
DROP TABLE IF EXISTS `tally_work`;
CREATE TABLE `tally_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(64) NOT NULL DEFAULT 0 COMMENT '仓库id',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '库区编码',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '理货作业单id',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '理货作业单id',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业类型理货',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '在线/离线',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `tally_apply_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '申请单ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_work_detail`;
CREATE TABLE `tally_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(64) NOT NULL DEFAULT 0 COMMENT '仓库id',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '库区编码',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '订单',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '理货作业单id',
  `work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业类型理货',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `sku_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'skuid',
  `lot_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次id',
  `pack_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '包装规格id',
  `frozen` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否冻结',
  `pack_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '包装整/散',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架编码',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位编码',
  `l3_inventory_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '三级库存id',
  `quantity` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '应捡数量',
  `act_quantity` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '实捡数量',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细状态',
  `stage` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '下架/上架阶段',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `id_from_wes` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'wes的明细id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work_detail_relation
-- ----------------------------
DROP TABLE IF EXISTS `tally_work_detail_relation`;
CREATE TABLE `tally_work_detail_relation`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(64) NOT NULL COMMENT '仓库id',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货作业单下架明细id',
  `putaway_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货作业单上架明细id',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `sku_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'skuid',
  `lot_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次id',
  `pack_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '包装规格id',
  `frozen` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否冻结',
  `pack_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '包装整/散',
  `l3_inventory_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '三级库存id',
  `quantity` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '应捡数量',
  `act_quantity` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '实捡数量',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单明细关系' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for validate_bucket_job
-- ----------------------------
DROP TABLE IF EXISTS `validate_bucket_job`;
CREATE TABLE `validate_bucket_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `sequence` bigint(16) DEFAULT NULL COMMENT '序列号',
  `task_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务组id',
  `validate_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '验证类型',
  `binding_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '绑定类型',
  `point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '验证点位',
  `x_point` bigint(16) DEFAULT NULL COMMENT 'x坐标',
  `y_point` bigint(16) DEFAULT NULL COMMENT 'y坐标',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架验证任务' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for validate_bucket_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `validate_bucket_job_v2`;
CREATE TABLE `validate_bucket_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `sequence` bigint(16) DEFAULT NULL COMMENT '序列号',
  `task_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务组id',
  `validate_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '验证类型',
  `binding_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '绑定类型',
  `point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '验证点位',
  `x_point` bigint(16) DEFAULT NULL COMMENT 'x坐标',
  `y_point` bigint(16) DEFAULT NULL COMMENT 'y坐标',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `single_agv_flag` tinyint(1) NOT NULL COMMENT '是否为一辆车调度',
  `actual_bucket_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扫描到的货柜号',
  `origin_bucket_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '系统记录的货柜号',
  `result_message` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '错误描述',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 184 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架验证任务' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for validate_bucket_task
-- ----------------------------
DROP TABLE IF EXISTS `validate_bucket_task`;
CREATE TABLE `validate_bucket_task`  (
  `id` bigint(16) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库di',
  `task_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务ID',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'task状态',
  `created_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_app` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 100 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_job
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_job`;
CREATE TABLE `w2p_countcheck_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '料箱目标路点',
  `turnover_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '周转货架code',
  `turnover_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '周转货架货位code',
  `turnover_bucket_face` tinyint(2) DEFAULT NULL COMMENT '周转货架面',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点调度任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_job_v2`;
CREATE TABLE `w2p_countcheck_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库位Code',
  `source_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '料箱源路点',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '工作站路点',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '料箱目标路点',
  `level3_inventory_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '料箱三级库存ID',
  `container_transfer_job_id` bigint(16) DEFAULT NULL COMMENT '容器转移任务id',
  `container_move_job_id` bigint(16) DEFAULT NULL COMMENT '容器搬运任务id',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_station_code`(`station_code`) USING BTREE,
  INDEX `idx_transfer_job_id`(`container_transfer_job_id`) USING BTREE,
  INDEX `idx_move_job_id`(`container_move_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点调度任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_work
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_work`;
CREATE TABLE `w2p_countcheck_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点入库单ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_work_detail`;
CREATE TABLE `w2p_countcheck_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点调度单ID',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '料箱Code',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点调度作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_work_detail_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_work_detail_v2`;
CREATE TABLE `w2p_countcheck_work_detail_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点调度单ID',
  `source_detail_id` bigint(16) DEFAULT -1 COMMENT '盘点订单明细行ID',
  `level1_container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级容器',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点调度作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_countcheck_work_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_countcheck_work_v2`;
CREATE TABLE `w2p_countcheck_work_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱盘点入库单ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱盘点作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_guided_put_away_job
-- ----------------------------
DROP TABLE IF EXISTS `w2p_guided_put_away_job`;
CREATE TABLE `w2p_guided_put_away_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '单据类型',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) NOT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `quantity` int(11) DEFAULT NULL COMMENT '计划数量',
  `fullfill_quantity` int(11) DEFAULT NULL COMMENT '实捡数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺捡或多捡原因',
  `new_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '新货位Code（实操反馈）',
  `level3_inventory_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '料箱三级库存ID',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `container_transfer_job_id` bigint(16) DEFAULT NULL COMMENT '容器转移任务id',
  `container_move_job_id` bigint(16) DEFAULT NULL COMMENT '容器搬运任务id',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站槽位Code',
  `source_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运起始位置code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱到人推荐上架任务表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for w2p_guided_putaway_work
-- ----------------------------
DROP TABLE IF EXISTS `w2p_guided_putaway_work`;
CREATE TABLE `w2p_guided_putaway_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '推荐上架作业单号',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源单号',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '单据类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `biz_class` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '业务分类',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '业务类型',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '存储类型，整存/散存',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '作业单优先级',
  `finished_date` datetime(3) DEFAULT NULL COMMENT '完成时间',
  `stop_date` datetime(3) DEFAULT NULL COMMENT '暂停时间',
  `cancel_date` datetime(3) DEFAULT NULL COMMENT '取消时间',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `opened` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0是FALSE,1为TRUE',
  `submit_times` int(4) NOT NULL DEFAULT 0 COMMENT '提交次数',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_work_id`(`work_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_biz_class`(`biz_class`) USING BTREE,
  INDEX `idx_biz_type`(`biz_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱到人推荐上架作业单主信息' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for w2p_guided_putaway_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `w2p_guided_putaway_work_detail`;
CREATE TABLE `w2p_guided_putaway_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单号',
  `detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) NOT NULL COMMENT 'SKU',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主',
  `lot_id` bigint(16) NOT NULL COMMENT '批次',
  `pack_id` bigint(16) NOT NULL COMMENT '包装规格',
  `original_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '原始计划上架数量',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '计划上架数量',
  `fulfill_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实际上架数量',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否冻结,0-否,1-是',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '工作站槽位三级库存ID',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_detail_id`(`detail_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱到人推荐上架作业单明细' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for w2p_picking_job
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_job`;
CREATE TABLE `w2p_picking_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `customer_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '三级库存ID',
  `quantity` int(11) DEFAULT NULL COMMENT '应拣数量',
  `actual_quantity` int(11) DEFAULT NULL COMMENT '实拣数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺拣原因',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `turnover_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '周转货架Code',
  `turnover_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '周转货架货位Code',
  `turnover_bucket_face` tinyint(2) DEFAULT NULL COMMENT '周转货架作业面',
  `bucket_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '存储货架点Code',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站槽位Code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拣货任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_picking_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_job_v2`;
CREATE TABLE `w2p_picking_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库单类型',
  `picking_order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单ID',
  `order_group_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '集合单类型',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单明细ID',
  `sku_id` bigint(16) DEFAULT NULL COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `package_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品存储类型，整存，散存',
  `customer_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '三级库存ID',
  `quantity` int(11) DEFAULT NULL COMMENT '应拣数量',
  `actual_quantity` int(11) DEFAULT NULL COMMENT '实拣数量',
  `qty_mismatch_reason` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '缺拣原因',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位Code',
  `container_transfer_job_id` bigint(16) DEFAULT NULL COMMENT '容器转移任务id',
  `container_move_job_id` bigint(16) DEFAULT NULL COMMENT '容器搬运任务id',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站槽位Code',
  `source_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运起始位置code',
  `station_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点点位code',
  `job_mode` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'job调度方式',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_station_code`(`station_code`) USING BTREE,
  INDEX `idx_transfer_job_id`(`container_transfer_job_id`) USING BTREE,
  INDEX `idx_move_job_id`(`container_move_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '拣货任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_picking_work
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_work`;
CREATE TABLE `w2p_picking_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单类型',
  `picking_order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单ID',
  `wave_order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单类型',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站编号',
  `priority_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `priority_value` int(11) NOT NULL DEFAULT 0 COMMENT '优先级数值',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站槽位编号',
  `ship_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '截止发货时间',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定货架编号',
  `work_station_match_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单工作站分配类型',
  `cross_zone` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否跨库区，0否，1是',
  `picking_available_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否有货先发，0否，1是',
  `match_station_field1` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段1',
  `match_station_field2` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段2',
  `match_station_field3` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段3',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE,
  INDEX `idx_orderid`(`order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱拣货调度作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_picking_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_work_detail`;
CREATE TABLE `w2p_picking_work_detail`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单明细ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单ID',
  `sku_id` bigint(16) NOT NULL DEFAULT 0 COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT 0 COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '应拣数量',
  `fulfill_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实拣数量',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workdetailid`(`picking_work_detail_id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱拣货调度作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_picking_work_detail_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_work_detail_v2`;
CREATE TABLE `w2p_picking_work_detail_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `picking_work_detail_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单明细ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '拣货作业单ID',
  `sku_id` bigint(16) NOT NULL DEFAULT 0 COMMENT 'SKU ID',
  `lot_id` bigint(16) DEFAULT 0 COMMENT '批次ID',
  `pack_id` bigint(16) DEFAULT NULL COMMENT '包装规格ID',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `frozen` tinyint(1) NOT NULL DEFAULT 0 COMMENT '库存是否冻结，0否，1是',
  `level3_inventory_id` bigint(16) DEFAULT NULL COMMENT '指定的三级库存ID',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '应拣数量',
  `fulfill_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实拣数量',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workdetailid`(`picking_work_detail_id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱拣货调度作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_picking_work_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_picking_work_v2`;
CREATE TABLE `w2p_picking_work_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单ID',
  `order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '订单类型',
  `picking_order_group_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单ID',
  `wave_order_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '集合单类型',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `picking_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站编号',
  `priority_value` int(11) NOT NULL DEFAULT 0 COMMENT '优先级数值',
  `priority_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `station_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定工作站槽位编号',
  `ship_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '截止发货时间',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '指定料箱编号',
  `work_station_match_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单工作站分配类型',
  `cross_zone` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否跨库区，0否，1是',
  `picking_available_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否有货先发，0否，1是',
  `match_station_field1` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段1',
  `match_station_field2` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段2',
  `match_station_field3` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配工作站字段3',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单状态',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`picking_work_id`) USING BTREE,
  INDEX `idx_orderid`(`order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱拣货调度作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_putaway_job
-- ----------------------------
DROP TABLE IF EXISTS `w2p_putaway_job`;
CREATE TABLE `w2p_putaway_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `put_away_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱直接上架调度单ID',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `target_face_num` tinyint(2) DEFAULT NULL,
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '料箱目标路点',
  `new_flag` tinyint(2) NOT NULL DEFAULT 1 COMMENT '是否是新增,0 新增 1非新增',
  `turnover_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '周转货架code',
  `turnover_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '周转货架货位code',
  `turnover_bucket_face` tinyint(2) DEFAULT NULL COMMENT '周转货架面',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱直接上架调度任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_putaway_job_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_putaway_job_v2`;
CREATE TABLE `w2p_putaway_job_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv 类型',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站Code',
  `put_away_work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '直接上架调度单ID',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '容器Code',
  `source_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移位起始点',
  `station_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '容器目标路点',
  `container_move_job_id` bigint(16) DEFAULT NULL COMMENT '容器搬运任务id',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_container_code`(`container_code`) USING BTREE,
  INDEX `idx_container_move_job_id`(`container_move_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架调度任务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_putaway_work
-- ----------------------------
DROP TABLE IF EXISTS `w2p_putaway_work`;
CREATE TABLE `w2p_putaway_work`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱直接上架调度单ID',
  `order_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱直接上架入库单ID',
  `source_order_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '调度单作业类型，离线，在线',
  `workbins` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '所需料箱以及是否是新增料箱',
  `workbin_qty` int(8) NOT NULL DEFAULT 0 COMMENT '所需料箱数量',
  `owner_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主Code',
  `tenant_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '租户ID',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱直接上架作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for w2p_putaway_work_v2
-- ----------------------------
DROP TABLE IF EXISTS `w2p_putaway_work_v2`;
CREATE TABLE `w2p_putaway_work_v2`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱直接上架调度单ID',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `priority_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '优先级类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_workid`(`work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for wcs_flyway_schema_history
-- ----------------------------
DROP TABLE IF EXISTS `wcs_flyway_schema_history`;
CREATE TABLE `wcs_flyway_schema_history`  (
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
  INDEX `wcs_flyway_schema_history_s_idx`(`success`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for wcs_hds_system_config
-- ----------------------------
DROP TABLE IF EXISTS `wcs_hds_system_config`;
CREATE TABLE `wcs_hds_system_config`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT NULL,
  `updated_date` datetime(0) DEFAULT NULL,
  `status` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 18 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for work_binding_station
-- ----------------------------
DROP TABLE IF EXISTS `work_binding_station`;
CREATE TABLE `work_binding_station`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `work_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业单号',
  `biz_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '业务类型-盘点、拣货、直接上架、推荐上架、理货',
  `work_mode` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业类型-在线离线',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否调度,0-否,1-是',
  `station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站-当前调度发起的工作站编号',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_work_id`(`work_id`) USING BTREE,
  INDEX `idx_station_code`(`station_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 416 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '推荐上架作业单明细' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for workbin_assign_record
-- ----------------------------
DROP TABLE IF EXISTS `workbin_assign_record`;
CREATE TABLE `workbin_assign_record`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(16) NOT NULL COMMENT '仓库ID',
  `zone_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区域',
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'agv code',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '辊筒类型',
  `agv_capacity` int(11) DEFAULT NULL COMMENT '辊筒运力',
  `point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '分配上料点',
  `load_nums` int(11) DEFAULT NULL COMMENT '上料点上料数量',
  `remaind_nums` int(11) DEFAULT NULL COMMENT '剩余上料数量',
  `state` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `created_date` datetime(0) DEFAULT NULL COMMENT '创建时间',
  `updated_date` datetime(0) DEFAULT NULL COMMENT '修改时间',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_point_code`(`point_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱车分车记录' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_bucket
-- ----------------------------
DROP TABLE IF EXISTS `workbin_bucket`;
CREATE TABLE `workbin_bucket`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `bucket_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架类型',
  `agv_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv搬运类型',
  `home_point` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架位置(固定货架和周转货架位置固定，不记录料箱agv货架位置信息)',
  `bucket_state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架状态',
  `top_face` tinyint(2) DEFAULT NULL COMMENT '货架朝向正北面',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_bucket_code`(`bucket_code`) USING BTREE,
  INDEX `idx_bucket_type`(`bucket_type`) USING BTREE,
  INDEX `idx_bucket_state`(`bucket_state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱货架表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_bucket_move_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_bucket_move_job`;
CREATE TABLE `workbin_bucket_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `bucket_move_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架移动类型',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架Code',
  `source_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '初始点位code',
  `target_waypoint_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标点位code',
  `target_station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标工作站code',
  `top_face` tinyint(2) DEFAULT NULL COMMENT '货架目标正北面',
  `put_down` tinyint(1) DEFAULT NULL COMMENT '是否放下货架，0否，1是',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_bucket_code`(`bucket_code`) USING BTREE,
  INDEX `idx_target_station_code`(`target_station_code`) USING BTREE,
  INDEX `idx_target_waypoint_code`(`target_waypoint_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '周转货架移位任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_job_extends
-- ----------------------------
DROP TABLE IF EXISTS `workbin_job_extends`;
CREATE TABLE `workbin_job_extends`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `robot_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单id',
  `container_digital_code` bigint(20) DEFAULT NULL COMMENT '料箱数字码',
  `container_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器编码',
  `target_slot_list` varchar(1024) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标库位集合',
  `target_zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `target_height` int(11) DEFAULT NULL COMMENT '目标点高度',
  `before_take_confirm` tinyint(1) DEFAULT NULL COMMENT '取料前交互与否',
  `before_put_confirm` tinyint(1) DEFAULT NULL COMMENT '放料前交互与否',
  `bucket_layer` int(11) DEFAULT NULL COMMENT '取料货位所在层数',
  `bucket_height` int(11) DEFAULT NULL COMMENT '取料高度',
  `take_face` int(11) DEFAULT NULL COMMENT '取料方向',
  `target_bucket_layer` int(11) DEFAULT NULL COMMENT '所在目标货架层数',
  `target_bucket_height` int(11) DEFAULT NULL COMMENT '所在目标货架层高',
  `put_face` int(11) DEFAULT NULL COMMENT '放料方向',
  `start_slot_digital_code` bigint(20) DEFAULT NULL,
  `end_slot_digital_code` bigint(20) DEFAULT NULL,
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '指定agv',
  `agv_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '指定agv类型',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `unq_robot_job_id`(`robot_job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱车作业单扩展字段' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_move_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_move_job`;
CREATE TABLE `workbin_move_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `move_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱移位类型',
  `fixed_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '固定货架',
  `fixed_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '固定货位',
  `workbin_agv` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱agv',
  `turnover_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '周转货架',
  `turnover_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '周转货位',
  `carry_agv` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '搬运agv',
  `target_station_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标工作站',
  `target_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '移位目标点',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_workbin_code`(`workbin_code`) USING BTREE,
  INDEX `idx_move_type`(`move_type`) USING BTREE,
  INDEX `idx_fixed_bucket_code`(`fixed_bucket_code`) USING BTREE,
  INDEX `idx_fixed_bucket_slot_code`(`fixed_bucket_slot_code`) USING BTREE,
  INDEX `idx_workbin_agv`(`workbin_agv`) USING BTREE,
  INDEX `idx_turnover_bucket_code`(`turnover_bucket_code`) USING BTREE,
  INDEX `idx_turnover_bucket_slot_code`(`turnover_bucket_slot_code`) USING BTREE,
  INDEX `idx_carry_agv`(`carry_agv`) USING BTREE,
  INDEX `idx_target_station_code`(`target_station_code`) USING BTREE,
  INDEX `idx_target_way_point_code`(`target_way_point_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱移位任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_put_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_put_job`;
CREATE TABLE `workbin_put_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位',
  `take_layer` int(64) DEFAULT NULL COMMENT '存放料箱的agv货架层',
  `take_height` int(64) DEFAULT NULL COMMENT '存放料箱的agv货位高度',
  `road_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业点',
  `put_face` tinyint(2) DEFAULT NULL COMMENT '作业方向',
  `put_height` int(64) DEFAULT NULL COMMENT '作业高度',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位',
  `end_move_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '取货完成后移动的点位',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_workbin_code`(`workbin_code`) USING BTREE,
  INDEX `idx_source_bucket_code`(`source_bucket_code`) USING BTREE,
  INDEX `idx_target_bucket_code`(`target_bucket_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '放料箱任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_standard_move_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_standard_move_job`;
CREATE TABLE `workbin_standard_move_job`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) NOT NULL,
  `zone_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `pre_id` bigint(20) DEFAULT NULL,
  `job_id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'job_id',
  `busi_group_id` bigint(20) NOT NULL COMMENT '分车组ID',
  `robot_job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上游任务ID',
  `container_digital_code` bigint(20) DEFAULT NULL COMMENT '料箱id',
  `source_way_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起始点位',
  `start_slot_digital_code` bigint(20) DEFAULT NULL,
  `start_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '起始库位',
  `end_slot_digital_code` bigint(20) DEFAULT NULL,
  `end_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标库位',
  `target_way_point_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标点位',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `take_height` int(11) DEFAULT NULL COMMENT '取货高度',
  `take_face` int(11) DEFAULT NULL COMMENT '取货方向',
  `put_height` int(11) DEFAULT NULL COMMENT '放置高度',
  `put_face` int(11) DEFAULT NULL COMMENT '放置方向',
  `target_layer` int(11) DEFAULT NULL COMMENT '存取一体时存放在缓存位层数',
  `source_layer` int(11) DEFAULT NULL COMMENT '存取一体时存放在缓存位高度',
  `job_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型 MOVE_JOB,PUT_JOB,TAKE_JOB,TAKE_PUT_JOB...',
  `agv_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `agv_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `priority_type` int(2) DEFAULT NULL COMMENT 'FORCE OR COMMON',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `before_take_confirm` tinyint(1) DEFAULT NULL,
  `before_put_confirm` tinyint(1) UNSIGNED ZEROFILL DEFAULT NULL,
  `need_operation` tinyint(1) DEFAULT NULL COMMENT '是否需要实操',
  `need_reset` tinyint(1) DEFAULT NULL COMMENT '是否需要返库',
  `put_down` tinyint(1) DEFAULT NULL COMMENT '是否放下',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `work_mode` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(0) DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_date` datetime(0) DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'unknown' COMMENT '创建入口',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'unknown' COMMENT '最后修改入口',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `unq_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '料箱车分段任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_take_down_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_take_down_job`;
CREATE TABLE `workbin_take_down_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '料箱Code',
  `bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货位编码',
  `bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架编码',
  `bucket_face` tinyint(2) DEFAULT NULL COMMENT '货架面',
  `station_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站编码',
  `station_stop_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '工作站停靠点编码',
  `agv_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '小车code',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_jobid`(`job_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_workbin_code`(`workbin_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '空料箱下架任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for workbin_take_job
-- ----------------------------
DROP TABLE IF EXISTS `workbin_take_job`;
CREATE TABLE `workbin_take_job`  (
  `id` bigint(16) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键id',
  `warehouse_id` bigint(16) DEFAULT NULL COMMENT '仓库ID',
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区code',
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务id',
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务类型',
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '任务状态',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '任务优先级',
  `agv_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv标识',
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'agv 类型',
  `workbin_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '料箱Code',
  `source_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货架',
  `source_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源货位',
  `road_way_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '作业点',
  `take_face` tinyint(2) DEFAULT NULL COMMENT '作业方向',
  `take_height` int(64) DEFAULT NULL COMMENT '作业高度',
  `put_layer` int(64) DEFAULT NULL COMMENT '存放料箱的agv货架层',
  `put_height` int(64) DEFAULT NULL COMMENT '存放料箱的agv货位高度',
  `target_bucket_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货架',
  `target_bucket_slot_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标货位',
  `end_move_point_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '取货完成后移动的点位',
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '创建入口',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'unknown' COMMENT '最后更新入口',
  `updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_job_id`(`job_id`) USING BTREE,
  INDEX `idx_job_type`(`job_type`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_agv_code`(`agv_code`) USING BTREE,
  INDEX `idx_workbin_code`(`workbin_code`) USING BTREE,
  INDEX `idx_source_bucket_code`(`source_bucket_code`) USING BTREE,
  INDEX `idx_target_bucket_code`(`target_bucket_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '取料箱任务表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Procedure structure for add_column
-- ----------------------------
DROP PROCEDURE IF EXISTS `add_column`;
delimiter ;;
CREATE DEFINER=`root`@`%` PROCEDURE `add_column`()
BEGIN
	IF NOT EXISTS( SELECT * FROM  information_schema.columns WHERE table_schema='evo_wcs_g2p' AND table_name='workbin_put_job' AND column_name='take_height')
	THEN
	ALTER TABLE evo_wcs_g2p.workbin_put_job ADD COLUMN `take_height` int(64) DEFAULT NULL COMMENT '存放料箱的agv货位高度' AFTER take_layer;
	END IF;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for add_col_remark
-- ----------------------------
DROP PROCEDURE IF EXISTS `add_col_remark`;
delimiter ;;
CREATE DEFINER=`root`@`%` PROCEDURE `add_col_remark`()
BEGIN IF NOT EXISTS (SELECT 1 FROM
information_schema.columns WHERE table_schema = 'evo_wcs_g2p' AND table_name =
'reprint_move_job' and column_name='equipment_id')
THEN
alter table evo_wcs_g2p.reprint_move_job add `equipment_id`  bigint(16) NOT NULL DEFAULT '0' COMMENT '设备号' AFTER `lock_flag`;
END IF;
END
;;
delimiter ;

-- ----------------------------
-- Function structure for flagForRead
-- ----------------------------
DROP FUNCTION IF EXISTS `flagForRead`;
delimiter ;;
CREATE DEFINER=`root`@`%` FUNCTION `flagForRead`(v_flag INT) RETURNS varchar(1024) CHARSET utf8
    DETERMINISTIC
begin
declare v_name varchar(1024) DEFAULT '';
if (v_flag & 0x80) = 0x80 then
  set v_name := CONCAT(v_name,'CLOSE_LIFT_DOOR,');
end if;
if (v_flag & 0x40) = 0x40 then
  set v_name := CONCAT(v_name,'MULTI_FLOOR,');
end if;
if (v_flag & 0x20) = 0x20 then
  set v_name := CONCAT(v_name,'LOCK_AGV,');
end if;
if (v_flag & 0x10) = 0x10 then
  set v_name := CONCAT(v_name,'IDLE_AGV_SCHEDULE,');
end if;
if (v_flag & 0x8) = 0x8 then
  set v_name := CONCAT(v_name,'ELEVATOR_JOB_LINKED,');
end if;
if (v_flag & 0x4) = 0x4 then
  set v_name := CONCAT(v_name,'AGV_RELAY,');
end if;
if (v_flag & 0x2) = 0x2 then
  set v_name := CONCAT(v_name,'STAND_BY,');
end if;
if (v_flag & 0x1) = 0x1 then
  set v_name := CONCAT(v_name,'PUT_DOWN,');
end if;
RETURN v_name;
end
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
