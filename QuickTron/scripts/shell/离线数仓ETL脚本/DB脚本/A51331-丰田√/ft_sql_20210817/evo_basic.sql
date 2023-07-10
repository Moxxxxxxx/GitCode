/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_basic

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:13
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for basic_agv
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv`;
CREATE TABLE `basic_agv`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '小车编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区Id',
  `operation_scope_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人作业范围ID',
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '小车类型',
  `agv_appearance_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人外形ID',
  `agv_frame_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '车架号',
  `drive_unit_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上位机版本',
  `ip` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '小车ip',
  `dsp_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'dsp版本',
  `battery_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `radar_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `camera_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '摄像头版本',
  `os` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作系统',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

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
-- Table structure for basic_agv_appearance_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_appearance_outdated`;
CREATE TABLE `basic_agv_appearance_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_appearance_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '机器人外形编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_appearance_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_outdated`;
CREATE TABLE `basic_agv_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '小车编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编码',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区编码',
  `operation_scope_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人作业范围ID',
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '小车类型',
  `agv_appearance_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人外形ID',
  `agv_frame_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '车架号',
  `drive_unit_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上位机版本',
  `ip` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '小车ip',
  `dsp_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'dsp版本',
  `battery_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `radar_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '电池版本',
  `camera_version` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '摄像头版本',
  `os` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作系统',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_part
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_part`;
CREATE TABLE `basic_agv_part`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_part_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '硬件类型编码',
  `agv_appearance_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人外形ID',
  `agv_part_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '机器人外形名称',
  `agv_container_spec` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '车辆载物区分',
  `part_order` int(2) UNSIGNED DEFAULT NULL COMMENT '部件排序',
  `length` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高',
  `self_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '自身高度',
  `safe_width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽安全距离',
  `safe_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高安全距离',
  `rotation_diameter` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '旋转直径',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_part_code`, `agv_appearance_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_part_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_part_outdated`;
CREATE TABLE `basic_agv_part_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `agv_part_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '硬件类型编码',
  `agv_appearance_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人外形ID',
  `agv_part_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '机器人外形名称',
  `agv_container_spec` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '车辆载物区分',
  `part_order` int(2) UNSIGNED DEFAULT NULL COMMENT '部件排序',
  `length` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高',
  `self_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '自身高度',
  `safe_width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽安全距离',
  `safe_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高安全距离',
  `rotation_diameter` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '旋转直径',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`agv_part_code`, `agv_appearance_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_point
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_point`;
CREATE TABLE `basic_agv_point`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库Id',
  `slot_id` bigint(20) NOT NULL COMMENT '库位Id',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '类型编码',
  `point_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架停靠点',
  `slot_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库位点编码',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`slot_id`, `agv_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_agv_type
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_type`;
CREATE TABLE `basic_agv_type`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '类型编码',
  `bucket_separable` tinyint(1) UNSIGNED NOT NULL COMMENT '是否车架分离',
  `charger_port_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电口类型',
  `walk_face` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行走面',
  `is_cross_bucket` tinyint(1) NOT NULL COMMENT '是否可从货架穿行',
  `agv_camera_distance` int(11) NOT NULL COMMENT '车载摄像距离',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `agv_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_agv_type_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_agv_type_outdated`;
CREATE TABLE `basic_agv_type_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `agv_type_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '类型编码',
  `bucket_separable` tinyint(1) UNSIGNED NOT NULL COMMENT '是否车架分离',
  `charger_port_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电口类型',
  `walk_face` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行走面',
  `is_cross_bucket` tinyint(1) NOT NULL COMMENT '是否可从货架穿行',
  `agv_camera_distance` int(11) NOT NULL COMMENT '车载摄像距离',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `agv_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

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
-- Table structure for basic_ancillary_point_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_ancillary_point_outdated`;
CREATE TABLE `basic_ancillary_point_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `ancillary_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '更改后的点条码',
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更换时的地图编码',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更换时的地图版本',
  `map_bar_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图上的点条码',
  `old_bar_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上一次更换的点条码',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
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
-- Table structure for basic_auto_discharge_cargo_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_auto_discharge_cargo_outdated`;
CREATE TABLE `basic_auto_discharge_cargo_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `equipment_id` bigint(20) NOT NULL COMMENT '目标设备',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '点编码',
  `agv_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '机器人直接驶入点编码',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_automated_path
-- ----------------------------
DROP TABLE IF EXISTS `basic_automated_path`;
CREATE TABLE `basic_automated_path`  (
  `id` bigint(20) NOT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `enabled` tinyint(1) NOT NULL COMMENT '启用标志',
  `start_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起始点编码',
  `start_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起点作业区分',
  `start_point_equipment_id` bigint(20) NOT NULL COMMENT '起点设备id',
  `end_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点点编码',
  `end_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点作业区分',
  `end_point_equipment_id` bigint(20) NOT NULL COMMENT '终点设备id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_automated_path_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_automated_path_outdated`;
CREATE TABLE `basic_automated_path_outdated`  (
  `id` bigint(20) NOT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `enabled` tinyint(1) NOT NULL COMMENT '启用标志',
  `start_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起始点编码',
  `start_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起点作业区分',
  `start_point_equipment_id` bigint(20) NOT NULL COMMENT '起点设备id',
  `end_point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点点编码',
  `end_work_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '终点作业区分',
  `end_point_equipment_id` bigint(20) NOT NULL COMMENT '终点设备id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_bar_code
-- ----------------------------
DROP TABLE IF EXISTS `basic_bar_code`;
CREATE TABLE `basic_bar_code`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `bar_code` varchar(60) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '外部货品编码',
  `owner_id` bigint(20) NOT NULL COMMENT '货主ID',
  `sku_id` bigint(20) UNSIGNED NOT NULL COMMENT '商品ID',
  `entity_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '条码匹配目标',
  `quantity` int(11) UNSIGNED NOT NULL COMMENT '库存数量',
  `entity_object` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '条码匹配维度',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_bucket
-- ----------------------------
DROP TABLE IF EXISTS `basic_bucket`;
CREATE TABLE `basic_bucket`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编码',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区编码',
  `bucket_template_id` bigint(20) DEFAULT NULL COMMENT '模板id',
  `bucket_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架编码',
  `bucket_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '货架类型编码',
  `enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '启用禁用',
  `station_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '工作站',
  `owner_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '货主',
  `point_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架停靠点',
  `top_face` varchar(5) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架朝向面',
  `destination` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目的地',
  `alias` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '别名',
  `digital_code` bigint(20) NOT NULL,
  `cage_car_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '笼车状态',
  `sku_mix_limit` int(11) DEFAULT 999 COMMENT '商品混放上限',
  `attribute1` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架推荐属性1',
  `attribute2` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架推荐属性2',
  `attribute3` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架推荐属性3',
  `attribute4` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架推荐属性4',
  `attribute5` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架推荐属性5',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '是否启用',
  `validate_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'notYet' COMMENT '货架验证状态',
  `validate_time` datetime(3) DEFAULT NULL COMMENT '货架验证时间',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `extended_field` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`bucket_code`, `warehouse_id`) USING BTREE,
  UNIQUE INDEX `uk_digital_code`(`digital_code`) USING BTREE,
  INDEX `bucker_code`(`bucket_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 6371 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_bucket_part
-- ----------------------------
DROP TABLE IF EXISTS `basic_bucket_part`;
CREATE TABLE `basic_bucket_part`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库Id',
  `bucket_part_code` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_type_id` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架类型编码',
  `bucket_part_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架部件名称',
  `bucket_part_layer` int(11) UNSIGNED DEFAULT NULL COMMENT '部件层',
  `length` int(11) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` int(11) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` int(11) UNSIGNED DEFAULT NULL COMMENT '高',
  `safe_length` int(11) UNSIGNED DEFAULT NULL COMMENT '长安全距离',
  `safe_width` int(11) UNSIGNED DEFAULT NULL COMMENT '宽安全距离',
  `safe_height` int(11) UNSIGNED DEFAULT NULL COMMENT '高安全距离',
  `rotation_radius` int(11) UNSIGNED DEFAULT NULL COMMENT '旋转直径',
  `offset_off_center_x` int(11) DEFAULT NULL COMMENT '距中心偏移X',
  `offset_off_center_y` int(11) DEFAULT NULL COMMENT '距中心偏移Y',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_bucket_type_layer`(`bucket_type_id`, `bucket_part_layer`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 207 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_bucket_template
-- ----------------------------
DROP TABLE IF EXISTS `basic_bucket_template`;
CREATE TABLE `basic_bucket_template`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `bucket_template_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货架模板编码',
  `bucket_type_id` bigint(20) DEFAULT NULL COMMENT '货架类型Id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`bucket_template_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_bucket_type
-- ----------------------------
DROP TABLE IF EXISTS `basic_bucket_type`;
CREATE TABLE `basic_bucket_type`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `bucket_type_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架类型编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库编码',
  `virtual_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架虚实区分',
  `move_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架移动区分',
  `walk_through` tinyint(4) DEFAULT 0 COMMENT '是否可穿行',
  `apply_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架用途区分,store:存储;turnover:周转;robot:机器人一体;system:系统逻辑;seeding:分播墙',
  `length` double(11, 3) NOT NULL COMMENT '货架长度',
  `width` double(11, 3) NOT NULL COMMENT '货架宽度',
  `height` double(11, 3) UNSIGNED NOT NULL COMMENT '高',
  `weight_limit` double(11, 3) DEFAULT NULL COMMENT '重量限制',
  `available_length` double(11, 3) UNSIGNED NOT NULL COMMENT '可用长',
  `available_width` double(11, 3) UNSIGNED NOT NULL COMMENT '可用宽',
  `available_height` double(11, 3) UNSIGNED NOT NULL COMMENT '可用高',
  `layer_layout` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业面每层布局',
  `layer_color` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业面每层颜色',
  `work_face` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作面',
  `deliver_face` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '投递面',
  `parcel_collect_face` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '集货面',
  `deny_enter_face` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '不可进入面',
  `face_count` int(11) UNSIGNED NOT NULL COMMENT '面数',
  `layer_count` int(11) UNSIGNED NOT NULL COMMENT '层数',
  `leg_diameter` int(11) UNSIGNED DEFAULT NULL COMMENT '货架腿直径',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `discern_bucket_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'yes' COMMENT '是否识别货架码',
  `fork_height` double(11, 3) NOT NULL DEFAULT 0.000 COMMENT '叉孔高度',
  `fork_base_height` double(11, 3) NOT NULL DEFAULT 0.000 COMMENT '叉孔底部高度',
  `texture_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '材质类型',
  `created_time` datetime(3) DEFAULT NULL,
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) DEFAULT NULL,
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`bucket_type_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 124 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_capacity
-- ----------------------------
DROP TABLE IF EXISTS `basic_capacity`;
CREATE TABLE `basic_capacity`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `capacity_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业能力组合',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `performance_range` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业能力范围',
  `equipment_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '可作业机器人类型',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`capacity_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

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
  `charger_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '充电类型编码',
  `enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否启用',
  `map_code_and_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图版本和编号',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `charger_code`, `map_code_and_version`) USING BTREE,
  INDEX `ix_code`(`warehouse_id`, `charger_code`, `state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_charger_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_charger_outdated`;
CREATE TABLE `basic_charger_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编码',
  `charger_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电桩编码',
  `charger_port_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '充电口类型',
  `charger_mode` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '充电模式',
  `charger_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '充电类型编码',
  `enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否启用',
  `map_code_and_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图版本和编号',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `charger_code`, `map_code_and_version`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_code_plan
-- ----------------------------
DROP TABLE IF EXISTS `basic_code_plan`;
CREATE TABLE `basic_code_plan`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `plan_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '方案名称',
  `plan_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '方案代码',
  `plan_type` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '方案类型，区分容器还是货架',
  `default_plan` tinyint(2) NOT NULL DEFAULT 0 COMMENT '是否是默认方案',
  `code_length` int(11) NOT NULL DEFAULT 0 COMMENT '编码长度',
  `face_length` int(11) DEFAULT 0 COMMENT '面长度',
  `layer_length` int(11) DEFAULT 0 COMMENT '层长度',
  `slot_length` int(11) DEFAULT 0 COMMENT '库位长度',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_config_design
-- ----------------------------
DROP TABLE IF EXISTS `basic_config_design`;
CREATE TABLE `basic_config_design`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `design_code` varchar(128) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `warehouse_id` bigint(20) NOT NULL,
  `design_name` varchar(128) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL,
  `config_json` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  `remark` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '该方案的说明备注',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`design_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_container
-- ----------------------------
DROP TABLE IF EXISTS `basic_container`;
CREATE TABLE `basic_container`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `container_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '容器编码',
  `bucket_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '货架id',
  `slot_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `tenant_id` bigint(20) UNSIGNED NOT NULL COMMENT '租户ID',
  `warehouse_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '仓库ID',
  `container_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '容器类型ID',
  `super_container_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '上级容器ID',
  `container_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器状态',
  `use_range` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '使用范围',
  `rfid_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'RFID编码',
  `digital_code` bigint(20) DEFAULT NULL,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`container_code`, `tenant_id`) USING BTREE,
  UNIQUE INDEX `uk_digital_code`(`digital_code`) USING BTREE,
  INDEX `container_code`(`container_code`) USING BTREE,
  INDEX `bucket_id`(`bucket_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_container_type
-- ----------------------------
DROP TABLE IF EXISTS `basic_container_type`;
CREATE TABLE `basic_container_type`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `container_type_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '容器类型编码',
  `tenant_id` bigint(20) UNSIGNED NOT NULL COMMENT '租户ID',
  `classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '容器区分',
  `container_type_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器类型名称',
  `work_face` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '作业面',
  `grid_row` int(11) UNSIGNED DEFAULT NULL COMMENT '料格行数',
  `grid_column` int(11) UNSIGNED DEFAULT NULL COMMENT '料格列数',
  `initial_face` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '初始面',
  `subordinate_type_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '下级容器类型ID',
  `length` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高',
  `volume` double(11, 3) DEFAULT NULL COMMENT '体积',
  `weight` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '重量',
  `hold_height` double(11, 3) DEFAULT NULL COMMENT '取货高度',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `discern_container_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'yes' COMMENT '是否识别货架码',
  `fork_height` double(11, 3) NOT NULL DEFAULT 0.000 COMMENT '叉孔高度',
  `fork_base_height` double(11, 3) NOT NULL DEFAULT 0.000 COMMENT '叉孔底部高度',
  `texture_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '材质类型',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`container_type_code`, `tenant_id`, `classification`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_device_floor
-- ----------------------------
DROP TABLE IF EXISTS `basic_device_floor`;
CREATE TABLE `basic_device_floor`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `floor_number` int(11) NOT NULL DEFAULT 1 COMMENT '楼层',
  `floor_description` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '楼层描述',
  `arrive_area` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '可达库区-已,分隔',
  `floor_exit` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '出口',
  `floor_entrance` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '入口',
  `exit_location` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '出口设备交互位置',
  `entrance_location` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '入口设备交互位置',
  `device_location` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '设备位置',
  `floor_status` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态-有效(effective),无效(invalid)',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '设备编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `created_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建用户',
  `created_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '创建应用',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `last_updated_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新用户',
  `last_updated_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '更新应用',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_device_management
-- ----------------------------
DROP TABLE IF EXISTS `basic_device_management`;
CREATE TABLE `basic_device_management`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备编码',
  `device_name` varchar(70) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `subjection_device` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '隶属设备',
  `device_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'elevator' COMMENT '设备类型-电梯(elevator)/升降机(lifter)/卷帘门(rollingdoor)',
  `initiate_mode` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'effective' COMMENT '启用状态-启用(effective)/禁用(invalid)',
  `affiliated_area` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '所属仓库',
  `is_line` tinyint(4) NOT NULL DEFAULT 1 COMMENT '是否在线:0-未在线，1-在线',
  `business_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务类型',
  `avg_type_limit` varchar(150) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'AGV类型限制',
  `location_config_pattern` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'dotpattern' COMMENT '位置配置模式-点模式(dotpattern)/区域模式(areapattern)',
  `now_location` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '当前位置',
  `floor_number` tinyint(4) NOT NULL DEFAULT 0 COMMENT '楼层数',
  `operation_direction` varchar(15) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'bothway' COMMENT '作业方向-只上(onlyup)/只下(onlydown)/上下双向(bothway)',
  `peripheral_ip` varchar(110) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外设ip',
  `communicate_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '通讯类型-RS485/HTTP/HTTPS',
  `carrying_capacity` bigint(20) NOT NULL DEFAULT 0 COMMENT '运载能力',
  `length_limit` bigint(20) NOT NULL DEFAULT 0 COMMENT '长限制',
  `width_limit` bigint(20) NOT NULL DEFAULT 0 COMMENT '宽限制',
  `height_limit` bigint(20) NOT NULL DEFAULT 0 COMMENT '高限制',
  `created_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建用户',
  `created_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '创建应用',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `last_updated_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新用户',
  `last_updated_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '更新应用',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `deviceCode`(`device_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_device_skip
-- ----------------------------
DROP TABLE IF EXISTS `basic_device_skip`;
CREATE TABLE `basic_device_skip`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `start_floor` int(11) NOT NULL COMMENT '起始层',
  `target_floor` int(11) NOT NULL COMMENT '目标层',
  `start_location` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '起始位置',
  `target_location` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '目标位置',
  `api_tpye` varchar(60) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '接口类型',
  `interaction_directive` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '起始交互指令集',
  `floor_status` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态-有效(effective),无效(invalid)',
  `device_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '设备编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `created_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建用户',
  `created_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '创建应用',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `last_updated_user` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '更新用户',
  `last_updated_app` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '更新应用',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_dictionary
-- ----------------------------
DROP TABLE IF EXISTS `basic_dictionary`;
CREATE TABLE `basic_dictionary`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `key_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '类型编码',
  `key_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '类型名称',
  `value_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '值Code',
  `i18n_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '国际化Code',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`key_code`, `value_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 658 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_equipment
-- ----------------------------
DROP TABLE IF EXISTS `basic_equipment`;
CREATE TABLE `basic_equipment`  (
  `id` bigint(20) NOT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `equipment_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备编码',
  `equipment_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备区分',
  `equipment_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `enabled` tinyint(1) NOT NULL COMMENT '启用状态',
  `out_in_warehouse_mode` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出入库模式',
  `carrier_capacity` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '运载能力',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`warehouse_id`, `equipment_code`, `equipment_classification`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_equipment_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_equipment_outdated`;
CREATE TABLE `basic_equipment_outdated`  (
  `id` bigint(20) NOT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `equipment_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备编码',
  `equipment_classification` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '设备区分',
  `equipment_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '设备名称',
  `enabled` tinyint(1) NOT NULL COMMENT '启用状态',
  `out_in_warehouse_mode` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出入库模式',
  `carrier_capacity` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '运载能力',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

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
-- Table structure for basic_external_point_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_external_point_outdated`;
CREATE TABLE `basic_external_point_outdated`  (
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
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = MyISAM CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_external_sku
-- ----------------------------
DROP TABLE IF EXISTS `basic_external_sku`;
CREATE TABLE `basic_external_sku`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `external_sku_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '外部商品编码',
  `sku_id` bigint(20) UNSIGNED NOT NULL COMMENT '商品Id',
  `owner_id` bigint(20) UNSIGNED NOT NULL COMMENT '货主ID',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '是否启用',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code_owner`(`external_sku_code`, `owner_id`) USING BTREE,
  UNIQUE INDEX `uk_code_sku`(`external_sku_code`, `sku_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_generate_confirm
-- ----------------------------
DROP TABLE IF EXISTS `basic_generate_confirm`;
CREATE TABLE `basic_generate_confirm`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库Id',
  `map_id` bigint(20) NOT NULL,
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图编号',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图版本',
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
-- Table structure for basic_i18n
-- ----------------------------
DROP TABLE IF EXISTS `basic_i18n`;
CREATE TABLE `basic_i18n`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `i18n_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '国际化Code',
  `language` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '语言区分',
  `value` varchar(10000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '值',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`i18n_code`, `language`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 491714 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_i18n_web_confirm
-- ----------------------------
DROP TABLE IF EXISTS `basic_i18n_web_confirm`;
CREATE TABLE `basic_i18n_web_confirm`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `web_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT 'WEB名称',
  `version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '版本',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 734 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

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
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '地图表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_map_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_map_outdated`;
CREATE TABLE `basic_map_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编码',
  `map_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图名称',
  `map_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图编号',
  `map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '地图版本',
  `base_map_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '原地图版本',
  `file_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '文件名称',
  `map_state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图上线状态',
  `json_data` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT 'JSON数据',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '启用禁用，0禁用，1启用(状态）',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `map_code`, `map_version`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '地图表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_mode
-- ----------------------------
DROP TABLE IF EXISTS `basic_mode`;
CREATE TABLE `basic_mode`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mode_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区作业模式',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `capacity` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业能力',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `created_time` datetime(3) NOT NULL COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`mode_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

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
-- Table structure for basic_module_confirm_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_module_confirm_outdated`;
CREATE TABLE `basic_module_confirm_outdated`  (
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
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
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
-- Table structure for basic_operation_scope_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_operation_scope_outdated`;
CREATE TABLE `basic_operation_scope_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `operation_scope_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业范围编码',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `zone_collection` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区集合',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`operation_scope_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_owner
-- ----------------------------
DROP TABLE IF EXISTS `basic_owner`;
CREATE TABLE `basic_owner`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `owner_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主编码',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户ID',
  `owner_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主名称',
  `contact` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '联系方式',
  `address` varchar(120) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地址',
  `owner_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主类型',
  `super_owner_id` bigint(20) DEFAULT NULL COMMENT '上级货主Id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用名称',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '更新人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新应用名称',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`owner_code`, `tenant_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_pack
-- ----------------------------
DROP TABLE IF EXISTS `basic_pack`;
CREATE TABLE `basic_pack`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `pack_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '包装规格编码',
  `owner_id` bigint(20) NOT NULL COMMENT '货主ID',
  `pack_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '包装规格名称',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`pack_code`, `owner_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_pack_capacity
-- ----------------------------
DROP TABLE IF EXISTS `basic_pack_capacity`;
CREATE TABLE `basic_pack_capacity`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `sku_id` bigint(20) UNSIGNED NOT NULL COMMENT '商品ID',
  `slot_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '货位类型ID',
  `capacity` int(255) UNSIGNED NOT NULL COMMENT '容量',
  `dispersion_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '整散类型',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`sku_id`, `slot_type_id`, `dispersion_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_pack_unit
-- ----------------------------
DROP TABLE IF EXISTS `basic_pack_unit`;
CREATE TABLE `basic_pack_unit`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `pack_unit_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '包装单位编码',
  `pack_id` bigint(20) UNSIGNED NOT NULL COMMENT '包装规格id',
  `pack_unit_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '包装规格名称',
  `pack_unit_level` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '包装单位级别',
  `quantity` int(11) UNSIGNED NOT NULL COMMENT '件装量',
  `length` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '长',
  `width` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '宽',
  `height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '高',
  `weight` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '重量',
  `volume` varchar(11) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '体积',
  `pack_material` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '包装材料',
  `boxing_required` tinyint(1) DEFAULT NULL COMMENT '是否装箱',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`pack_unit_code`, `pack_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_recovery
-- ----------------------------
DROP TABLE IF EXISTS `basic_recovery`;
CREATE TABLE `basic_recovery`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `table_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '表名',
  `data_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '数据编码',
  `data_value` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '数据',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 93401 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_relationship
-- ----------------------------
DROP TABLE IF EXISTS `basic_relationship`;
CREATE TABLE `basic_relationship`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `table_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '表名',
  `relational_table` varchar(650) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '相关表',
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
) ENGINE = InnoDB AUTO_INCREMENT = 67 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_resource_allocation_strategy
-- ----------------------------
DROP TABLE IF EXISTS `basic_resource_allocation_strategy`;
CREATE TABLE `basic_resource_allocation_strategy`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库ID',
  `assignable_scope` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作用域类型',
  `assignable_scope_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作用域编码',
  `assignable_scope_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '作用域名称',
  `assignable_model` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '可分配资源名称',
  `assignable_model_type_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '可分配资源类型编码',
  `assignable_model_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '可分配资源类型名称',
  `assignable_max_num` int(11) NOT NULL COMMENT '可分配最大数量',
  `assignable_max_num_is_compulsory` tinyint(255) NOT NULL COMMENT '可分配最大数量是否强制',
  `assignable_min_num` int(11) NOT NULL COMMENT '可分配最小数量',
  `assignable_min_num_is_compulsory` tinyint(255) NOT NULL COMMENT '可分配最小数量是否强制',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_sku
-- ----------------------------
DROP TABLE IF EXISTS `basic_sku`;
CREATE TABLE `basic_sku`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `owner_id` bigint(20) UNSIGNED NOT NULL COMMENT '所属货主',
  `sku_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品编码',
  `sku_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品名称',
  `batch_enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '是否启用批次',
  `sn_enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '是否有唯一编码',
  `over_weight_flag` tinyint(1) DEFAULT NULL COMMENT '是否超重',
  `image_url` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外形图片地址',
  `expiration_date` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '保质期',
  `near_expiration_date` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '近效期',
  `spec` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货品规格',
  `supplier` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '供应商名称',
  `abc_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'abc分类',
  `major_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品大类',
  `medium_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品中类',
  `minor_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品小类',
  `mutex_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '互斥分类',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `extended_field` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_basic_sku_code`(`owner_id`, `sku_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_slot
-- ----------------------------
DROP TABLE IF EXISTS `basic_slot`;
CREATE TABLE `basic_slot`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编码',
  `slot_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位编码',
  `bucket_id` bigint(20) UNSIGNED NOT NULL COMMENT '货架编码',
  `slot_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '货位类型编码',
  `enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '启用状态',
  `owner_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '货主',
  `bucket_face` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '所在货架面号',
  `bucket_layer` int(11) UNSIGNED NOT NULL COMMENT '所在货架层',
  `front_PTL_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '正面PTL编码',
  `back_PTL_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '反面ptl编码',
  `rfid_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'RFID编码',
  `ground_height` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '距离地面高度',
  `roadway_point_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库位对应的点编码',
  `x` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '在货架x坐标',
  `y` double(11, 3) UNSIGNED DEFAULT NULL COMMENT '在货架y坐标',
  `dispersion_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '整散类型',
  `slot_hot` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库位热度',
  `slot_number` int(11) NOT NULL COMMENT '按面库位序号',
  `extension_distance` int(11) DEFAULT NULL COMMENT '距货架伸叉距离',
  `digital_code` bigint(20) NOT NULL,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_digital_code`(`digital_code`) USING BTREE,
  UNIQUE INDEX `uk_slot_code`(`slot_code`, `bucket_id`) USING BTREE,
  INDEX `slot_code`(`slot_code`) USING BTREE,
  INDEX `bucket_id`(`bucket_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 371627 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_slot_template
-- ----------------------------
DROP TABLE IF EXISTS `basic_slot_template`;
CREATE TABLE `basic_slot_template`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `bucket_template_id` bigint(20) NOT NULL COMMENT '模板Id',
  `slot_type_id` bigint(20) DEFAULT NULL COMMENT '库位类型Id',
  `bucket_face` int(8) DEFAULT 0 COMMENT '货架面',
  `bucket_layer` int(8) DEFAULT 0 COMMENT '货架层',
  `slot_order` int(8) DEFAULT 0 COMMENT '货位顺序',
  `layer_count` int(8) DEFAULT 0 COMMENT '货位层数',
  `slot_count` int(8) DEFAULT 0 COMMENT '每层货位个数',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 201 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_slot_type
-- ----------------------------
DROP TABLE IF EXISTS `basic_slot_type`;
CREATE TABLE `basic_slot_type`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库编码',
  `slot_type_code` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位类型编码',
  `apply_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库位用途区分',
  `layer_count` int(11) UNSIGNED NOT NULL COMMENT '货架层库位层数',
  `slot_count` int(11) UNSIGNED NOT NULL COMMENT '库位每层个数',
  `height` double(11, 3) NOT NULL COMMENT '高',
  `width` double(11, 3) NOT NULL COMMENT '宽',
  `depth` double(11, 3) NOT NULL COMMENT '深',
  `weight` double(11, 3) DEFAULT NULL COMMENT '货位重量限制',
  `view_distinguish` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'common' COMMENT '库位图示区分',
  `group_layer_count` int(11) UNSIGNED DEFAULT NULL COMMENT '成组库位层数',
  `group_slot_count` int(11) UNSIGNED DEFAULT NULL COMMENT '成组库位每层个数',
  `guide_way` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '引导方式',
  `reflect_distance` int(11) UNSIGNED DEFAULT NULL COMMENT '反光板距离',
  `reflect_insider_distance` int(11) UNSIGNED DEFAULT NULL COMMENT '反光板距离',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `created_time` datetime(3) NOT NULL COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_id`, `slot_type_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 19 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_station
-- ----------------------------
DROP TABLE IF EXISTS `basic_station`;
CREATE TABLE `basic_station`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '物理主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库编号',
  `station_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `station_name` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站名称',
  `zone_id` bigint(20) NOT NULL COMMENT '库区编码',
  `station_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '固定类型',
  `enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '启用禁用',
  `map_code_and_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图版本和编号',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建人',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL COMMENT '创建日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新人',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_station_station_code`(`station_code`, `warehouse_id`, `map_code_and_version`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 21 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_station_point
-- ----------------------------
DROP TABLE IF EXISTS `basic_station_point`;
CREATE TABLE `basic_station_point`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `point_code` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '停靠点编码',
  `warehouse_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '仓库Id',
  `station_id` bigint(20) UNSIGNED NOT NULL COMMENT '工作站ID',
  `work_face` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '停靠点作业面朝向',
  `point_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '停靠点类型',
  `map_code_and_version` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地图版本和编号',
  `enabled` tinyint(1) UNSIGNED DEFAULT NULL,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '逻辑删除标志，0未删除，1已删除',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建人',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL COMMENT '创建日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新人',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`point_code`, `station_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 23 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_system_config
-- ----------------------------
DROP TABLE IF EXISTS `basic_system_config`;
CREATE TABLE `basic_system_config`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `module` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '所属模块',
  `config_group` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'GLOBAL',
  `warehouse_id` bigint(20) UNSIGNED DEFAULT NULL,
  `group_instance_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `os` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `hierarchy` int(2) NOT NULL DEFAULT 1,
  `directory` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `sub_directory` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级分类',
  `sequence` int(11) NOT NULL DEFAULT 1 COMMENT '功能模块下的展示顺序',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `display_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value_enum` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `non_real_time` tinyint(1) DEFAULT NULL COMMENT '非实时标记',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '该配置的说明备注',
  `display` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'true' COMMENT '是否展示',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`config_group`, `group_instance_id`, `name`) USING BTREE,
  INDEX `ix_system_config_createdDate`(`created_time`) USING BTREE,
  INDEX `ix_system_config_lastUpdatedDate`(`last_updated_time`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 14099 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_system_config_instance_origin
-- ----------------------------
DROP TABLE IF EXISTS `basic_system_config_instance_origin`;
CREATE TABLE `basic_system_config_instance_origin`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `module` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '模块',
  `config_group` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '配置组',
  `origin_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '来源类型',
  `actions` varchar(2048) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '来源操作内容',
  `fields_map` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '取值映射',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_module`(`module`, `config_group`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 21 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for basic_tenant
-- ----------------------------
DROP TABLE IF EXISTS `basic_tenant`;
CREATE TABLE `basic_tenant`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `tenant_code` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '库区编号',
  `tenant_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '库区名称',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建人',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新人',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_zone_zone_code`(`tenant_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库区表' ROW_FORMAT = Compact;

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
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`zone_id`, `agv_type_id`, `transport_entity`, `transport_entity_type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_transport_entity_outdated
-- ----------------------------
DROP TABLE IF EXISTS `basic_transport_entity_outdated`;
CREATE TABLE `basic_transport_entity_outdated`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `zone_id` bigint(20) UNSIGNED NOT NULL COMMENT '库区ID',
  `agv_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '机器人类型ID',
  `transport_entity` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '搬送对象区分',
  `transport_entity_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '搬送对象类型Id',
  `checkout_bar_code_flag` tinyint(1) UNSIGNED NOT NULL COMMENT '是否检验条码',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`zone_id`, `agv_type_id`, `transport_entity`, `transport_entity_type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_warehouse
-- ----------------------------
DROP TABLE IF EXISTS `basic_warehouse`;
CREATE TABLE `basic_warehouse`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `warehouse_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '未填仓库编码' COMMENT '仓库编码',
  `warehouse_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '未填仓库名称' COMMENT '仓库名称',
  `address` varchar(120) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地址',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`warehouse_code`, `tenant_id`) USING BTREE COMMENT '仓库编码'
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_zone
-- ----------------------------
DROP TABLE IF EXISTS `basic_zone`;
CREATE TABLE `basic_zone`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `warehouse_id` bigint(20) UNSIGNED NOT NULL COMMENT '仓库编号',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '未填库区编码' COMMENT '库区编码',
  `zone_name` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区名称',
  `zone_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '0' COMMENT '库区类型',
  `mode_id` bigint(20) UNSIGNED DEFAULT NULL COMMENT '作业模式',
  `floor` bigint(20) NOT NULL DEFAULT 1 COMMENT '库区楼层',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '是否启用',
  `stride_floor` varchar(60) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '跨楼层操作',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建App',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '最后更新人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更APP',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `extended_field` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_zone_zone_code`(`zone_code`, `warehouse_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库区表' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for flyway_schema_history
-- ----------------------------
DROP TABLE IF EXISTS `flyway_schema_history`;
CREATE TABLE `flyway_schema_history`  (
  `installed_rank` int(11) NOT NULL,
  `version` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `description` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `script` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `checksum` int(11) DEFAULT NULL,
  `installed_by` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `installed_on` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `execution_time` int(11) NOT NULL,
  `success` tinyint(1) NOT NULL,
  PRIMARY KEY (`installed_rank`) USING BTREE,
  INDEX `flyway_schema_history_s_idx`(`success`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for view_station
-- ----------------------------
DROP TABLE IF EXISTS `view_station`;
CREATE TABLE `view_station`  (
  `warehouse_id` tinyint(4) NOT NULL,
  `station_code` tinyint(4) NOT NULL,
  `station_name` tinyint(4) NOT NULL,
  `station_type` tinyint(4) NOT NULL,
  `zone_name` tinyint(4) NOT NULL,
  `primary_zone_code` tinyint(4) NOT NULL
) ENGINE = MyISAM CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Fixed;

SET FOREIGN_KEY_CHECKS = 1;
