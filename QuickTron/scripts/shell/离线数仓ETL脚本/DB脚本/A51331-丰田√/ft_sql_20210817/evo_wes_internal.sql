/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_internal

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:31:06
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for hot_move
-- ----------------------------
DROP TABLE IF EXISTS `hot_move`;
CREATE TABLE `hot_move`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `hot_move_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '任务号',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `status` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '单据状态 ',
  `move_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '移位类型',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_hot_move_number`(`hot_move_number`) USING BTREE,
  INDEX `idx_hot_move_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `idx_hot_move_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '自动移位' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_all_sku
-- ----------------------------
DROP TABLE IF EXISTS `tally_all_sku`;
CREATE TABLE `tally_all_sku`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `sku_id` int(11) NOT NULL COMMENT '商品id',
  `lot_id` int(11) NOT NULL COMMENT '批次属性id',
  `pack_id` int(11) NOT NULL COMMENT '包装规格id',
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `owner_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主编码',
  `all_station_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `recommend_rate` float(5, 2) NOT NULL COMMENT '推荐度',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_sku`(`sku_id`, `lot_id`, `pack_id`, `zone_code`, `owner_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '正在理货商品' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_apply
-- ----------------------------
DROP TABLE IF EXISTS `tally_apply`;
CREATE TABLE `tally_apply`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `zone_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `warehouse_id` int(11) NOT NULL COMMENT '仓库ID',
  `tally_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '理货类型',
  `max_bucket_slot_num` int(11) DEFAULT NULL COMMENT '最大释放库位数',
  `bucket_type_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架类型编码',
  `bucket_slot_type_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位类型',
  `slot_used_capacity` int(3) DEFAULT NULL COMMENT '库位使用容量',
  `source_slot_usage` float(3, 2) DEFAULT NULL COMMENT '源库位使用率',
  `source_slot_capacity_usage` float(3, 2) DEFAULT NULL COMMENT '源库位容量使用率',
  `tally_work_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'ON_LINE' COMMENT '理货作业类型(ON_LINE/OFF_LINE)',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '申请状态',
  `complete_state` int(11) NOT NULL COMMENT '申请完成状态',
  `station_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货申请单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_order
-- ----------------------------
DROP TABLE IF EXISTS `tally_order`;
CREATE TABLE `tally_order`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_order_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货单号',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `tally_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货类型',
  `external_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `station_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `goal_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `tally_apply_id` bigint(20) DEFAULT NULL COMMENT '申请单ID',
  `tally_work_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'ON_LINE' COMMENT '理货作业类型(ON_LINE/OFF_LINE)',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_order_number`(`warehouse_id`, `tally_order_number`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_type`(`tally_type`) USING BTREE,
  INDEX `idx_station_code`(`warehouse_id`, `station_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_order_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_order_detail`;
CREATE TABLE `tally_order_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_order_id` bigint(20) NOT NULL COMMENT '理货单ID',
  `detail_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `dispersion_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '整散类型',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) NOT NULL COMMENT '商品批次',
  `pack_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '包装规格',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '槽位ID',
  `station_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '槽位编码',
  `plan_total_quantity` int(11) DEFAULT NULL,
  `real_total_quantity` int(11) DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_sku`(`tally_order_id`, `detail_type`, `sku_id`, `lot_id`, `pack_id`, `state`) USING BTREE,
  INDEX `idx_tally_order_id`(`tally_order_id`, `state`) USING BTREE,
  INDEX `idx_type`(`tally_order_id`, `detail_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_order_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_order_fulfill_detail`;
CREATE TABLE `tally_order_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_order_detail_id` bigint(20) NOT NULL COMMENT '理货明细ID',
  `type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '理货类型',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位编码',
  `pack_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `plan_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '计划数量',
  `real_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实际数量',
  `shortage_reason` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '缺货原因',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作人',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_fulfill_detail`(`tally_order_detail_id`, `bucket_code`, `bucket_slot_code`, `type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货单货位明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_shelf_scheduling
-- ----------------------------
DROP TABLE IF EXISTS `tally_shelf_scheduling`;
CREATE TABLE `tally_shelf_scheduling`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `station_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调度状态',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_station_code`(`station_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_shelf_scheduling_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_shelf_scheduling_detail`;
CREATE TABLE `tally_shelf_scheduling_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_shelf_scheduling_id` bigint(20) NOT NULL COMMENT '货架调度id',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货架',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调度状态',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_tally_shelf_scheduling_id`(`tally_shelf_scheduling_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度货架明细表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_shelf_scheduling_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_shelf_scheduling_fulfill_detail`;
CREATE TABLE `tally_shelf_scheduling_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_shelf_scheduling_detail_id` bigint(20) NOT NULL COMMENT '货架调度货架明细id',
  `bucket_face` bigint(20) NOT NULL COMMENT '货架面',
  `state` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调度状态',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_tally_shelf_scheduling_detail_id`(`tally_shelf_scheduling_detail_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架调度货架面明细表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work
-- ----------------------------
DROP TABLE IF EXISTS `tally_work`;
CREATE TABLE `tally_work`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_work_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货作业单号',
  `tally_order_id` bigint(20) NOT NULL COMMENT '理货单ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `tally_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '理货类型',
  `station_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区编码',
  `goal_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `tally_apply_id` bigint(20) DEFAULT NULL COMMENT '申请单ID',
  `tally_work_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'ON_LINE' COMMENT '理货作业类型(ON_LINE/OFF_LINE)',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_order_number`(`warehouse_id`, `tally_work_number`) USING BTREE,
  INDEX `idx_tally_order_id`(`tally_order_id`) USING BTREE,
  INDEX `idx_state`(`state`) USING BTREE,
  INDEX `idx_type`(`tally_type`) USING BTREE,
  INDEX `idx_station_code`(`warehouse_id`, `station_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_work_detail`;
CREATE TABLE `tally_work_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_order_detail_id` bigint(20) NOT NULL COMMENT '理货明细ID',
  `tally_work_id` bigint(20) NOT NULL COMMENT '理货作业单ID',
  `work_detail_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `dispersion_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '整散类型',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) NOT NULL COMMENT '商品批次',
  `pack_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '包装规格',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '槽位ID',
  `station_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '槽位编码',
  `plan_total_quantity` int(11) DEFAULT NULL,
  `real_total_quantity` int(11) DEFAULT NULL,
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_sku`(`tally_work_id`, `work_detail_type`, `sku_id`, `lot_id`, `pack_id`, `state`) USING BTREE,
  INDEX `idx_tally_order_id`(`tally_work_id`, `state`) USING BTREE,
  INDEX `idx_type`(`tally_work_id`, `work_detail_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tally_work_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `tally_work_fulfill_detail`;
CREATE TABLE `tally_work_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `tally_work_detail_id` bigint(20) NOT NULL COMMENT '理货作业明细ID',
  `type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '作业单类型',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位编码',
  `pack_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `plan_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '计划数量',
  `real_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实际数量',
  `shortage_reason` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '缺货原因',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作人',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_fulfill_detail`(`tally_work_detail_id`, `bucket_code`, `bucket_slot_code`, `type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '理货作业单货位明细' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
