/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_cyclecount

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:56
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for cycle_count
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count`;
CREATE TABLE `cycle_count`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_number` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '盘点单号',
  `cycle_count_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '盘点单类型(全盘、货品盘点)',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区编号',
  `include_empty_bucket_slot` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否盘点空货位',
  `include_empty_container` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否盘点空容器',
  `operating_mode` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '0' COMMENT '盘点工作模式(明盘、盲盘)',
  `redo_cycle_count_time` int(11) DEFAULT 1 COMMENT '复盘次数',
  `adjustment_generated` tinyint(1) DEFAULT 0 COMMENT '是否已生成调整单',
  `manual` tinyint(1) NOT NULL DEFAULT 0,
  `done_date` datetime(3) DEFAULT NULL COMMENT '完成时间',
  `done_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '完成人',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_cycle_count_number`(`cycle_count_number`) USING BTREE,
  INDEX `idx_cycle_count_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_cycle_count_state`(`state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 106 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_adjust_record
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_adjust_record`;
CREATE TABLE `cycle_count_adjust_record`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `cycle_count_detail_id` bigint(20) NOT NULL COMMENT '盘点明细ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) NOT NULL COMMENT '包装ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `runtime_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '超时数量',
  `actual_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '实际数量',
  `diff_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '差异数量',
  `frozen_flag` tinyint(1) DEFAULT 0 COMMENT '冻结标记',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `operate_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_cycle_count_adjust_record_detailId`(`cycle_count_id`, `cycle_count_detail_id`) USING BTREE,
  INDEX `idx_cycle_count_adjust_record_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_adjust_record_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_adjust_record_cycle_count_detail_id`(`cycle_count_detail_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_adjust_record_cycle_count_detail_id` FOREIGN KEY (`cycle_count_detail_id`) REFERENCES `cycle_count_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_cycle_count_adjust_record_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点调整单' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for cycle_count_by_bucket
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_by_bucket`;
CREATE TABLE `cycle_count_by_bucket`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_face` int(11) DEFAULT NULL COMMENT '货架面',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_by_bucket_cycle_count_id`(`cycle_count_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_by_bucket_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 236 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货架盘点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_by_change
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_by_change`;
CREATE TABLE `cycle_count_by_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `lot_atts` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批次属性',
  `start_time` datetime(3) DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime(3) DEFAULT NULL COMMENT '截至时间',
  `bill_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '单据类型',
  `change_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '动碰模式',
  `bill_number` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '单据编号',
  `bill_id` bigint(20) DEFAULT NULL COMMENT '单据ID',
  `sku_id` bigint(20) DEFAULT NULL COMMENT '商品ID',
  `lot_id` bigint(20) DEFAULT NULL,
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_by_change_cycle_count_id`(`cycle_count_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_by_change_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '动碰盘点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_by_container
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_by_container`;
CREATE TABLE `cycle_count_by_container`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_container_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_container_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_by_container_cycle_count_id`(`cycle_count_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_by_container_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '容器盘点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_by_sku
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_by_sku`;
CREATE TABLE `cycle_count_by_sku`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `lot_atts` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批次属性',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_by_sku_cycle_count_id`(`cycle_count_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_by_sku_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '商品盘点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_detail
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_detail`;
CREATE TABLE `cycle_count_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单id',
  `level3_inventory_id` bigint(20) DEFAULT NULL COMMENT '三级库存ID',
  `inventory_profit_version` int(11) DEFAULT NULL COMMENT '三级库存版本号',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'sku id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `frozen_flag` tinyint(1) DEFAULT 0 COMMENT '冻结标记',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `bucket_face` int(11) DEFAULT NULL COMMENT '货架面',
  `quantity` int(11) UNSIGNED ZEROFILL DEFAULT NULL COMMENT '库存数量',
  `runtime_quantity` int(11) UNSIGNED ZEROFILL DEFAULT NULL COMMENT '超时数量',
  `actual_quantity` int(11) DEFAULT NULL COMMENT '实际数量',
  `diff_quantity` int(11) DEFAULT NULL COMMENT '差异数量',
  `diff_reason` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '盘点差异原因',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作人',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_detail_cycle_count_id`(`cycle_count_id`) USING BTREE,
  INDEX `uidx_cycle_count_detail_level3InventoryId`(`cycle_count_id`) USING BTREE,
  INDEX `idx_cycle_count_id_bucket_slot`(`cycle_count_id`, `bucket_code`, `bucket_slot_code`) USING BTREE,
  CONSTRAINT `fk_cycle_count_detail_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 96549 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点明细单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_detail_property
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_detail_property`;
CREATE TABLE `cycle_count_detail_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单id',
  `cycle_count_detail_id` bigint(20) NOT NULL COMMENT '盘点明细ID',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_detail_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_detail_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_detail_property_cycle_count_id_idx`(`cycle_count_id`) USING BTREE,
  INDEX `fk_cycle_count_detail_property_cycle_count_detail_id`(`cycle_count_detail_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_detail_property_cycle_count_detail_id` FOREIGN KEY (`cycle_count_detail_id`) REFERENCES `cycle_count_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_cycle_count_detail_property_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点明细扩展属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_profit_control
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_profit_control`;
CREATE TABLE `cycle_count_profit_control`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `level3_inventory_id` bigint(20) NOT NULL COMMENT '三级库存ID',
  `version` bigint(20) NOT NULL DEFAULT 1 COMMENT '版本号',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_cycle_count_profit_control_inventoryId`(`level3_inventory_id`) USING BTREE,
  INDEX `idx_cycle_count_profit_control_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_profit_control_last_updated_date`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点盘盈控制' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_property
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_property`;
CREATE TABLE `cycle_count_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_cycle_count_property_property_name`(`property_name`) USING BTREE,
  INDEX `fk_cycle_count_property_cycle_count_id`(`cycle_count_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_property_cycle_count_id` FOREIGN KEY (`cycle_count_id`) REFERENCES `cycle_count` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点单扩展属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_state_change
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_state_change`;
CREATE TABLE `cycle_count_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_cycle_count_state_change_warehouse_id_cycle_count_id`(`warehouse_id`, `cycle_count_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 410 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点单状态变化' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_work
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_work`;
CREATE TABLE `cycle_count_work`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_id` bigint(20) NOT NULL COMMENT '盘点单ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区编号',
  `zone_mode` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库区作业模式',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `cycle_count_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '盘点单类型',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `operating_mode` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '0' COMMENT '盘点工作模式',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_work_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_work_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_work_cycle_count_idx`(`cycle_count_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 106 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_work_detail`;
CREATE TABLE `cycle_count_work_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_work_id` bigint(20) NOT NULL COMMENT '盘点作业单id',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_face` int(11) DEFAULT NULL COMMENT '货架面',
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '二级容器',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_work_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_work_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_cycle_count_work_detail_cycle_count_work_idx`(`cycle_count_work_id`) USING BTREE,
  CONSTRAINT `fk_cycle_count_work_detail_cycle_count_work_id` FOREIGN KEY (`cycle_count_work_id`) REFERENCES `cycle_count_work` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 6489 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle_count_work_state_change
-- ----------------------------
DROP TABLE IF EXISTS `cycle_count_work_state_change`;
CREATE TABLE `cycle_count_work_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `cycle_count_work_id` bigint(20) NOT NULL COMMENT '盘点作业单ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_cycle_count_work_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_cycle_count_work_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_cycle_count_work_state_change_warehouse_id_work_id`(`warehouse_id`, `cycle_count_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 307 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '盘点单状态变化' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for inventory_adjustment
-- ----------------------------
DROP TABLE IF EXISTS `inventory_adjustment`;
CREATE TABLE `inventory_adjustment`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `adjustment_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调整单号',
  `adjustment_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调整类型',
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_inventory_adjustment_number`(`warehouse_id`, `adjustment_number`) USING BTREE,
  INDEX `idx_inventory_adjustment_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_inventory_adjustment_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存调整单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for inventory_adjustment_detail
-- ----------------------------
DROP TABLE IF EXISTS `inventory_adjustment_detail`;
CREATE TABLE `inventory_adjustment_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `inventory_adjustment_id` bigint(20) NOT NULL COMMENT '调整单号',
  `from_owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货主代码',
  `from_sku_id` bigint(20) DEFAULT NULL COMMENT '源SKU',
  `from_lot_id` bigint(20) DEFAULT NULL COMMENT '源批次ID',
  `from_pack_id` bigint(20) DEFAULT NULL COMMENT '源包装ID',
  `from_frozen_flag` tinyint(1) DEFAULT 0 COMMENT '源冻结标记',
  `from_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `from_bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货架编码',
  `from_bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货位编码',
  `from_level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源一级容器',
  `from_level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源二级容器',
  `from_quantity` int(11) DEFAULT 0 COMMENT '源变化数量',
  `to_owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货主代码',
  `to_sku_id` bigint(20) DEFAULT NULL COMMENT '目标SKU',
  `to_lot_id` bigint(20) DEFAULT NULL COMMENT '目标批次ID',
  `to_pack_id` bigint(20) DEFAULT NULL COMMENT '目标包装ID',
  `to_frozen_flag` tinyint(1) DEFAULT 0 COMMENT '目标冻结标记',
  `to_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `to_bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货架编码',
  `to_bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位编码',
  `to_level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标一级容器',
  `to_level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标二级容器',
  `to_quantity` int(11) DEFAULT 0 COMMENT '目标变化数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_inventory_adjustment_detail_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_inventory_adjustment_detail_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `fk_inventory_adjustment_detail_adjustmentId`(`inventory_adjustment_id`) USING BTREE,
  CONSTRAINT `fk_inventory_adjustment_detail_adjustmentId` FOREIGN KEY (`inventory_adjustment_id`) REFERENCES `inventory_adjustment` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存调整单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for inventory_transaction
-- ----------------------------
DROP TABLE IF EXISTS `inventory_transaction`;
CREATE TABLE `inventory_transaction`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `inventory_level` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '库存级别',
  `inventory_id` bigint(20) DEFAULT NULL COMMENT '相应级别库存ID',
  `biz_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务类型',
  `biz_idempotent_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务幂等健',
  `biz_bill_id` bigint(20) DEFAULT NULL COMMENT '来源单据id',
  `biz_bill_detail_id` bigint(20) DEFAULT NULL COMMENT '来源单据明细id',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'sku id',
  `sn_enabled` tinyint(1) DEFAULT NULL COMMENT '启用sn码管理',
  `lot_id` bigint(20) DEFAULT -1 COMMENT '商品批次',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装规格',
  `frozen_flag` tinyint(1) DEFAULT 0 COMMENT '冻结标记',
  `quantity` int(11) DEFAULT 0 COMMENT '库存数量',
  `out_locked_quantity` int(11) DEFAULT 0 COMMENT '锁定数量',
  `in_locked_quantity` int(11) DEFAULT 0 COMMENT '待移入数量',
  `post_quantity` int(11) DEFAULT 0 COMMENT '更新后数量',
  `post_out_locked_quantity` int(11) DEFAULT 0 COMMENT '更新后锁定数量',
  `post_in_locked_quantity` int(11) DEFAULT 0 COMMENT '更新后锁定数量',
  `transaction_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '事务时间',
  `state` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态（NEW/DONE/ERROR）',
  `correlation_id` bigint(20) DEFAULT NULL COMMENT '关联号',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ix_inventory_transaction_idempotentId`(`biz_type`, `biz_idempotent_id`) USING BTREE,
  INDEX `ix_inventory_transaction_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_inventory_transaction_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_inventory_transaction_state`(`warehouse_id`, `state`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存事务' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for inventory_transaction_each
-- ----------------------------
DROP TABLE IF EXISTS `inventory_transaction_each`;
CREATE TABLE `inventory_transaction_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `inventory_transaction_id` bigint(20) NOT NULL COMMENT '库存事务id',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `level3_inventory_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位商品id',
  `sn_id` bigint(20) NOT NULL COMMENT 'SN主键',
  `sn` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '商品SN码',
  `quantity` int(11) NOT NULL COMMENT '库存数量',
  `locked_quantity` int(11) DEFAULT NULL COMMENT '锁定数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_inventory_transaction_each_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_inventory_transaction_each_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `inventory_transaction_each_ibfk_1`(`inventory_transaction_id`) USING BTREE,
  CONSTRAINT `inventory_transaction_each_ibfk_1` FOREIGN KEY (`inventory_transaction_id`) REFERENCES `inventory_transaction` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存事务明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level1_inventory
-- ----------------------------
DROP TABLE IF EXISTS `level1_inventory`;
CREATE TABLE `level1_inventory`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主',
  `sku_id` bigint(20) NOT NULL COMMENT '商品id',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '库存数量',
  `out_locked_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '锁定数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ix_level1_inventory_key`(`warehouse_id`, `owner_code`, `sku_id`, `frozen_flag`) USING BTREE,
  INDEX `ix_level1_inventory_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_level1_inventory_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '一级库存' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level1_inventory_daily_report
-- ----------------------------
DROP TABLE IF EXISTS `level1_inventory_daily_report`;
CREATE TABLE `level1_inventory_daily_report`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `reprort_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '日期',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库id',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `initial_quantity` int(11) DEFAULT NULL COMMENT '初期库存',
  `in_quantity` int(11) DEFAULT NULL COMMENT '入仓库存',
  `out_quantity` int(11) DEFAULT NULL COMMENT '出仓库存',
  `profit_quantity` int(11) DEFAULT NULL COMMENT '盘盈库存',
  `loss_quantity` int(11) DEFAULT NULL COMMENT '盘亏库存',
  `final_quantity` int(11) DEFAULT NULL COMMENT '期末库存',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_level1_inventory_daily_report_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_level1_inventory_daily_report_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '一级库存日报' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level2_inventory
-- ----------------------------
DROP TABLE IF EXISTS `level2_inventory`;
CREATE TABLE `level2_inventory`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '分区编号',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主',
  `sku_id` bigint(20) NOT NULL COMMENT '商品id',
  `lot_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '批次id',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '库存数量',
  `out_locked_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '锁定数量',
  `in_locked_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '待移入数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_level2_inventory_key`(`warehouse_id`, `sku_id`, `frozen_flag`, `lot_id`, `zone_code`) USING BTREE,
  INDEX `uix_level2_inventory_zoneCode_ownCode`(`warehouse_id`, `owner_code`, `zone_code`) USING BTREE,
  INDEX `ix_level2_inventory_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_level2_inventory_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '二级库存' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level2_inventory_daily_report
-- ----------------------------
DROP TABLE IF EXISTS `level2_inventory_daily_report`;
CREATE TABLE `level2_inventory_daily_report`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `reprort_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '日期',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库id',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '分区编号',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '批次id',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装id',
  `initial_quantity` int(11) DEFAULT NULL COMMENT '初期库存',
  `in_quantity` int(11) DEFAULT NULL COMMENT '入仓库存',
  `out_quantity` int(11) DEFAULT NULL COMMENT '出仓库存',
  `profit_quantity` int(11) DEFAULT NULL COMMENT '盘盈库存',
  `loss_quantity` int(11) DEFAULT NULL COMMENT '盘亏库存',
  `final_quantity` int(11) DEFAULT NULL COMMENT '期末库存',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_level2_inventory_daily_report_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_level2_inventory_daily_report_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '二级库存日报' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level3_inventory
-- ----------------------------
DROP TABLE IF EXISTS `level3_inventory`;
CREATE TABLE `level3_inventory`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位编码',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '二级容器',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'skuid',
  `lot_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '商品批次',
  `pack_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '包装规格',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '库存数量',
  `out_locked_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '锁定数量',
  `in_locked_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '待移入数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_level3_inventory_key`(`warehouse_id`, `bucket_slot_code`, `level1_container_code`, `level2_container_code`, `owner_code`, `sku_id`, `lot_id`, `pack_id`, `frozen_flag`) USING BTREE,
  INDEX `ix_level3_inventory_sku`(`warehouse_id`, `sku_id`, `lot_id`, `frozen_flag`) USING BTREE,
  INDEX `ix_level3_inventory_bucketSlotCode`(`warehouse_id`, `bucket_slot_code`) USING BTREE,
  INDEX `ix_level3_inventory_bucketCode`(`warehouse_id`, `bucket_code`, `owner_code`) USING BTREE,
  INDEX `ix_level3_inventory_level1ContainerCode`(`warehouse_id`, `level1_container_code`) USING BTREE,
  INDEX `ix_level3_inventory_level2ContainerCode`(`warehouse_id`, `level2_container_code`) USING BTREE,
  INDEX `ix_level3_inventory_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_level3_inventory_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货位商品（三级库存）' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for level3_inventory_each
-- ----------------------------
DROP TABLE IF EXISTS `level3_inventory_each`;
CREATE TABLE `level3_inventory_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `level3_inventory_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级库存ID',
  `sn_id` bigint(20) NOT NULL COMMENT '商品SN码主键',
  `sn` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '商品SN码',
  `quantity` int(11) NOT NULL COMMENT '库存数量',
  `locked_quantity` int(11) DEFAULT NULL COMMENT '锁定数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_level3_inventory_each_sn_id`(`warehouse_id`, `sn_id`) USING BTREE,
  INDEX `ix_level3_inventory_each_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_level3_inventory_each_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `sku_id`(`sku_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货位商品SN码' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for lot
-- ----------------------------
DROP TABLE IF EXISTS `lot`;
CREATE TABLE `lot`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `sku_id` bigint(20) NOT NULL COMMENT '商品id',
  `lot_att01` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性01',
  `lot_att02` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性02',
  `lot_att03` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性03',
  `lot_att04` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性04',
  `lot_att05` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性05',
  `lot_att06` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性06',
  `lot_att07` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性07',
  `lot_att08` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性08',
  `lot_att09` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性09',
  `lot_att10` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性10',
  `lot_att11` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性11',
  `lot_att12` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '批次属性12',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_lot_skuIdAndAllAtt`(`warehouse_id`, `sku_id`, `lot_att01`, `lot_att02`, `lot_att03`, `lot_att04`, `lot_att05`, `lot_att06`, `lot_att07`, `lot_att08`, `lot_att09`, `lot_att10`, `lot_att11`, `lot_att12`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '批次' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for sku_sn
-- ----------------------------
DROP TABLE IF EXISTS `sku_sn`;
CREATE TABLE `sku_sn`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库ID',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `parent_id` bigint(20) DEFAULT NULL COMMENT '上级包装SN码ID',
  `sn` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '商品SN码',
  `sn_unique_assist_key` bigint(20) NOT NULL DEFAULT 1 COMMENT '商品SN码唯一性辅助键',
  `pack_level` tinyint(2) DEFAULT NULL COMMENT '包装规格层级',
  `pack_item_uom` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '包装规格子项',
  `state` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `replenish_order_id` bigint(20) DEFAULT NULL COMMENT '入库单id',
  `replenish_order_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '入库单号',
  `replenish_order_detail_id` bigint(20) DEFAULT NULL COMMENT '入库单明细id',
  `picking_order_id` bigint(20) DEFAULT NULL COMMENT '出库单id',
  `picking_order_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库单号',
  `picking_order_detail_id` bigint(20) DEFAULT NULL COMMENT '出库单明细id',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `delete_flag` tinyint(1) DEFAULT NULL COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_sku_sn_sn`(`sn`, `sn_unique_assist_key`) USING BTREE,
  INDEX `ix_sku_sn_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `ix_sku_sn_createdDate`(`created_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '商品SN码' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
