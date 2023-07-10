/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_inventory

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:31:14
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for inventory_adjustment
-- ----------------------------
DROP TABLE IF EXISTS `inventory_adjustment`;
CREATE TABLE `inventory_adjustment`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `adjustment_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调整单号',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `adjustment_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '调整类型',
  `source_bill_no` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '来源单据号',
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
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
) ENGINE = InnoDB AUTO_INCREMENT = 23 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存调整单' ROW_FORMAT = Dynamic;

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
  `from_bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源货位编码',
  `from_level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源一级容器',
  `from_level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '源二级容器',
  `from_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '源库存数',
  `from_feedback_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '源反馈数量',
  `to_owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '目标货主代码',
  `to_sku_id` bigint(20) NOT NULL COMMENT '目标SKU',
  `to_lot_id` bigint(20) DEFAULT NULL COMMENT '目标批次ID',
  `to_pack_id` bigint(20) DEFAULT NULL COMMENT '目标包装ID',
  `to_frozen_flag` tinyint(1) DEFAULT 0 COMMENT '目标冻结标记',
  `to_bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标货位编码',
  `to_level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标一级容器',
  `to_level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '目标二级容器',
  `to_level3_inventory_id` bigint(20) DEFAULT NULL,
  `to_inventory_profit_version` int(11) DEFAULT 0,
  `to_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '目标库存数',
  `to_feedback_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '目标反馈数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `to_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `from_zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `from_bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `to_bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_inventory_adjustment_detail_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_inventory_adjustment_detail_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `fk_inventory_adjustment_detail_adjustmentId`(`inventory_adjustment_id`) USING BTREE,
  CONSTRAINT `fk_inventory_adjustment_detail_adjustmentId` FOREIGN KEY (`inventory_adjustment_id`) REFERENCES `inventory_adjustment` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 29 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存调整单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for inventory_snapshot
-- ----------------------------
DROP TABLE IF EXISTS `inventory_snapshot`;
CREATE TABLE `inventory_snapshot`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `snapshot_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '快照ID',
  `snapshot_unit` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '快照粒度',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `sku_id` bigint(20) NOT NULL COMMENT '商品id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次id',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装id',
  `frozen_flag` tinyint(1) DEFAULT NULL COMMENT '冻结标记',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '库存数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `ix_inventory_snapshot_id`(`snapshot_id`) USING BTREE,
  INDEX `ix_inventory_snapshot_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_inventory_snapshot_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存快照' ROW_FORMAT = Dynamic;

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
  `biz_type_group` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '新业务类型',
  `inventory_action_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库存变化类型',
  `biz_idempotent_id` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '业务幂等健',
  `biz_bill_id` bigint(20) DEFAULT 0 COMMENT '来源单据id',
  `biz_bill_number` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '来源单据number',
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
  INDEX `ix_inventory_transaction_state`(`warehouse_id`, `state`) USING BTREE,
  INDEX `ix_inventory_transaction_biz_bill_id`(`biz_bill_id`) USING BTREE,
  INDEX `ix_inventory_transaction_biz_bill_number`(`biz_bill_number`) USING BTREE,
  INDEX `ix_transaction_bill`(`inventory_level`, `inventory_id`, `biz_bill_number`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 322544 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '库存事务' ROW_FORMAT = Dynamic;

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
) ENGINE = InnoDB AUTO_INCREMENT = 1703 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '一级库存' ROW_FORMAT = Dynamic;

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
  `consistent_flag` tinyint(1) NOT NULL DEFAULT 1 COMMENT '一致性标记',
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
) ENGINE = InnoDB AUTO_INCREMENT = 5448 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '二级库存' ROW_FORMAT = Dynamic;

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
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货位编码',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '二级容器',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'skuid',
  `lot_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '商品批次',
  `pack_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '包装规格',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  `quantity` int(11) DEFAULT 0 COMMENT '库存数量',
  `out_locked_quantity` int(11) DEFAULT 0 COMMENT '锁定数量',
  `in_locked_quantity` int(11) DEFAULT 0 COMMENT '待移入数量',
  `profit_version` int(11) NOT NULL DEFAULT 1 COMMENT '盘盈版本号',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `zone_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'ZoneCode',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_level3_inventory_key`(`warehouse_id`, `bucket_slot_code`, `level1_container_code`, `level2_container_code`, `owner_code`, `sku_id`, `lot_id`, `pack_id`, `frozen_flag`) USING BTREE,
  INDEX `ix_level3_inventory_sku`(`warehouse_id`, `sku_id`, `lot_id`, `frozen_flag`) USING BTREE,
  INDEX `ix_level3_inventory_bucketSlotCode`(`warehouse_id`, `bucket_slot_code`) USING BTREE,
  INDEX `ix_level3_inventory_bucketCode`(`warehouse_id`, `bucket_code`, `owner_code`) USING BTREE,
  INDEX `ix_level3_inventory_level1ContainerCode`(`warehouse_id`, `level1_container_code`) USING BTREE,
  INDEX `ix_level3_inventory_level2ContainerCode`(`warehouse_id`, `level2_container_code`) USING BTREE,
  INDEX `ix_level3_inventory_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_level3_inventory_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 36454 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '货位商品（三级库存）' ROW_FORMAT = Dynamic;

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
  `warehouse_id` bigint(20) DEFAULT NULL COMMENT '仓库id',
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
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_lot_skuIdAndAllAtt`(`sku_id`, `lot_att01`, `lot_att02`, `lot_att03`, `lot_att04`, `lot_att05`, `lot_att06`, `lot_att07`, `lot_att08`, `lot_att09`, `lot_att10`, `lot_att11`, `lot_att12`) USING BTREE,
  UNIQUE INDEX `uix_lot_externalId`(`external_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1707 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '批次' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for lot_attribute
-- ----------------------------
DROP TABLE IF EXISTS `lot_attribute`;
CREATE TABLE `lot_attribute`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `attribute_name` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `attribute_desc` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性描述',
  `source_attribute_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '源属性名',
  `attribute_format` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性格式',
  `principal` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否主要属性',
  `extended` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否扩展属性(上游系统不包括)',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否启用',
  `editable` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否可编辑',
  `barcode_enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否有批次编码',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uix_lot_attr_name`(`attribute_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 13 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '批次属性' ROW_FORMAT = Dynamic;

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
