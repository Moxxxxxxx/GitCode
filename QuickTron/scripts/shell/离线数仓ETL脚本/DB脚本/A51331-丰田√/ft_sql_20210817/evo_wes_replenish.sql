/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_replenish

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:31:28
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for direct_put_away_apply_bill
-- ----------------------------
DROP TABLE IF EXISTS `direct_put_away_apply_bill`;
CREATE TABLE `direct_put_away_apply_bill`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `bill_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '申请单号',
  `station_id` bigint(20) NOT NULL COMMENT '工作站',
  `station_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '工作站编码',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `task_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '任务类型',
  `biz_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务类型（在线、离线）',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态(执行中，完成)',
  `bucket_codes` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号列表',
  `bucket_types` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架类型列表',
  `bucket_slot_types` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位类型列表',
  `container_types` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器类型列表',
  `container_qty` int(11) DEFAULT NULL COMMENT '箱数量',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器Code',
  `bucket_layers` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架层列表',
  `min_empty_rate` float DEFAULT NULL COMMENT '最小空置率',
  `max_empty_rate` float DEFAULT NULL COMMENT '最大空置率',
  `dispersion_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位整散类型',
  `sku_attributes` longtext CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '货品属性',
  `precise_match` tinyint(4) DEFAULT NULL COMMENT '是否精确匹配',
  `available_bucket_count` int(11) NOT NULL DEFAULT 0 COMMENT '可用货架数',
  `done_date` datetime(0) DEFAULT NULL COMMENT '完成日期',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `no_append` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否有追加任务',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_direct_put_away_apply_bill_bill_number`(`bill_number`) USING BTREE,
  INDEX `idx_direct_put_away_apply_bill_state`(`state`) USING BTREE,
  INDEX `idx_direct_put_away_apply_bill_created_date`(`created_date`) USING BTREE,
  INDEX `idx_direct_put_away_apply_bill_last_updated_date`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 543 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架申请单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for direct_put_away_apply_bucket
-- ----------------------------
DROP TABLE IF EXISTS `direct_put_away_apply_bucket`;
CREATE TABLE `direct_put_away_apply_bucket`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `direct_put_away_apply_bill_id` bigint(20) NOT NULL COMMENT '申请单ID',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号bucket',
  `bucket_face` int(11) DEFAULT NULL COMMENT '货架面',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器Code',
  `level1_container_status` int(11) DEFAULT NULL COMMENT '一级容器状态',
  `container_qty` int(11) DEFAULT NULL COMMENT '箱数量',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_direct_put_away_apply_bucket_created_date`(`created_date`) USING BTREE,
  INDEX `idx_direct_put_away_apply_bucket_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_direct_put_away_apply_bucket_apply_bill_id_idx`(`direct_put_away_apply_bill_id`) USING BTREE,
  CONSTRAINT `fk_direct_put_away_apply_bucket_apply_bill_id` FOREIGN KEY (`direct_put_away_apply_bill_id`) REFERENCES `direct_put_away_apply_bill` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 4327 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '直接上架申请货架信息' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order`;
CREATE TABLE `replenish_order`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '入库单号',
  `sn_unique_assist_key` bigint(20) NOT NULL DEFAULT 1 COMMENT '入库单SN码唯一性辅助键',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `external_order_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部单号',
  `order_type` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '入库单类型',
  `priority_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `priority_value` int(11) DEFAULT NULL COMMENT '优先级值',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `feedback_state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '反馈状态',
  `order_date` datetime(0) DEFAULT NULL COMMENT '订单起始日期',
  `done_date` datetime(0) DEFAULT NULL COMMENT '完成日期',
  `done_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '完成人',
  `container_count` int(11) DEFAULT NULL COMMENT '箱数',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
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
  UNIQUE INDEX `uidx_replenish_order_number`(`replenish_order_number`, `sn_unique_assist_key`) USING BTREE,
  INDEX `idx_replenish_order_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_state`(`state`) USING BTREE,
  INDEX `idx_replenish_order_external_id`(`external_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 265 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_detail
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_detail`;
CREATE TABLE `replenish_order_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_id` bigint(20) NOT NULL COMMENT '入库单ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '明细状态',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `sku_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品编码',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `unit_id` bigint(20) DEFAULT NULL COMMENT '包装单位ID',
  `level1_container_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '工作站槽位ID',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '待上架数量',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际上架数量',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `lot_att01` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att02` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att03` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att04` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att05` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att06` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att07` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att08` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att09` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att10` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att11` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att12` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_state`(`state`) USING BTREE,
  INDEX `fk_replenish_order_detail_replenish_order_id_idx`(`replenish_order_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_detail_replenish_order_id` FOREIGN KEY (`replenish_order_id`) REFERENCES `replenish_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 4229 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_detail_each
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_detail_each`;
CREATE TABLE `replenish_order_detail_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_detail_id` bigint(20) NOT NULL COMMENT '入库单明细号',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '出库明细详情状态',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `sn` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '序列号',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `original_quantity` int(11) NOT NULL COMMENT '原始所需上架数',
  `received_quantity` int(11) DEFAULT NULL COMMENT '收货数量',
  `defective_quantity` int(11) DEFAULT NULL COMMENT '残次数量',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际上架数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_detail_each_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_each_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_each_state`(`state`) USING BTREE,
  INDEX `idx_replenish_order_detail_each_skuID`(`sku_id`) USING BTREE,
  INDEX `idx_replenish_order_detail_each_sn`(`sn`) USING BTREE,
  INDEX `fk_replenish_order_detail_each_replenish_order_detail_idx`(`replenish_order_detail_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_detail_each_replenish_order_detail_id` FOREIGN KEY (`replenish_order_detail_id`) REFERENCES `replenish_order_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单商品唯一码明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_detail_property
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_detail_property`;
CREATE TABLE `replenish_order_detail_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_id` bigint(20) NOT NULL COMMENT '入库单ID',
  `replenish_order_detail_id` bigint(20) NOT NULL COMMENT '入库单明细ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_detail_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_detail_property_property_name`(`property_name`) USING BTREE,
  INDEX `fk_replenish_order_detail_property_replenish_order_detail_id_idx`(`replenish_order_detail_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_detail_property_replenish_order_detail_id` FOREIGN KEY (`replenish_order_detail_id`) REFERENCES `replenish_order_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 12251 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单明细扩展属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_fulfill_detail`;
CREATE TABLE `replenish_order_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_id` bigint(20) NOT NULL COMMENT '入库单ID',
  `replenish_order_detail_id` bigint(20) NOT NULL COMMENT '入库单明细ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `unit_id` bigint(20) DEFAULT NULL COMMENT '基本包装单位ID',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '三级容器',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号',
  `bucket_slot_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `fulfill_quantity` int(11) NOT NULL COMMENT '数量',
  `mismatch_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '差异数量',
  `quantity_mismatch_reason` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '差异原因',
  `actual_put_away` tinyint(1) DEFAULT 0 COMMENT '是否实际上架',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作人',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `bucket_slot_id` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位ID',
  `station_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上架工作站',
  `done_flag` tinyint(1) DEFAULT 0 COMMENT '明细完成标记',
  `job_id` varchar(70) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'wcs任务Id',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_state`(`state`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_skuID`(`sku_id`) USING BTREE,
  INDEX `fk_replenish_order_fulfill_detail_replenish_order_detail_idx`(`replenish_order_detail_id`) USING BTREE,
  INDEX `fk_replenish_order_fulfill_detail_replenish_order_id`(`replenish_order_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_fulfill_detail_replenish_order_detail_id` FOREIGN KEY (`replenish_order_detail_id`) REFERENCES `replenish_order_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_replenish_order_fulfill_detail_replenish_order_id` FOREIGN KEY (`replenish_order_id`) REFERENCES `replenish_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 8906 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单上架明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_fulfill_detail_each
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_fulfill_detail_each`;
CREATE TABLE `replenish_order_fulfill_detail_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_fulfill_detail_id` bigint(20) NOT NULL COMMENT '上架明细ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `sn` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '序列号',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `fulfill_quantity` int(11) NOT NULL COMMENT '实际上架数量',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号',
  `bucket_slot_id` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位ID',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_each_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_fulfill_detail_each_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_replenish_order_fulfill_detail_each_fulfill_detail_id_idx`(`replenish_order_fulfill_detail_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_fulfill_detail_each_fulfill_detail_id` FOREIGN KEY (`replenish_order_fulfill_detail_id`) REFERENCES `replenish_order_fulfill_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单商品唯一码上架明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_property
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_property`;
CREATE TABLE `replenish_order_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_order_id` bigint(20) NOT NULL COMMENT '入库单ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_property_property_name`(`property_name`) USING BTREE,
  INDEX `fk_replenish_order_property_replenish_order_idx`(`replenish_order_id`) USING BTREE,
  CONSTRAINT `fk_replenish_order_property_replenish_order_id` FOREIGN KEY (`replenish_order_id`) REFERENCES `replenish_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 786 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单扩展属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_order_state_change
-- ----------------------------
DROP TABLE IF EXISTS `replenish_order_state_change`;
CREATE TABLE `replenish_order_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `replenish_order_id` bigint(20) NOT NULL COMMENT '入库单ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_order_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_order_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_order_state_change_warehouse_id_replenish_order_id`(`warehouse_id`, `replenish_order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 782 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单状态变化' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_work
-- ----------------------------
DROP TABLE IF EXISTS `replenish_work`;
CREATE TABLE `replenish_work`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_work_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单号',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `work_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单类型',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `priority_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `priority_value` int(11) DEFAULT NULL COMMENT '优先级值',
  `station_id` bigint(20) DEFAULT NULL COMMENT '工作站',
  `station_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站',
  `done_date` datetime(0) DEFAULT NULL COMMENT '完成日期',
  `done_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '完成人',
  `source_order_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '原单据类型',
  `source_order_id` bigint(20) NOT NULL COMMENT '原单据ID',
  `replenish_mode` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '补货方式',
  `opened` tinyint(1) DEFAULT 0 COMMENT '可追加标记',
  `submit_times` int(11) DEFAULT 1 COMMENT '提交次数',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
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
  UNIQUE INDEX `uidx_replenish_work_number`(`replenish_work_number`) USING BTREE,
  INDEX `idx_replenish_work_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_work_last_updated_date`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2935 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `replenish_work_detail`;
CREATE TABLE `replenish_work_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_work_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `unit_id` bigint(20) DEFAULT NULL COMMENT '基本包装单位ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '工作站槽位ID',
  `quantity` int(11) NOT NULL DEFAULT 0 COMMENT '待上架数量',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际上架数量',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `source_order_id` bigint(20) NOT NULL COMMENT '原单据ID',
  `source_order_detail_id` bigint(20) NOT NULL COMMENT '原始单据明细ID',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '库区编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_work_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_work_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_work_detail_state`(`state`) USING BTREE,
  INDEX `fk_replenish_work_detail_replenish_work_idx`(`replenish_work_id`) USING BTREE,
  CONSTRAINT `fk_replenish_work_detail_replenish_work_id` FOREIGN KEY (`replenish_work_id`) REFERENCES `replenish_work` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 8192 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_work_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `replenish_work_fulfill_detail`;
CREATE TABLE `replenish_work_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_work_detail_id` bigint(20) NOT NULL COMMENT '作业单明细ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库明细详情状态',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际所拣数量',
  `mismatch_quantity` int(11) NOT NULL DEFAULT 0 COMMENT '差异数量',
  `quantity_mismatch_reason` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '差异原因',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作人',
  `source_order_id` bigint(20) NOT NULL COMMENT '原单据ID',
  `source_order_detail_id` bigint(20) NOT NULL COMMENT '原始单据明细ID',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号',
  `bucket_slot_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `bucket_slot_id` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位ID',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_state`(`state`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_skuID`(`sku_id`) USING BTREE,
  INDEX `fk_replenish_work_fulfill_detail_replenish_order_detail_idx`(`replenish_work_detail_id`) USING BTREE,
  CONSTRAINT `fk_replenish_work_fulfill_detail_replenish_work_detail_id` FOREIGN KEY (`replenish_work_detail_id`) REFERENCES `replenish_work_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 4599 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库作业单上架明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_work_fulfill_detail_each
-- ----------------------------
DROP TABLE IF EXISTS `replenish_work_fulfill_detail_each`;
CREATE TABLE `replenish_work_fulfill_detail_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `replenish_work_fulfill_detail_id` bigint(20) NOT NULL COMMENT '出库详情明细ID',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `sn` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '序列号',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `level1_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '一级容器',
  `level2_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '二级容器',
  `level3_container_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '三级容器',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库明细详情状态',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际上架数量',
  `source_order_id` bigint(20) NOT NULL COMMENT '原单据ID',
  `source_order_detail_id` bigint(20) NOT NULL COMMENT '原始单据明细ID',
  `bucket_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架号',
  `bucket_slot_id` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位ID',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '库区ID',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_each_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_work_fulfill_detail_each_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `fk_replenish_work_fulfill_detail_each_fulfill_detail_id_idx`(`replenish_work_fulfill_detail_id`) USING BTREE,
  CONSTRAINT `fk_replenish_work_fulfill_detail_each_fulfill_detail_id` FOREIGN KEY (`replenish_work_fulfill_detail_id`) REFERENCES `replenish_work_fulfill_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库作业单商品唯一码上架明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for replenish_work_state_change
-- ----------------------------
DROP TABLE IF EXISTS `replenish_work_state_change`;
CREATE TABLE `replenish_work_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `replenish_work_id` bigint(20) NOT NULL COMMENT '入库作业单ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_replenish_work_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_replenish_work_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_replenish_work_state_change_warehouse_id_replenish_work_id`(`warehouse_id`, `replenish_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3246 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '入库单状态变化' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
