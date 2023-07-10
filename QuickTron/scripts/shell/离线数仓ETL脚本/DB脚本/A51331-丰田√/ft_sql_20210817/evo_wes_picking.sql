/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_picking

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:31:20
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

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
-- Table structure for order_distribute_strategy
-- ----------------------------
DROP TABLE IF EXISTS `order_distribute_strategy`;
CREATE TABLE `order_distribute_strategy`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `specify_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '指定类型',
  `specify_source` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '指定源',
  `specify_target` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '指定目标',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `created_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建用户',
  `created_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
  `last_updated_user` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新用户',
  `last_updated_app` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_specify_source_target_warehouse_id`(`specify_type`, `specify_source`, `specify_target`, `warehouse_id`) USING BTREE,
  INDEX `ix_order_distribute_strategy_specify_type`(`specify_type`) USING BTREE,
  INDEX `ix_order_distribute_strategy_specify_source`(`specify_source`) USING BTREE,
  INDEX `ix_order_distribute_strategy_specify_target`(`specify_target`) USING BTREE,
  INDEX `ix_order_distribute_strategy_warehouse_id`(`warehouse_id`) USING BTREE,
  INDEX `ix_order_distribute_strategy_createdDate`(`created_date`) USING BTREE,
  INDEX `ix_order_distribute_strategy_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '单据指定策略' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order
-- ----------------------------
DROP TABLE IF EXISTS `picking_order`;
CREATE TABLE `picking_order`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '出库单号',
  `sn_unique_assist_key` bigint(20) NOT NULL DEFAULT 0 COMMENT '出库单SN码唯一性辅助键',
  `tenant_id` bigint(20) DEFAULT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `order_type` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '出库单类型',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `printing_times` int(11) NOT NULL DEFAULT 0 COMMENT '拣选开始打印次数',
  `out_of_stock_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '缺货标记',
  `priority_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `priority_value` int(11) DEFAULT NULL COMMENT '优先级值',
  `picking_order_group_id` bigint(20) DEFAULT NULL COMMENT '订单组ID',
  `order_date` datetime(0) DEFAULT NULL COMMENT '订单起始日期',
  `ship_deadline` datetime(0) DEFAULT NULL COMMENT '截至日期',
  `done_date` datetime(0) DEFAULT NULL COMMENT '完成日期',
  `splittable` tinyint(4) DEFAULT NULL COMMENT '是否可分箱',
  `station_id` bigint(20) DEFAULT NULL COMMENT '工作站',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '工作站槽位',
  `station_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '槽位编码',
  `work_count` int(11) DEFAULT NULL COMMENT '作业单数',
  `manual_allot` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否手动分配库存',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `force_work_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '有货先作业标记',
  `short_pick_deliver` tinyint(1) NOT NULL DEFAULT 0 COMMENT '有货先发标记',
  `create_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '建单方式',
  `cancel_reason` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '取消原因',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_picking_order_number_sn`(`picking_order_number`, `sn_unique_assist_key`) USING BTREE,
  INDEX `idx_picking_order_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_state`(`state`) USING BTREE,
  INDEX `fk_picking_order_picking_order_group_id`(`picking_order_group_id`) USING BTREE,
  INDEX `idx_picking_order_external_id`(`external_id`) USING BTREE,
  CONSTRAINT `fk_picking_order_picking_order_group_id` FOREIGN KEY (`picking_order_group_id`) REFERENCES `picking_order_group` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 6688 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_detail
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_detail`;
CREATE TABLE `picking_order_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `external_id` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `picking_order_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `sku_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'sku_code',
  `unit_id` bigint(20) DEFAULT NULL COMMENT '包装单位ID',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `option_quantity` int(11) DEFAULT NULL COMMENT '选择包装数量',
  `quantity` int(11) NOT NULL COMMENT '需拣数量',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际所拣数量',
  `short_pick` int(11) DEFAULT NULL COMMENT '不拣数量，拣货时用户编辑的不用拣的数量',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `level3_inventory_id` bigint(20) DEFAULT NULL COMMENT '三级库存ID',
  `lot_att01` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att02` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att03` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att04` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att05` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att06` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att07` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att08` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att09` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att10` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att11` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `lot_att12` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '批属性',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `origin_quantity` int(11) DEFAULT NULL COMMENT '拣货数量',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `picking_order_detail_external_id_unique`(`external_id`) USING BTREE,
  INDEX `idx_picking_order_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_detail_state`(`state`) USING BTREE,
  INDEX `fk_picking_order_detail_picking_order_id_idx`(`picking_order_id`) USING BTREE,
  CONSTRAINT `fk_picking_order_detail_picking_order_id` FOREIGN KEY (`picking_order_id`) REFERENCES `picking_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 26238 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_detail_property
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_detail_property`;
CREATE TABLE `picking_order_detail_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_detail_id` bigint(20) NOT NULL COMMENT '订单明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '区',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_order_detail_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_detail_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_detail_property_property_name`(`property_name`) USING BTREE,
  INDEX `fk_picking_order_detail_property_picking_order_detail_idx`(`picking_order_detail_id`) USING BTREE,
  CONSTRAINT `fk_picking_order_detail_property_picking_order_detail_id` FOREIGN KEY (`picking_order_detail_id`) REFERENCES `picking_order_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 52455 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单详情属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_fulfill_detail`;
CREATE TABLE `picking_order_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_detail_id` bigint(20) NOT NULL COMMENT '订单明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库明细详情状态',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `pack_id` bigint(20) NOT NULL COMMENT '包装ID',
  `lot_id` bigint(20) NOT NULL COMMENT '批次ID',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '槽位ID',
  `station_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '槽位编码',
  `station_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `quantity` int(11) NOT NULL COMMENT '数量',
  `short_pick` int(11) DEFAULT NULL COMMENT '不拣数量，拣货时用户编辑的不用拣的数量',
  `container_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器号',
  `package_uuid` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '分箱唯一码',
  `level3_inventory_id` bigint(20) NOT NULL COMMENT '三级库存id',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `job_id` bigint(20) NOT NULL COMMENT '任务ID',
  `short_pick_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否缺拣标记',
  `short_pick_reason` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '缺拣原因',
  `location_container_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库容器号',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作人',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `frozen_flag` tinyint(4) DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `picking_order_fulfill_detail_job_container_unique`(`job_id`, `container_code`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_state`(`state`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_skuID`(`sku_id`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_stationSlotID`(`station_slot_id`) USING BTREE,
  INDEX `fk_picking_order_fulfill_detail_picking_order_detail_idx`(`picking_order_detail_id`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_package_uuid`(`package_uuid`) USING BTREE,
  CONSTRAINT `fk_picking_order_fulfill_detail_picking_order_detail_id` FOREIGN KEY (`picking_order_detail_id`) REFERENCES `picking_order_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 25540 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单拣货明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_fulfill_detail_each
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_fulfill_detail_each`;
CREATE TABLE `picking_order_fulfill_detail_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_fulfill_detail_id` bigint(20) NOT NULL COMMENT '出库详情明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `sn` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '序列号',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `quantity` int(11) NOT NULL COMMENT '需拣数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区编号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_each_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_each_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_fulfill_detail_each_picking_order_fulfill`(`picking_order_fulfill_detail_id`) USING BTREE,
  CONSTRAINT `fk_picking_order_fulfill_detail_each_picking_order_fulfill_id` FOREIGN KEY (`picking_order_fulfill_detail_id`) REFERENCES `picking_order_fulfill_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单商品唯一码拣货明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_group
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_group`;
CREATE TABLE `picking_order_group`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `group_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '组号',
  `unique_assist_key` bigint(20) NOT NULL DEFAULT 1 COMMENT '唯一性辅助键',
  `picking_group_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '集合单类型',
  `external_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外部系统ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '组状态',
  `printing_times` int(11) NOT NULL DEFAULT 0 COMMENT '拣选开始打印次数',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `station_id` bigint(20) DEFAULT NULL COMMENT '工作站',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `create_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否手动创建标志',
  `start_date` datetime(0) DEFAULT NULL COMMENT '要求开始时间',
  `deliver_date` datetime(0) DEFAULT NULL COMMENT '要求发货时间',
  `priority_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `priority_value` int(11) DEFAULT NULL COMMENT '优先级值',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_order_group_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_group_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_group_state`(`state`) USING BTREE,
  INDEX `idx_picking_order_group_group_code`(`group_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3536 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单组' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_print_property
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_print_property`;
CREATE TABLE `picking_order_print_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `own_key` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '唯一键',
  `parent_key` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '父唯一键',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `own_key_property_name`(`own_key`, `property_name`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单打印属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_property
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_property`;
CREATE TABLE `picking_order_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `property_name` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名',
  `property_value` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '区',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_order_property_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_property_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_property_property_name`(`property_name`) USING BTREE,
  INDEX `fk_picking_order_property_picking_order_id_idx`(`picking_order_id`) USING BTREE,
  CONSTRAINT `fk_picking_order_property_picking_order_id` FOREIGN KEY (`picking_order_id`) REFERENCES `picking_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 76542 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_order_state_change
-- ----------------------------
DROP TABLE IF EXISTS `picking_order_state_change`;
CREATE TABLE `picking_order_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_order_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '区',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_order_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_order_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_order_state_change_state`(`state`) USING BTREE,
  INDEX `fk_picking_order_state_change_picking_order_id_idx`(`picking_order_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 48347 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库单状态变化' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_work
-- ----------------------------
DROP TABLE IF EXISTS `picking_work`;
CREATE TABLE `picking_work`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_work_number` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单号',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `picking_order_group_id` bigint(20) DEFAULT NULL COMMENT '订单组ID',
  `work_type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '作业单类型',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `out_of_stock_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '缺货标记',
  `picking_order_id` bigint(20) NOT NULL COMMENT '出库单ID',
  `splittable` tinyint(4) DEFAULT NULL COMMENT '是否可分箱',
  `station_id` bigint(20) DEFAULT NULL COMMENT '工作站',
  `station_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '工作站槽位',
  `station_slot_code` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站槽位编码',
  `cross_zone_flag` tinyint(4) NOT NULL DEFAULT 0 COMMENT '是否跨区作业',
  `priority_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '优先级类型',
  `priority_value` int(11) DEFAULT NULL COMMENT '优先级值',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `remark` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_id` bigint(20) DEFAULT NULL COMMENT '区',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '区编号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `ship_deadline` datetime(0) DEFAULT NULL COMMENT '截至日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_picking_work_number`(`picking_work_number`) USING BTREE,
  INDEX `idx_picking_work_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_work_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_work_state`(`state`) USING BTREE,
  INDEX `fk_picking_work_order_group_id`(`picking_order_group_id`) USING BTREE,
  INDEX `fk_picking_work_picking_order_id`(`picking_order_id`) USING BTREE,
  CONSTRAINT `fk_picking_work_order_group_id` FOREIGN KEY (`picking_order_group_id`) REFERENCES `picking_order_group` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT `fk_picking_work_picking_order_id` FOREIGN KEY (`picking_order_id`) REFERENCES `picking_order` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 7045 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '作业单' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_work_detail
-- ----------------------------
DROP TABLE IF EXISTS `picking_work_detail`;
CREATE TABLE `picking_work_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `picking_order_detail_id` bigint(20) NOT NULL COMMENT '出库单明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `level2_inventory_id` bigint(20) NOT NULL COMMENT '二级库存id',
  `level3_inventory_id` bigint(20) DEFAULT NULL COMMENT '三级库存ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `lot_id` bigint(20) DEFAULT NULL COMMENT '批次ID',
  `use_frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '使用冻结库存标记',
  `pack_id` bigint(20) DEFAULT NULL COMMENT '包装ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '状态',
  `quantity` int(11) NOT NULL COMMENT '需拣数量',
  `fulfill_quantity` int(11) DEFAULT NULL COMMENT '实际所拣数量',
  `short_pick` int(11) DEFAULT NULL COMMENT '不拣数量，拣货时用户编辑的不用拣的数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区编号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_work_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_work_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_work_detail_state`(`state`) USING BTREE,
  INDEX `fk_picking_work_detail_picking_work_idx`(`picking_work_id`) USING BTREE,
  CONSTRAINT `fk_picking_work_detail_picking_work_id` FOREIGN KEY (`picking_work_id`) REFERENCES `picking_work` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 26309 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '作业单明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_work_fulfill_detail
-- ----------------------------
DROP TABLE IF EXISTS `picking_work_fulfill_detail`;
CREATE TABLE `picking_work_fulfill_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_work_detail_id` bigint(20) NOT NULL COMMENT '作业单明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库明细详情状态',
  `sku_id` bigint(20) NOT NULL COMMENT '商品ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主代码',
  `work_id` bigint(20) NOT NULL COMMENT '作业单ID',
  `pack_id` bigint(20) NOT NULL COMMENT '包装ID',
  `lot_id` bigint(20) NOT NULL COMMENT '批次ID',
  `station_slot_id` bigint(20) DEFAULT NULL COMMENT '槽位ID',
  `station_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '槽位编码',
  `station_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '工作站编码',
  `quantity` int(11) DEFAULT NULL COMMENT '数量',
  `short_pick` int(11) DEFAULT NULL COMMENT '不拣数量，拣货时用户编辑的不用拣的数量',
  `container_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '容器号',
  `package_uuid` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '分箱唯一码',
  `level3_inventory_id` bigint(20) NOT NULL COMMENT '三级库存id',
  `bucket_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货架编码',
  `bucket_slot_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货位编码',
  `job_id` bigint(20) NOT NULL COMMENT '任务ID',
  `short_pick_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否缺拣标记',
  `short_pick_reason` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '缺拣原因',
  `location_container_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '出库容器号',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作人',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  `frozen_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '冻结标记',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `picking_work_fulfill_detail_job_conatiner_unique`(`job_id`, `container_code`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_state`(`state`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_skuID`(`sku_id`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_stationSlotID`(`station_slot_id`) USING BTREE,
  INDEX `fk_picking_work_fulfill_detail_picking_work_detail_idx`(`picking_work_detail_id`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_package_uuid`(`package_uuid`) USING BTREE,
  CONSTRAINT `fk_picking_work_fulfill_detail_picking_work_detail_id` FOREIGN KEY (`picking_work_detail_id`) REFERENCES `picking_work_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 25543 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '作业单拣货详情' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_work_fulfill_detail_each
-- ----------------------------
DROP TABLE IF EXISTS `picking_work_fulfill_detail_each`;
CREATE TABLE `picking_work_fulfill_detail_each`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `picking_work_fulfill_detail_id` bigint(20) NOT NULL COMMENT '出库详情明细ID',
  `tenant_id` bigint(20) NOT NULL COMMENT '租户',
  `sn` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '序列号',
  `sku_id` bigint(20) NOT NULL COMMENT 'sku id',
  `quantity` int(11) NOT NULL COMMENT '需拣数量',
  `version` int(11) DEFAULT 1 COMMENT '版本号',
  `zone_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '区编号',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `delete_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '删除标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_each_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_each_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_work_fulfill_detail_each_picking_work_fulfill_idx`(`picking_work_fulfill_detail_id`) USING BTREE,
  CONSTRAINT `fk_picking_work_fulfill_detail_each_picking_work_fulfill_detail` FOREIGN KEY (`picking_work_fulfill_detail_id`) REFERENCES `picking_work_fulfill_detail` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '作业单商品唯一码拣货明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for picking_work_state_change
-- ----------------------------
DROP TABLE IF EXISTS `picking_work_state_change`;
CREATE TABLE `picking_work_state_change`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `picking_work_id` bigint(20) NOT NULL COMMENT '出库作业单ID',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `remark` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_picking_work_state_change_created_date`(`created_date`) USING BTREE,
  INDEX `idx_picking_work_state_change_last_updated_date`(`last_updated_date`) USING BTREE,
  INDEX `idx_picking_work_state_change_warehouse_id_picking_work_id`(`warehouse_id`, `picking_work_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 24377 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '出库作业单状态变化' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
