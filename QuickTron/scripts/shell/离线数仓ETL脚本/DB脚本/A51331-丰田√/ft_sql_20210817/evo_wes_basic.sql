/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : evo_wes_basic

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:30:49
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

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
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_barcode`(`bar_code`, `sku_id`, `entity_id`, `entity_object`) USING BTREE,
  INDEX `ix_sku_code`(`sku_id`) USING BTREE,
  INDEX `ix_entity_code`(`owner_id`, `entity_object`, `entity_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9355 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_optional_pack
-- ----------------------------
DROP TABLE IF EXISTS `basic_optional_pack`;
CREATE TABLE `basic_optional_pack`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `sku_id` bigint(20) UNSIGNED NOT NULL COMMENT '商品ID',
  `pack_id` bigint(20) UNSIGNED NOT NULL COMMENT '包装规格ID',
  `default_pack_flag` tinyint(1) UNSIGNED NOT NULL COMMENT '是否默认包装规格',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`sku_id`, `pack_id`, `default_pack_flag`) USING BTREE,
  INDEX `ix_code`(`sku_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9359 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_owner
-- ----------------------------
DROP TABLE IF EXISTS `basic_owner`;
CREATE TABLE `basic_owner`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `owner_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主编码',
  `tenant_id` bigint(20) NOT NULL DEFAULT -1 COMMENT '租户ID',
  `owner_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货主名称',
  `contact` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '联系方式',
  `address` varchar(120) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '地址',
  `owner_type` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主类型',
  `super_owner_id` bigint(20) DEFAULT NULL COMMENT '上级货主Id',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '备注',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用名称',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新应用名称',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_code`(`tenant_id`, `owner_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_pack
-- ----------------------------
DROP TABLE IF EXISTS `basic_pack`;
CREATE TABLE `basic_pack`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `pack_code` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '包装规格编码',
  `owner_id` bigint(20) NOT NULL COMMENT '货主ID',
  `pack_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`pack_code`, `owner_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_pack_capacity
-- ----------------------------
DROP TABLE IF EXISTS `basic_pack_capacity`;
CREATE TABLE `basic_pack_capacity`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `sku_id` bigint(20) UNSIGNED NOT NULL COMMENT '商品ID',
  `dimension` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '匹配维度',
  `dimension_type_id` bigint(20) UNSIGNED NOT NULL COMMENT '匹配类型ID',
  `capacity` int(255) UNSIGNED NOT NULL COMMENT '容量',
  `dispersion_type` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '整散类型',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`sku_id`, `dimension`, `dimension_type_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2282 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

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
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '是否启用',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_packId_unitLevel`(`pack_id`, `pack_unit_level`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for basic_sku
-- ----------------------------
DROP TABLE IF EXISTS `basic_sku`;
CREATE TABLE `basic_sku`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `owner_id` bigint(20) UNSIGNED NOT NULL COMMENT '所属货主',
  `sku_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '商品编码',
  `sku_name` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品名称',
  `batch_enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '是否启用批次',
  `sn_enabled` tinyint(1) UNSIGNED NOT NULL COMMENT '是否有唯一编码',
  `lot_barcode_enabled` tinyint(1) UNSIGNED NOT NULL DEFAULT 0 COMMENT '是否有批次编码',
  `over_weight_flag` tinyint(1) DEFAULT NULL COMMENT '是否超重',
  `upper_limit_quantity` int(11) DEFAULT 200 COMMENT '库存上限',
  `lower_limit_quantity` int(11) DEFAULT 10 COMMENT '库存下限',
  `image_url` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '外形图片地址',
  `expiration_date` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '保质期',
  `near_expiration_date` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '近效期',
  `spec` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '货品规格',
  `supplier` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '供应商名称',
  `abc_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'abc分类',
  `major_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品大类',
  `medium_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品中类',
  `minor_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '商品小类',
  `mutex_category` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '互斥分类',
  `state` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'effective' COMMENT '状态',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '扩展字段',
  `created_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '创建者',
  `created_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `created_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `last_updated_user` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'EVO_BASIC' COMMENT '最后更新者',
  `last_updated_app` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后更新应用',
  `last_updated_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新日期',
  `extended_field` longtext CHARACTER SET utf8 COLLATE utf8_general_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `ux_basic_sku_code`(`owner_id`, `sku_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9358 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for dictionary
-- ----------------------------
DROP TABLE IF EXISTS `dictionary`;
CREATE TABLE `dictionary`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `owner_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '货主代码',
  `dictionary_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字典代码',
  `dictionary_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字典名称',
  `description` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '描述',
  `dictionary_group` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'default' COMMENT '字典分组',
  `sort_num` int(11) NOT NULL DEFAULT 1 COMMENT '排序字段',
  `i18n_required` tinyint(1) NOT NULL DEFAULT 0 COMMENT '需要国际化',
  `hierarchical_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '分层的',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_dictionary_dictionaryCode`(`warehouse_id`, `owner_code`, `dictionary_code`) USING BTREE,
  INDEX `idx_dictionary_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_dictionary_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 115 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'WES字典' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dictionary_item
-- ----------------------------
DROP TABLE IF EXISTS `dictionary_item`;
CREATE TABLE `dictionary_item`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `dictionary_id` bigint(20) NOT NULL COMMENT '字典ID',
  `item_key` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字典键',
  `item_value` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '字典值',
  `item_desc` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '字典描述',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `sort_num` int(11) NOT NULL DEFAULT 1 COMMENT '排序字段',
  `parent_item_id` bigint(20) DEFAULT NULL COMMENT '上级ItemID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_dictionary_data_dictionaryIdItemKey`(`warehouse_id`, `dictionary_id`, `item_key`) USING BTREE,
  INDEX `idx_dictionary_data_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_dictionary_data_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 297 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = 'WES字典项' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for extension
-- ----------------------------
DROP TABLE IF EXISTS `extension`;
CREATE TABLE `extension`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `extension_point_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展点编码',
  `extension_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展编码',
  `extension_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展名称',
  `extension_group` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展分组',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `sort_num` int(11) NOT NULL DEFAULT 1 COMMENT '排序字段',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_extension_pointCode`(`warehouse_id`, `extension_point_code`, `extension_group`, `extension_code`) USING BTREE,
  UNIQUE INDEX `uidx_extension_pointName`(`warehouse_id`, `extension_point_code`, `extension_group`, `extension_name`) USING BTREE,
  INDEX `idx_extension_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_extension_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `idx_extension_extensionPointCode`(`extension_point_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 762 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '扩展' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for extension_point
-- ----------------------------
DROP TABLE IF EXISTS `extension_point`;
CREATE TABLE `extension_point`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '所属仓库',
  `namespace` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '命名空间',
  `extension_point_code` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展点编码',
  `extension_point_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展点名称',
  `extension_point_desc` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '扩展点名称',
  `allow_multiple` tinyint(1) NOT NULL DEFAULT 1 COMMENT '允许多值',
  `sort_num` int(11) NOT NULL DEFAULT 1 COMMENT '排序字段',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_extension_point_pointCode`(`warehouse_id`, `extension_point_code`) USING BTREE,
  UNIQUE INDEX `uidx_extension_point_pointName`(`warehouse_id`, `extension_point_name`) USING BTREE,
  INDEX `idx_extension_point_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_extension_point_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 15 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '扩展点' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for feng_tian_customer
-- ----------------------------
DROP TABLE IF EXISTS `feng_tian_customer`;
CREATE TABLE `feng_tian_customer`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `customer_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '客户号码',
  `dlr_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'DLR代码',
  `fpd_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `short_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `dsn` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `udf1` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `udf2` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `udf3` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `udf4` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `udf5` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性值',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `u_idx_dlr_code`(`dlr_code`) USING BTREE,
  UNIQUE INDEX `u_idx_customer_code`(`customer_code`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 579 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '丰田货主信息' ROW_FORMAT = Compact;

-- ----------------------------
-- Table structure for feng_tian_feedback
-- ----------------------------
DROP TABLE IF EXISTS `feng_tian_feedback`;
CREATE TABLE `feng_tian_feedback`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `keyword` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '关键信息',
  `group_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '组id',
  `feedback_type` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '类型',
  `state` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '状态',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uidx_keyword_feedback_type`(`keyword`, `feedback_type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 11082 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '反馈' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for feng_tian_sku_property
-- ----------------------------
DROP TABLE IF EXISTS `feng_tian_sku_property`;
CREATE TABLE `feng_tian_sku_property`  (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,
  `sku_id` bigint(20) UNSIGNED NOT NULL,
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
  UNIQUE INDEX `u_idx_skuId_propName`(`sku_id`, `property_name`) USING BTREE,
  INDEX `idx_property_name`(`property_name`) USING BTREE,
  INDEX `idx_sku_id`(`sku_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 67840 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '商品属性' ROW_FORMAT = Dynamic;

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
-- Table structure for flyway_schema_history_copy
-- ----------------------------
DROP TABLE IF EXISTS `flyway_schema_history_copy`;
CREATE TABLE `flyway_schema_history_copy`  (
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
-- Table structure for match_bill
-- ----------------------------
DROP TABLE IF EXISTS `match_bill`;
CREATE TABLE `match_bill`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `owner_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '货主',
  `external_business_type_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上游业务类型编码',
  `external_business_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '上游业务类型名称',
  `external_order_type_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '上游单据类型编码',
  `external_order_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '上游单据类型名称',
  `business_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'WES业务类型',
  `order_type_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'WES单据类型编码',
  `order_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'WES单据类型名称',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建日期',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '上游单据匹配' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy
-- ----------------------------
DROP TABLE IF EXISTS `policy`;
CREATE TABLE `policy`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `category_id` bigint(20) NOT NULL COMMENT '策略类别ID',
  `policy_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略代码',
  `policy_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略名称',
  `policy_desc` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '策略描述',
  `input_data_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '输入数据类型',
  `input_object_class` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '输入对象类型',
  `input_multiple_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '输入多值标记',
  `result_data_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '结论数据类型',
  `result_object_class` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '结论对象类型',
  `result_multiple_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '结论多值标记',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `sort_num` int(4) NOT NULL DEFAULT 1 COMMENT '排序值',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_policyCode`(`warehouse_id`, `policy_code`) USING BTREE,
  INDEX `idx_policy_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 120 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_category
-- ----------------------------
DROP TABLE IF EXISTS `policy_category`;
CREATE TABLE `policy_category`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `category_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略类别标识',
  `category_name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略类别名称',
  `category_desc` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT '' COMMENT '策略类别描述',
  `parent_id` bigint(20) DEFAULT NULL COMMENT '上级ID',
  `sort_num` int(4) NOT NULL DEFAULT 1 COMMENT '排序值',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_category_categoryCode_sortNum`(`warehouse_id`, `category_code`, `sort_num`) USING BTREE,
  INDEX `idx_policy_category_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_category_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略类别' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_group
-- ----------------------------
DROP TABLE IF EXISTS `policy_group`;
CREATE TABLE `policy_group`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `policy_group_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略组编码',
  `policy_group_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略组名称',
  `policy_group_desc` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '策略组描述',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_group_policyGroupCode`(`warehouse_id`, `policy_group_code`) USING BTREE,
  INDEX `idx_policy_group_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_group_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略组' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_group_item
-- ----------------------------
DROP TABLE IF EXISTS `policy_group_item`;
CREATE TABLE `policy_group_item`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `policy_group_id` bigint(20) NOT NULL COMMENT '策略组ID',
  `policy_id` bigint(20) NOT NULL COMMENT '策略ID',
  `sort_num` int(4) NOT NULL DEFAULT 1 COMMENT '排序值',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_group_item_policyGroupIdPolicyId`(`warehouse_id`, `policy_group_id`, `policy_id`) USING BTREE,
  INDEX `idx_policy_group_item_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_group_item_lastUpdatedDate`(`last_updated_date`) USING BTREE,
  INDEX `idx_policy_group_item_policyGroupId`(`warehouse_id`, `policy_group_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略组明细' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_object_class
-- ----------------------------
DROP TABLE IF EXISTS `policy_object_class`;
CREATE TABLE `policy_object_class`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `object_class_code` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '对象类型编码',
  `object_class_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '对象类型名称',
  `object_class_desc` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '对象类型描述',
  `java_class_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '对应Java类名',
  `display_format` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '展示格式化',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_object_class_objectClassCode`(`warehouse_id`, `object_class_code`) USING BTREE,
  INDEX `idx_policy_object_class_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_object_class_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 126 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略对象类型' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_property
-- ----------------------------
DROP TABLE IF EXISTS `policy_property`;
CREATE TABLE `policy_property`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `object_class_id` bigint(20) NOT NULL COMMENT '对象类型ID',
  `property_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性名称',
  `property_desc` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性描述',
  `data_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属性数据类型',
  `data_object_class` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属性对象类型',
  `choice_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '枚举值标记',
  `choice_variable` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '枚举值系统变量',
  `multiple_flag` tinyint(1) NOT NULL DEFAULT 0 COMMENT '多值标记',
  `required` tinyint(1) NOT NULL DEFAULT 1 COMMENT '必填标记',
  `sort_num` int(4) NOT NULL DEFAULT 1 COMMENT '排序值',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_property_objectClassId`(`warehouse_id`, `object_class_id`, `property_name`) USING BTREE,
  INDEX `idx_policy_property_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_property_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 292 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略对象属性' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_property_choice_variable
-- ----------------------------
DROP TABLE IF EXISTS `policy_property_choice_variable`;
CREATE TABLE `policy_property_choice_variable`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `variable_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '值枚举变量名',
  `value_expression` varchar(500) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '值域SQL表达式',
  `i18n_required` tinyint(1) NOT NULL DEFAULT 0 COMMENT '需要国际化',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_choice_variableName`(`variable_name`) USING BTREE,
  INDEX `idx_policy_category_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_category_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 24 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略属性值枚举变量' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for policy_rule
-- ----------------------------
DROP TABLE IF EXISTS `policy_rule`;
CREATE TABLE `policy_rule`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `policy_id` bigint(20) NOT NULL COMMENT '所属策略',
  `rule_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '策略规则名称',
  `conditions` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '条件',
  `result` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '结论',
  `sort_num` int(4) NOT NULL DEFAULT 1 COMMENT '排序值',
  `default_rule` tinyint(1) NOT NULL DEFAULT 0 COMMENT '默认规则标记',
  `enabled` tinyint(1) NOT NULL DEFAULT 1 COMMENT '启用标记',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_policy_rule_sortNum`(`warehouse_id`, `policy_id`, `sort_num`) USING BTREE,
  INDEX `idx_policy_rule_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_policy_rule_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 230 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '策略规则' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for route
-- ----------------------------
DROP TABLE IF EXISTS `route`;
CREATE TABLE `route`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `route_code` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '路线编码',
  `route_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '路线名称',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作人',
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
  UNIQUE INDEX `uidx_route_code`(`route_code`) USING BTREE,
  UNIQUE INDEX `uidx_route_name`(`route_name`) USING BTREE,
  INDEX `idx_route_created_date`(`created_date`) USING BTREE,
  INDEX `idx_route_last_updated_date`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 18 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '路线表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for route_detail
-- ----------------------------
DROP TABLE IF EXISTS `route_detail`;
CREATE TABLE `route_detail`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库ID',
  `route_id` bigint(20) NOT NULL COMMENT '路线id',
  `customer` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '客户',
  `start_time` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '起始时间',
  `end_time` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '结束时间',
  `operator` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '操作人',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_code`(`route_id`, `customer`, `start_time`, `end_time`) USING BTREE,
  INDEX `idx_route_detail_created_date`(`created_date`) USING BTREE,
  INDEX `idx_route_detail_last_updated_date`(`last_updated_date`) USING BTREE,
  CONSTRAINT `fk_route_detail_route_id` FOREIGN KEY (`route_id`) REFERENCES `route` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE = InnoDB AUTO_INCREMENT = 120 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '路线详情表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for sequence
-- ----------------------------
DROP TABLE IF EXISTS `sequence`;
CREATE TABLE `sequence`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `sequence_name` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '序列名称',
  `min_value` bigint(20) NOT NULL DEFAULT 1 COMMENT '最小值',
  `max_value` bigint(20) NOT NULL DEFAULT 999999999999999 COMMENT '最大值',
  `next_value` bigint(20) NOT NULL DEFAULT 1 COMMENT '下一个值',
  `increment_by` bigint(20) NOT NULL DEFAULT 1 COMMENT '增长步长',
  `cache` bigint(20) NOT NULL DEFAULT 20 COMMENT '内存预分配数量',
  `created_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `created_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
  `created_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建应用',
  `last_updated_date` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后修改时间',
  `last_updated_user` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改人',
  `last_updated_app` varchar(45) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '最后修改应用',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_sequence_sequenceName`(`sequence_name`) USING BTREE,
  INDEX `idx_sequence_createdDate`(`created_date`) USING BTREE,
  INDEX `idx_sequence_lastUpdatedDate`(`last_updated_date`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5118 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '通用序列' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
