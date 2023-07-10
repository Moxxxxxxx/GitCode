/*
 Navicat Premium Data Transfer

 Source Server         : 237.5库
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 172.31.237.5:3306
 Source Schema         : evo_wcs_g2p

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 22/07/2021 10:07:49
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for job_state_change
-- ----------------------------
DROP TABLE IF EXISTS `job_state_change`;
CREATE TABLE `job_state_change`  (
  `id` int(11) NOT NULL,
  `warehouse_id` bigint(16) NULL DEFAULT NULL,
  `zone_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `job_id` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `job_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `agv_code` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `agv_type` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `state` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `created_date` datetime(3) NULL DEFAULT NULL,
  `updated_app` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `updated_date` datetime(3) NULL DEFAULT NULL,
  `project_code` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '项目编号',
  PRIMARY KEY (`id`, `project_code`) USING BTREE,
  INDEX `idx_job_state_change_job_id`(`job_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '任务状态变更记录' ROW_FORMAT = COMPACT;

SET FOREIGN_KEY_CHECKS = 1;
