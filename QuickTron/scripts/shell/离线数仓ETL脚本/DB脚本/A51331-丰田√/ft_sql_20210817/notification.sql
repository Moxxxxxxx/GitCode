/*
 Navicat Premium Data Transfer

 Source Server         : 192.168.62.200
 Source Server Type    : MySQL
 Source Server Version : 50732
 Source Host           : 192.168.62.200:3306
 Source Schema         : notification

 Target Server Type    : MySQL
 Target Server Version : 50732
 File Encoding         : 65001

 Date: 17/08/2021 10:31:36
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
-- Table structure for history_notification_message
-- ----------------------------
DROP TABLE IF EXISTS `history_notification_message`;
CREATE TABLE `history_notification_message`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `unit_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT 'avgId',
  `message_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '消息id',
  `unit_type` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '单位类型',
  `warning_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '告警类型',
  `title` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '标题',
  `service_name` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '服务名称：如wes/wcs/rsc',
  `read_status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '0-未读，1-已读',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '异常状态,0-开启，1-关闭',
  `event` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '通知类型',
  `notify_level` int(11) NOT NULL DEFAULT 0 COMMENT '通知等级(0-未知,1-普通,3-警告,5-故障)',
  `happen_at` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '发生时间',
  `close_at` datetime(0) NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '告警关闭时间',
  `message_body` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '消息体',
  `compress_message_body` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '压缩后的消息体',
  `warehouse_id` int(11) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '创建用户',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '创建应用',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '更新用户',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '更新应用',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `message_id`(`message_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '接收信息历史表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_item
-- ----------------------------
DROP TABLE IF EXISTS `notification_item`;
CREATE TABLE `notification_item`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `unit_type` int(11) NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ä¸ªä½“ç±»åž‹',
  `unit_id` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥ä¸ªä½“ID',
  `title` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥æ ‡é¢˜',
  `content` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥å†…å®¹',
  `service` varchar(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'è°ƒç”¨æœåŠ¡',
  `event` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'äº‹ä»¶',
  `level` int(11) NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ç­‰çº§',
  `solve_link` int(11) DEFAULT NULL,
  `happen_at` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'å‘ç”Ÿæ—¶é—´',
  `close_at` datetime(0) NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT 'å…³é—­æ—¶é—´',
  `status` int(11) NOT NULL DEFAULT 0 COMMENT 'æ¶ˆæ¯çŠ¶æ€',
  `task_status` int(11) NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ä»»åŠ¡åˆ›å»ºçŠ¶æ€ï¼ˆ1ï¼šå¾…åˆ›å»ºä»»åŠ¡ï¼Œ2ï¼šå·²åˆ›å»ºï¼Œ3ï¼šåˆ›å»ºå¤±è´¥ï¼‰',
  `task_remark` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'ä»»åŠ¡å¤‡æ³¨',
  `warehouse_id` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'æ‰€å±žä»“åº“ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  `notify_level` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_message
-- ----------------------------
DROP TABLE IF EXISTS `notification_message`;
CREATE TABLE `notification_message`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `unit_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT 'avgId',
  `message_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '消息id',
  `unit_type` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '单位类型',
  `warning_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '告警类型',
  `title` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '标题',
  `service_name` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '服务名称：如wes/wcs/rsc',
  `read_status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '0-未读，1-已读',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '异常状态,0-开启，1-关闭',
  `event` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '通知类型',
  `notify_level` int(11) NOT NULL DEFAULT 0 COMMENT '通知等级(0-未知,1-普通,3-警告,5-故障)',
  `happen_at` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '发生时间',
  `close_at` datetime(0) NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT '告警关闭时间',
  `message_body` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '消息体',
  `compress_message_body` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '压缩后的消息体',
  `warehouse_id` int(11) NOT NULL DEFAULT 0 COMMENT '仓库ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '创建用户',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '创建应用',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '更新用户',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '更新应用',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `message_id`(`message_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '接收信息表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_rule
-- ----------------------------
DROP TABLE IF EXISTS `notification_rule`;
CREATE TABLE `notification_rule`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `unit_type` int(11) NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ä¸ªä½“å¯¹è±¡ç±»åž‹',
  `event` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥äº‹ä»¶',
  `notify_level` int(11) NOT NULL DEFAULT 0 COMMENT '通知等级',
  `enable` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'æ˜¯å¦å¯ç”¨',
  `start_time` time(0) NOT NULL DEFAULT '00:00:00' COMMENT 'è§„åˆ™ç”Ÿæ•ˆæ—¶é—´èµ·ç‚¹',
  `end_time` time(0) NOT NULL DEFAULT '00:00:00' COMMENT 'è§„åˆ™ç”Ÿæ•ˆæ—¶é—´ç»ˆç‚¹',
  `warehouse_id` int(11) NOT NULL DEFAULT 0 COMMENT 'æ‰€å±žä»“åº“ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 7 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_rule_target
-- ----------------------------
DROP TABLE IF EXISTS `notification_rule_target`;
CREATE TABLE `notification_rule_target`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `rule_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥è§„åˆ™ID',
  `user_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥å¯¹è±¡ç”¨æˆ·ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_rule_way
-- ----------------------------
DROP TABLE IF EXISTS `notification_rule_way`;
CREATE TABLE `notification_rule_way`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `rule_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥è§„åˆ™ID',
  `way` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥æ¸ é“ç±»åž‹ [PLATFORM, DING, SMS, VMS]',
  `interval` int(11) NOT NULL DEFAULT 1 COMMENT 'å¤šæ¬¡é€šçŸ¥é—´éš”æ—¶é—´ï¼ˆåˆ†é’Ÿï¼‰',
  `times` int(11) NOT NULL DEFAULT 1 COMMENT 'é€šçŸ¥æ¬¡æ•°',
  `token` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥æ¸ é“æ‰€éœ€tokenå‚æ•°å€¼',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 15 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_task
-- ----------------------------
DROP TABLE IF EXISTS `notification_task`;
CREATE TABLE `notification_task`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `item_id` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥æ¶ˆæ¯ID',
  `rule_id` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥è§„åˆ™ID',
  `user_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥å¯¹è±¡ç”¨æˆ·ID',
  `phone` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥å¯¹è±¡æ‰‹æœºå·',
  `way` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥æ¸ é“ç±»åž‹[PLATFORM, DING, SMS, VMS]',
  `total_times` int(11) NOT NULL DEFAULT 0 COMMENT 'æ€»è®¡é€šçŸ¥æ¬¡æ•°',
  `done_times` int(11) NOT NULL DEFAULT 0 COMMENT 'å·²é€šçŸ¥æ¬¡æ•°',
  `last_out_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æœ€åŽä¸€æ¬¡é€šçŸ¥å›žæ‰§ID',
  `last_finish_date` datetime(0) NOT NULL DEFAULT '1900-01-01 00:00:00' COMMENT 'æœ€åŽä¸€æ¬¡é€šçŸ¥æ—¶é—´',
  `interval_min` int(11) NOT NULL DEFAULT 1 COMMENT 'å¤šæ¬¡é€šçŸ¥æ—¶é—´é—´éš”ï¼ˆç§’ï¼‰',
  `status` int(11) NOT NULL DEFAULT 0 COMMENT 'çŠ¶æ€ï¼ˆ1:å¾…å¤„ç†ï¼Œ2ï¼šå·²å¤„ç†ï¼Œ3ï¼šå·²é€è¾¾ï¼Œ4ï¼šå–æ¶ˆï¼‰',
  `remark` varchar(300) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'å¤‡æ³¨ä¿¡æ¯',
  `warehouse_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'ä»“åº“ID',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  `interval` int(11) NOT NULL DEFAULT 1 COMMENT 'å¤šæ¬¡é€šçŸ¥æ—¶é—´é—´éš”ï¼ˆç§’ï¼‰',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for notification_task_result
-- ----------------------------
DROP TABLE IF EXISTS `notification_task_result`;
CREATE TABLE `notification_task_result`  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `task_id` int(10) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ä»»åŠ¡ID',
  `out_id` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥å¤–éƒ¨ID',
  `result` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'é€šçŸ¥ä»»åŠ¡ç»“æžœå†…å®¹',
  `status` int(11) NOT NULL DEFAULT 0 COMMENT 'é€šçŸ¥ä»»åŠ¡ç»“æžœçŠ¶æ€',
  `created_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºäºº',
  `created_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'åˆ›å»ºåº”ç”¨',
  `created_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'åˆ›å»ºæ—¶é—´',
  `last_updated_user` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°äºº',
  `last_updated_app` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'æ›´æ–°åº”ç”¨',
  `last_updated_time` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'æ›´æ–°æ—¶é—´',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `inx_out_id`(`out_id`) USING BTREE,
  INDEX `inx_task_id`(`task_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
