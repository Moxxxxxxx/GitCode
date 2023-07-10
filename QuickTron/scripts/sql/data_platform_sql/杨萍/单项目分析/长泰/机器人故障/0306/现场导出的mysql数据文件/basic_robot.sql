/*
Navicat MySQL Data Transfer

Source Server         : 10.54.100.111
Source Server Version : 50732
Source Host           : 10.54.100.111:3306
Source Database       : phoenix_basic

Target Server Type    : MYSQL
Target Server Version : 50732
File Encoding         : 65001

Date: 2023-03-06 17:34:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for basic_robot
-- ----------------------------
DROP TABLE IF EXISTS `basic_robot`;
CREATE TABLE `basic_robot` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `create_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  `update_time` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  `create_user` varchar(255) DEFAULT NULL COMMENT '创建用户',
  `update_user` varchar(255) DEFAULT NULL COMMENT '更新用户',
  `state` varchar(255) NOT NULL COMMENT '状态',
  `ip` varchar(255) DEFAULT NULL COMMENT 'IP地址',
  `rack_code` varchar(255) DEFAULT NULL COMMENT '货架编码',
  `robot_code` varchar(32) NOT NULL COMMENT '机器人编码',
  `warehouse_id` bigint(20) NOT NULL COMMENT '仓库id',
  `zone_collection` varchar(255) DEFAULT NULL COMMENT '可作业区域集合,逗号分隔',
  `running_map` varchar(255) DEFAULT NULL COMMENT '机器人当前所在地图',
  `usage_state` varchar(255) DEFAULT NULL COMMENT '使用状态',
  `robot_type_code` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_robot` (`robot_code`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='机器人';

-- ----------------------------
-- Records of basic_robot
-- ----------------------------
INSERT INTO `basic_robot` VALUES ('1', '2022-10-20 16:40:14.111', '2023-03-06 12:20:56.647', 'admin', 'admin', 'effective', '10.54.100.11', null, 'CARRIER_105410011', '1', 'kckq', null, 'using', 'H80A-HBDQR0N-91');
INSERT INTO `basic_robot` VALUES ('2', '2022-10-20 16:40:14.114', '2023-03-06 09:36:49.050', 'admin', 'admin', 'effective', '10.54.100.12', null, 'CARRIER_105410012', '1', 'kckq', null, 'using', 'H80A-HBDQR0N-91');
INSERT INTO `basic_robot` VALUES ('3', '2022-10-20 16:40:14.115', '2023-03-06 09:36:52.897', 'admin', 'admin', 'effective', '10.54.100.13', null, 'CARRIER_105410013', '1', 'kckq', '', 'using', 'H80A-HBDQR0N-91');
INSERT INTO `basic_robot` VALUES ('4', '2022-10-20 16:40:14.116', '2023-03-05 10:58:17.030', 'admin', 'admin', 'effective', '10.54.100.14', null, 'CARRIER_105410014', '1', 'kckq', null, 'using', 'H80A-HBDQR0N-91');
INSERT INTO `basic_robot` VALUES ('5', '2022-10-20 16:40:14.117', '2023-03-05 10:58:17.030', 'admin', 'admin', 'effective', '10.54.100.15', null, 'CARRIER_105410015', '1', 'kckq', null, 'using', 'H80A-HBDQR0N-91');
INSERT INTO `basic_robot` VALUES ('6', '2022-10-20 16:40:14.118', '2023-03-06 09:39:02.466', 'admin', 'admin', 'effective', '10.54.100.16', null, 'CARRIER_105410016', '1', 'kckq', null, 'using', 'H80A-HBDQR0N-91');
