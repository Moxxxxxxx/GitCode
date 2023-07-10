-- phpMyAdmin SQL Dump
-- version 5.0.2
-- https://www.phpmyadmin.net/
--
-- 主机： mysql
-- 生成日期： 2021-06-18 06:36:26
-- 服务器版本： 5.7.30
-- PHP 版本： 7.4.6

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- 数据库： `data_quality`
--
CREATE DATABASE IF NOT EXISTS `data_quality` DEFAULT CHARACTER SET = utf8mb4;
USE `data_quality`;

-- --------------------------------------------------------

--
-- 表的结构 `auth_group`
--

DROP TABLE IF EXISTS `auth_group`;
CREATE TABLE `auth_group` (
                              `id` int(11) NOT NULL,
                              `name` varchar(150) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `auth_group_permissions`
--

DROP TABLE IF EXISTS `auth_group_permissions`;
CREATE TABLE `auth_group_permissions` (
                                          `id` int(11) NOT NULL,
                                          `group_id` int(11) NOT NULL,
                                          `permission_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `auth_permission`
--

DROP TABLE IF EXISTS `auth_permission`;
CREATE TABLE `auth_permission` (
                                   `id` int(11) NOT NULL,
                                   `name` varchar(255) NOT NULL,
                                   `content_type_id` int(11) NOT NULL,
                                   `codename` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `auth_permission`
--

INSERT INTO `auth_permission` (`id`, `name`, `content_type_id`, `codename`) VALUES
(1, 'Can add log entry', 1, 'add_logentry'),
(2, 'Can change log entry', 1, 'change_logentry'),
(3, 'Can delete log entry', 1, 'delete_logentry'),
(4, 'Can view log entry', 1, 'view_logentry'),
(5, 'Can add permission', 2, 'add_permission'),
(6, 'Can change permission', 2, 'change_permission'),
(7, 'Can delete permission', 2, 'delete_permission'),
(8, 'Can view permission', 2, 'view_permission'),
(9, 'Can add group', 3, 'add_group'),
(10, 'Can change group', 3, 'change_group'),
(11, 'Can delete group', 3, 'delete_group'),
(12, 'Can view group', 3, 'view_group'),
(13, 'Can add user', 4, 'add_user'),
(14, 'Can change user', 4, 'change_user'),
(15, 'Can delete user', 4, 'delete_user'),
(16, 'Can view user', 4, 'view_user'),
(17, 'Can add content type', 5, 'add_contenttype'),
(18, 'Can change content type', 5, 'change_contenttype'),
(19, 'Can delete content type', 5, 'delete_contenttype'),
(20, 'Can view content type', 5, 'view_contenttype'),
(21, 'Can add session', 6, 'add_session'),
(22, 'Can change session', 6, 'change_session'),
(23, 'Can delete session', 6, 'delete_session'),
(24, 'Can view session', 6, 'view_session');

-- --------------------------------------------------------

--
-- 表的结构 `auth_user`
--

DROP TABLE IF EXISTS `auth_user`;
CREATE TABLE `auth_user` (
                             `id` int(11) NOT NULL,
                             `password` varchar(128) NOT NULL,
                             `last_login` datetime(6) DEFAULT NULL,
                             `is_superuser` tinyint(1) NOT NULL,
                             `username` varchar(150) NOT NULL,
                             `first_name` varchar(30) NOT NULL,
                             `last_name` varchar(150) NOT NULL,
                             `email` varchar(254) NOT NULL,
                             `is_staff` tinyint(1) NOT NULL,
                             `is_active` tinyint(1) NOT NULL,
                             `date_joined` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `auth_user`
--

INSERT INTO `auth_user` (`id`, `password`, `last_login`, `is_superuser`, `username`, `first_name`, `last_name`, `email`, `is_staff`, `is_active`, `date_joined`) VALUES
(1, 'pbkdf2_sha256$180000$rD6PBXXwR3tE$we3ruyxFZihP1+g4xxL7QTJO0BcUrLJ4KRKLkramZek=', NULL, 1, 'water', '', '', 'zhangzhijun@falhhold.com', 1, 1, '2021-06-01 07:02:12.366421');

-- --------------------------------------------------------

--
-- 表的结构 `auth_user_groups`
--

DROP TABLE IF EXISTS `auth_user_groups`;
CREATE TABLE `auth_user_groups` (
                                    `id` int(11) NOT NULL,
                                    `user_id` int(11) NOT NULL,
                                    `group_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `auth_user_user_permissions`
--

DROP TABLE IF EXISTS `auth_user_user_permissions`;
CREATE TABLE `auth_user_user_permissions` (
                                              `id` int(11) NOT NULL,
                                              `user_id` int(11) NOT NULL,
                                              `permission_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `check_execute_log`
--

DROP TABLE IF EXISTS `check_execute_log`;
CREATE TABLE `check_execute_log` (
                                     `id` int(11) NOT NULL,
                                     `productname` varchar(30) NOT NULL,
                                     `execute_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                     `check_item` varchar(100) DEFAULT NULL COMMENT '检查项',
                                     `execute_user` varchar(30) NOT NULL,
                                     `execute_result_info` varchar(300) DEFAULT NULL COMMENT '执行结果信息',
                                     `db` varchar(100) DEFAULT NULL,
                                     `status` varchar(300) DEFAULT NULL COMMENT '执行结果'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `check_execute_log`
--

INSERT INTO `check_execute_log` (`id`, `productname`, `execute_date`, `check_item`, `execute_user`, `execute_result_info`, `db`, `status`) VALUES
(1, 'product1', '2020-06-16 11:39:20', '用户激活数准确性校验', 'admin', '', NULL, 'fail'),

-- --------------------------------------------------------

--
-- 表的结构 `check_jobs`
--

DROP TABLE IF EXISTS `check_jobs`;
CREATE TABLE `check_jobs` (
                              `id` bigint(20) UNSIGNED NOT NULL,
                              `jobname` varchar(255) DEFAULT NULL,
                              `productname` varchar(255) DEFAULT NULL,
                              `job_description` varchar(255) DEFAULT NULL,
                              `job_excute_date` datetime DEFAULT NULL,
                              `job_excute_result` varchar(20) DEFAULT '',
                              `is_excuted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1-已执行 0-未执行',
                              `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0=未删除 1=已删除',
                              `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
                              `created_by` varchar(255) CHARACTER SET utf8 NOT NULL COMMENT '创建者',
                              `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `check_result_product1`
--

DROP TABLE IF EXISTS `check_result_product1`;
CREATE TABLE `check_result_product1` (
                                      `id` int(11) NOT NULL COMMENT '排序用id',
                                      `productname` varchar(10) CHARACTER SET utf8mb4 DEFAULT '' COMMENT '预留字段，系统名',
                                      `check_item` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '检查项',
                                      `target_table` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '目标表',
                                      `problem_type` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '问题分类1、完整性检验2、准确性检验3、合理性检验4、一致性检验5、及时性检验',
                                      `check_sql` text CHARACTER SET utf8mb4 COMMENT '检查SQL',
                                      `check_code` varchar(4000) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '检查代码',
                                      `problem_id` varchar(32) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '问题数据对应主键编号',
                                      `item_count` int(11) DEFAULT NULL COMMENT '检查数据量',
                                      `problem_count` int(11) DEFAULT NULL COMMENT '问题数据量',
                                      `problem_per` decimal(10,2) DEFAULT NULL COMMENT '问题百分比',
                                      `db` varchar(30) CHARACTER SET utf8mb4 DEFAULT NULL,
                                      `remote_ip` varchar(30) CHARACTER SET utf8mb4 DEFAULT NULL,
                                      `note` varchar(200) CHARACTER SET utf8mb4 DEFAULT NULL,
                                      `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1-已启用 0-已停用',
                                      `update_flag` varchar(2) CHARACTER SET utf8mb4 DEFAULT 'N',
                                      `check_date` timestamp NULL DEFAULT NULL,
                                      `check_result` varchar(10) CHARACTER SET utf8mb4 DEFAULT '',
                                      `check_version` int(11) DEFAULT NULL,
                                      `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0=未删除 1=已删除'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- 转存表中的数据 `check_result_product1`
--

INSERT INTO `check_result_product1` (`id`, `productname`, `check_item`, `target_table`, `problem_type`, `check_sql`, `check_code`, `problem_id`, `item_count`, `problem_count`, `problem_per`, `db`, `remote_ip`, `note`, `status`, `update_flag`, `check_date`, `check_result`, `check_version`, `is_deleted`) VALUES
(1, 'product1', '用户激活数准确性校验', 'None', '完整性检验', 'SELECT count(*) FROM `base_login_log_info` WHERE log_time BETWEEN \'2020-06-01\' and \'2020-06-02\';=;963', NULL, NULL, NULL, NULL, NULL, '', 'None', '用户激活数校验', 1, 'N', '2020-06-16 12:38:06', 'success', 18, 0),

-- --------------------------------------------------------

--
-- 表的结构 `check_result_template`
--

DROP TABLE IF EXISTS `check_result_template`;
CREATE TABLE `check_result_template` (
                                         `id` int(11) NOT NULL COMMENT '排序用id',
                                         `productname` varchar(10) DEFAULT '' COMMENT '预留字段，系统名',
                                         `check_item` varchar(100) DEFAULT NULL COMMENT '检查项',
                                         `target_table` varchar(20) DEFAULT NULL COMMENT '目标表',
                                         `problem_type` varchar(20) DEFAULT NULL COMMENT '问题分类1、完整性检验2、准确性检验3、合理性检验4、一致性检验5、及时性检验',
                                         `check_sql` text COMMENT '检查SQL',
                                         `check_code` varchar(4000) DEFAULT NULL COMMENT '检查代码',
                                         `problem_id` varchar(32) DEFAULT NULL COMMENT '问题数据对应主键编号',
                                         `item_count` int(11) DEFAULT NULL COMMENT '检查数据量',
                                         `problem_count` int(11) DEFAULT NULL COMMENT '问题数据量',
                                         `problem_per` decimal(10,2) DEFAULT NULL COMMENT '问题百分比',
                                         `db` varchar(30) DEFAULT NULL,
                                         `remote_ip` varchar(30) DEFAULT NULL,
                                         `note` varchar(200) DEFAULT NULL,
                                         `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '1-已启用 0-已停用',
                                         `update_flag` varchar(2) DEFAULT 'N',
                                         `check_date` timestamp NULL DEFAULT NULL,
                                         `check_result` varchar(10) DEFAULT '',
                                         `check_version` int(11) DEFAULT NULL,
                                         `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0=未删除 1=已删除'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `check_result_template`
--

INSERT INTO `check_result_template` (`id`, `productname`, `check_item`, `target_table`, `problem_type`, `check_sql`, `check_code`, `problem_id`, `item_count`, `problem_count`, `problem_per`, `db`, `remote_ip`, `note`, `status`, `update_flag`, `check_date`, `check_result`, `check_version`, `is_deleted`) VALUES
(1, 'product1', '用户激活数准确性校验', 'None', '完整性检验', 'SELECT count(*) FROM `base_login_log_info` WHERE log_time BETWEEN \'2020-06-01\' and \'2020-06-02\';=;963', NULL, NULL, NULL, NULL, NULL, '', 'None', '用户激活数校验', 1, 'N', '2020-06-17 09:45:25', 'fail', NULL, 0),

-- --------------------------------------------------------

--
-- 表的结构 `data_standard_desc`
--

DROP TABLE IF EXISTS `data_standard_desc`;
CREATE TABLE `data_standard_desc` (
                                      `id` int(11) NOT NULL,
                                      `name` text CHARACTER SET utf8,
                                      `content` text CHARACTER SET utf8
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `data_standard_detail`
--

DROP TABLE IF EXISTS `data_standard_detail`;
CREATE TABLE `data_standard_detail` (
                                        `id` int(11) NOT NULL,
                                        `std_id` text CHARACTER SET utf8,
                                        `name` text CHARACTER SET utf8,
                                        `en_name` text CHARACTER SET utf8,
                                        `business_definition` text CHARACTER SET utf8 COMMENT '业务定义',
                                        `business_rule` text CHARACTER SET utf8 COMMENT '业务规则',
                                        `std_source` text CHARACTER SET utf8 COMMENT '业务定义',
                                        `data_type` text CHARACTER SET utf8 COMMENT '标准来源',
                                        `data_format` text CHARACTER SET utf8 COMMENT '数据类别',
                                        `code_rule` text CHARACTER SET utf8 COMMENT '代码编码规则',
                                        `code_range` text CHARACTER SET utf8 COMMENT '取值范围',
                                        `code_meaning` text CHARACTER SET utf8 COMMENT '代码取值含义',
                                        `business_range` text CHARACTER SET utf8 COMMENT '数据业务范围',
                                        `dept` text CHARACTER SET utf8 COMMENT '数据责任部门',
                                        `system` text CHARACTER SET utf8 COMMENT '数据使用系统'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `dim_date`
--

DROP TABLE IF EXISTS `dim_date`;
CREATE TABLE `dim_date` (
                            `date` datetime DEFAULT NULL,
                            `day_id` int(11) NOT NULL,
                            `year` int(11) DEFAULT NULL,
                            `month` int(11) DEFAULT NULL,
                            `day` int(11) DEFAULT NULL,
                            `quarter` int(11) DEFAULT NULL,
                            `day_name` text,
                            `weekofyear` bigint(20) DEFAULT NULL,
                            `dayofyear` int(11) DEFAULT NULL,
                            `daysinmonth` int(11) DEFAULT NULL,
                            `dayofweek` int(11) DEFAULT NULL,
                            `is_leap_year` tinyint(1) DEFAULT NULL,
                            `is_month_end` tinyint(1) DEFAULT NULL,
                            `is_month_start` tinyint(1) DEFAULT NULL,
                            `is_quarter_end` tinyint(1) DEFAULT NULL,
                            `is_quarter_start` tinyint(1) DEFAULT NULL,
                            `is_year_end` tinyint(1) DEFAULT NULL,
                            `is_year_start` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `django_admin_log`
--

DROP TABLE IF EXISTS `django_admin_log`;
CREATE TABLE `django_admin_log` (
                                    `id` int(11) NOT NULL,
                                    `action_time` datetime(6) NOT NULL,
                                    `object_id` longtext,
                                    `object_repr` varchar(200) NOT NULL,
                                    `action_flag` smallint(5) UNSIGNED NOT NULL,
                                    `change_message` longtext NOT NULL,
                                    `content_type_id` int(11) DEFAULT NULL,
                                    `user_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `django_content_type`
--

DROP TABLE IF EXISTS `django_content_type`;
CREATE TABLE `django_content_type` (
                                       `id` int(11) NOT NULL,
                                       `app_label` varchar(100) NOT NULL,
                                       `model` varchar(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `django_content_type`
--

INSERT INTO `django_content_type` (`id`, `app_label`, `model`) VALUES
(1, 'admin', 'logentry'),
(3, 'auth', 'group'),
(2, 'auth', 'permission'),
(4, 'auth', 'user'),
(5, 'contenttypes', 'contenttype'),
(6, 'sessions', 'session');

-- --------------------------------------------------------

--
-- 表的结构 `django_migrations`
--

DROP TABLE IF EXISTS `django_migrations`;
CREATE TABLE `django_migrations` (
                                     `id` int(11) NOT NULL,
                                     `app` varchar(255) NOT NULL,
                                     `name` varchar(255) NOT NULL,
                                     `applied` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `django_migrations`
--

INSERT INTO `django_migrations` (`id`, `app`, `name`, `applied`) VALUES
(1, 'contenttypes', '0001_initial', '2020-06-01 06:59:27.401245'),
(2, 'auth', '0001_initial', '2020-06-01 06:59:28.326130'),
(3, 'admin', '0001_initial', '2020-06-01 06:59:30.472597'),
(4, 'admin', '0002_logentry_remove_auto_add', '2020-06-01 06:59:31.058816'),
(5, 'admin', '0003_logentry_add_action_flag_choices', '2020-06-01 06:59:31.081701'),
(6, 'contenttypes', '0002_remove_content_type_name', '2020-06-01 06:59:31.505251'),
(7, 'auth', '0002_alter_permission_name_max_length', '2020-06-01 06:59:31.668808'),
(8, 'auth', '0003_alter_user_email_max_length', '2020-06-01 06:59:31.734378'),
(9, 'auth', '0004_alter_user_username_opts', '2020-06-01 06:59:31.761036'),
(10, 'auth', '0005_alter_user_last_login_null', '2020-06-01 06:59:31.996819'),
(11, 'auth', '0006_require_contenttypes_0002', '2020-06-01 06:59:32.005929'),
(12, 'auth', '0007_alter_validators_add_error_messages', '2020-06-01 06:59:32.035557'),
(13, 'auth', '0008_alter_user_username_max_length', '2020-06-01 06:59:32.290452'),
(14, 'auth', '0009_alter_user_last_name_max_length', '2020-06-01 06:59:32.490199'),
(15, 'auth', '0010_alter_group_name_max_length', '2020-06-01 06:59:32.569264'),
(16, 'auth', '0011_update_proxy_permissions', '2020-06-01 06:59:32.597617'),
(17, 'sessions', '0001_initial', '2020-06-01 06:59:32.745866');

-- --------------------------------------------------------

--
-- 表的结构 `django_session`
--

DROP TABLE IF EXISTS `django_session`;
CREATE TABLE `django_session` (
                                  `session_key` varchar(40) NOT NULL,
                                  `session_data` longtext NOT NULL,
                                  `expire_date` datetime(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `django_session`
--

INSERT INTO `django_session` (`session_key`, `session_data`, `expire_date`) VALUES
('02ytnxna9h7s2rq2rbbyn5hw4uaf5dmn', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-18 07:36:53.048815'),
('0303zy4gbro11px14n7vc0op97gnzvjz', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 02:11:47.260158'),
('0kwodx8ife8x7l49r7jhvgqzmxvyzslt', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-20 08:24:09.941895'),
('1nef1aymcsmbtlyr4rzdk2rwkylx1xjy', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 04:15:19.214712'),
('3jpjo67vvno7pm6uiq1mtvb9l6qns0ah', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-22 06:23:52.612551'),
('48pfaqmu1qxnaeohg0u3jqhbf3po7r8k', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 03:40:13.525218'),
('5d55cebqe98vzcf5cobjfeqsu4nahr5v', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-20 08:18:08.844826'),
('7dhe08qqgezoyhr0sexfzo2ntsvpnpls', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-27 07:56:43.862269'),
('7j3n4nzayeow0z0thod60yhll4pzylxp', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 02:14:27.005831'),
('a719rgo0w3n80vulzcdt3wkunr3jakxc', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-25 11:29:37.455283'),
('b9yvl5yswelf216p08f6pway1c5ercru', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-18 12:02:30.674669'),
('ba66ltj74v58i9gvmw2qypgjtb1boqez', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-07-01 07:24:15.944528'),
('c31gwrl8en88e03rmmsp54ounuxyrdjz', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 04:20:57.747014'),
('e76icxdttzdea8ghpo331i6u8s23bw19', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-18 07:10:28.064299'),
('gkxz7mg4y6p8cu4nsmwglm50j2dbf6cr', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 04:45:57.418733'),
('l2cdzqm2oyme5wnxegfafnpay7l0trwx', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 03:39:52.799210'),
('le7awk46f09oxu2h6erroftrl0z55ceq', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-07-01 09:24:03.331556'),
('mladoc4yrfftxy45r9tdk2bgtw2j6jgk', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-07-01 09:36:04.679600'),
('ndnvllrtbkx53wlx0gp82629xeqf8dlg', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-20 10:12:03.349945'),
('njh9cbw45jv8em1orgepfqj6k1u4lybe', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-27 07:57:13.250665'),
('nncf4b3l8nhjmwh1x2fppsbjos9yy0km', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 04:22:08.746409'),
('op0r0c5ir3okgd54h6gebfwjvfhtgj73', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-15 07:31:14.821149'),
('pulfepgvxunw5vbwwqgkes6k4zdb7yp0', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-29 04:01:11.064457'),
('vd33ixgw8fp6wl5xe0xi01jfc64g1u66', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-17 12:06:20.019513'),
('vhgtpcnrsxom7xx2xmkqohjsv1zz3l0z', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-30 11:52:46.527314'),
('xzbb82e31oqzkwq8sfwkli3acb1nz7co', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-16 05:47:22.619880'),
('zyspifww3avqlzrjzb5aob2lwkio5m3u', 'NWZhM2FkYWIwOWY2Y2I5ZWYzMGViZDJiYWYyZWEwNjBhNGFkZDg5Zjp7InVzZXJuYW1lIjoiYWRtaW4iLCJpc19sb2dpbiI6dHJ1ZX0=', '2020-06-19 02:36:37.930060');

-- --------------------------------------------------------

--
-- 表的结构 `prewarning_info`
--

DROP TABLE IF EXISTS `prewarning_info`;
CREATE TABLE `prewarning_info` (
                                   `id` bigint(20) UNSIGNED NOT NULL,
                                   `subject` varchar(255) DEFAULT NULL,
                                   `severity_level` int(11) DEFAULT NULL COMMENT '严重程度',
                                   `abnormal_info` varchar(255) DEFAULT NULL,
                                   `abnormal_date` datetime DEFAULT NULL,
                                   `is_read` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否已读 0=未读 1=已读',
                                   `is_deleted` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否删除 0=未删除 1=已删除',
                                   `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
                                   `created_by` varchar(255) CHARACTER SET utf8 NOT NULL COMMENT '创建者',
                                   `update_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `prewarning_info`
--

INSERT INTO `prewarning_info` (`id`, `subject`, `severity_level`, `abnormal_info`, `abnormal_date`, `is_read`, `is_deleted`, `create_time`, `created_by`, `update_time`) VALUES
(1, ' excute fail', 1, 'the rule is 用户激活数准确性校验 the err ', '2020-06-16 19:39:19', 0, 0, '2020-06-16 11:39:19', 'admin', NULL),
(34, 'excute fail', 1, 'the rule is opcode_211  the acutal checkresult is 0 and expect is 0\nthe err ', '2020-06-18 13:41:40', 0, 0, '2020-06-18 05:41:40', 'admin', NULL);

-- --------------------------------------------------------

--
-- 表的结构 `server_info`
--

DROP TABLE IF EXISTS `server_info`;
CREATE TABLE `server_info` (
                               `id` int(11) NOT NULL,
                               `productname` varchar(30) NOT NULL,
                               `server_name` varchar(10) DEFAULT NULL COMMENT '服务器名称',
                               `ip` varchar(16) DEFAULT NULL,
                               `user` varchar(32) DEFAULT NULL,
                               `password` varchar(200) DEFAULT NULL,
                               `port` int(11) DEFAULT NULL,
                               `note` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- 表的结构 `source_db_info`
--

DROP TABLE IF EXISTS `source_db_info`;
CREATE TABLE `source_db_info` (
                                  `id` int(11) NOT NULL,
                                  `productname` varchar(10) DEFAULT NULL COMMENT '产品线名称',
                                  `name` varchar(40) DEFAULT NULL,
                                  `alias` varchar(50) DEFAULT NULL,
                                  `connection_string` varchar(100) DEFAULT NULL,
                                  `ip` varchar(16) DEFAULT NULL,
                                  `user` varchar(32) DEFAULT NULL,
                                  `passwd` varchar(200) DEFAULT NULL,
                                  `db` varchar(32) DEFAULT NULL,
                                  `port` int(11) DEFAULT NULL,
                                  `db_type` varchar(32) DEFAULT NULL,
                                  `charset` varchar(10) DEFAULT NULL,
                                  `note` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转存表中的数据 `source_db_info`
--

INSERT INTO `source_db_info` (`id`, `productname`, `name`, `alias`, `connection_string`, `ip`, `user`, `passwd`, `db`, `port`, `db_type`, `charset`, `note`) VALUES
(1, 'product1', NULL, NULL, 'mysql+mysqldb://root:123456@mysql:3306/data_center?charset=utf8mb4', 'mysql', 'root', '123456', 'data_center', 3306, 'mysql', NULL, '后台数据库');

-- --------------------------------------------------------

--
-- 表的结构 `source_system_demand`
--

DROP TABLE IF EXISTS `source_system_demand`;
CREATE TABLE `source_system_demand` (
                                        `id` int(11) DEFAULT NULL,
                                        `productname` varchar(10) DEFAULT NULL,
                                        `item_name` varchar(100) DEFAULT NULL,
                                        `demand_name` varchar(100) DEFAULT NULL,
                                        `demand_created` varchar(20) DEFAULT NULL,
                                        `quarter` varchar(10) DEFAULT NULL,
                                        `status` varchar(100) DEFAULT NULL,
                                        `row_created` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- 转储表的索引
--

--
-- 表的索引 `auth_group`
--
ALTER TABLE `auth_group`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `name` (`name`);

--
-- 表的索引 `auth_group_permissions`
--
ALTER TABLE `auth_group_permissions`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `auth_group_permissions_group_id_permission_id_0cd325b0_uniq` (`group_id`,`permission_id`),
    ADD KEY `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` (`permission_id`);

--
-- 表的索引 `auth_permission`
--
ALTER TABLE `auth_permission`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `auth_permission_content_type_id_codename_01ab375a_uniq` (`content_type_id`,`codename`);

--
-- 表的索引 `auth_user`
--
ALTER TABLE `auth_user`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `username` (`username`);

--
-- 表的索引 `auth_user_groups`
--
ALTER TABLE `auth_user_groups`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `auth_user_groups_user_id_group_id_94350c0c_uniq` (`user_id`,`group_id`),
    ADD KEY `auth_user_groups_group_id_97559544_fk_auth_group_id` (`group_id`);

--
-- 表的索引 `auth_user_user_permissions`
--
ALTER TABLE `auth_user_user_permissions`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `auth_user_user_permissions_user_id_permission_id_14a6b632_uniq` (`user_id`,`permission_id`),
    ADD KEY `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` (`permission_id`);

--
-- 表的索引 `check_execute_log`
--
ALTER TABLE `check_execute_log`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `check_jobs`
--
ALTER TABLE `check_jobs`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `check_result_template`
--
ALTER TABLE `check_result_template`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `data_standard_desc`
--
ALTER TABLE `data_standard_desc`
    ADD PRIMARY KEY (`id`) USING BTREE;

--
-- 表的索引 `data_standard_detail`
--
ALTER TABLE `data_standard_detail`
    ADD PRIMARY KEY (`id`) USING BTREE;

--
-- 表的索引 `dim_date`
--
ALTER TABLE `dim_date`
    ADD PRIMARY KEY (`day_id`);

--
-- 表的索引 `django_admin_log`
--
ALTER TABLE `django_admin_log`
    ADD PRIMARY KEY (`id`),
    ADD KEY `django_admin_log_content_type_id_c4bce8eb_fk_django_co` (`content_type_id`),
    ADD KEY `django_admin_log_user_id_c564eba6_fk_auth_user_id` (`user_id`);

--
-- 表的索引 `django_content_type`
--
ALTER TABLE `django_content_type`
    ADD PRIMARY KEY (`id`),
    ADD UNIQUE KEY `django_content_type_app_label_model_76bd3d3b_uniq` (`app_label`,`model`);

--
-- 表的索引 `django_migrations`
--
ALTER TABLE `django_migrations`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `django_session`
--
ALTER TABLE `django_session`
    ADD PRIMARY KEY (`session_key`),
    ADD KEY `django_session_expire_date_a5c62663` (`expire_date`);

--
-- 表的索引 `prewarning_info`
--
ALTER TABLE `prewarning_info`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `server_info`
--
ALTER TABLE `server_info`
    ADD PRIMARY KEY (`id`);

--
-- 表的索引 `source_db_info`
--
ALTER TABLE `source_db_info`
    ADD PRIMARY KEY (`id`);

--
-- 在导出的表使用AUTO_INCREMENT
--

--
-- 使用表AUTO_INCREMENT `auth_group`
--
ALTER TABLE `auth_group`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `auth_group_permissions`
--
ALTER TABLE `auth_group_permissions`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `auth_permission`
--
ALTER TABLE `auth_permission`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=25;

--
-- 使用表AUTO_INCREMENT `auth_user`
--
ALTER TABLE `auth_user`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- 使用表AUTO_INCREMENT `auth_user_groups`
--
ALTER TABLE `auth_user_groups`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `auth_user_user_permissions`
--
ALTER TABLE `auth_user_user_permissions`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `check_execute_log`
--
ALTER TABLE `check_execute_log`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=44;

--
-- 使用表AUTO_INCREMENT `check_jobs`
--
ALTER TABLE `check_jobs`
    MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `data_standard_desc`
--
ALTER TABLE `data_standard_desc`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `data_standard_detail`
--
ALTER TABLE `data_standard_detail`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `django_admin_log`
--
ALTER TABLE `django_admin_log`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `django_content_type`
--
ALTER TABLE `django_content_type`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;

--
-- 使用表AUTO_INCREMENT `django_migrations`
--
ALTER TABLE `django_migrations`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=18;

--
-- 使用表AUTO_INCREMENT `prewarning_info`
--
ALTER TABLE `prewarning_info`
    MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=35;

--
-- 使用表AUTO_INCREMENT `server_info`
--
ALTER TABLE `server_info`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- 使用表AUTO_INCREMENT `source_db_info`
--
ALTER TABLE `source_db_info`
    MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- 限制导出的表
--

--
-- 限制表 `auth_group_permissions`
--
ALTER TABLE `auth_group_permissions`
    ADD CONSTRAINT `auth_group_permissio_permission_id_84c5c92e_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
    ADD CONSTRAINT `auth_group_permissions_group_id_b120cbf9_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`);

--
-- 限制表 `auth_permission`
--
ALTER TABLE `auth_permission`
    ADD CONSTRAINT `auth_permission_content_type_id_2f476e4b_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`);

--
-- 限制表 `auth_user_groups`
--
ALTER TABLE `auth_user_groups`
    ADD CONSTRAINT `auth_user_groups_group_id_97559544_fk_auth_group_id` FOREIGN KEY (`group_id`) REFERENCES `auth_group` (`id`),
    ADD CONSTRAINT `auth_user_groups_user_id_6a12ed8b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`);

--
-- 限制表 `auth_user_user_permissions`
--
ALTER TABLE `auth_user_user_permissions`
    ADD CONSTRAINT `auth_user_user_permi_permission_id_1fbb5f2c_fk_auth_perm` FOREIGN KEY (`permission_id`) REFERENCES `auth_permission` (`id`),
    ADD CONSTRAINT `auth_user_user_permissions_user_id_a95ead1b_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`);

--
-- 限制表 `django_admin_log`
--
ALTER TABLE `django_admin_log`
    ADD CONSTRAINT `django_admin_log_content_type_id_c4bce8eb_fk_django_co` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`),
    ADD CONSTRAINT `django_admin_log_user_id_c564eba6_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `auth_user` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;


ALTER TABLE `prewarning_info`
  ADD COLUMN `note`  varchar(255) CHARACTER SET utf8mb4 NULL DEFAULT '' AFTER `update_time`;

ALTER TABLE `check_execute_log`
  ADD COLUMN `is_delete`  tinyint(4) NULL DEFAULT 0 AFTER `status`;