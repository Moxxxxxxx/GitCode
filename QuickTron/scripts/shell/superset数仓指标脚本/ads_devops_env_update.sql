-- HIVE建表：
CREATE TABLE IF NOT EXISTS ads.ads_devops_env_update
(
  `owner_name`                          string comment '操作者',
  `env_id`                              bigint comment '环境id',
  `env_deploy_name`                     string comment '环境名称',
  `server_master_ip`                    string comment '主服务器',
  `server_slave_ip`                     string comment '从服务器',
  `env_update_status`                   string comment '更新状态',
  `submit_time`                         string comment '更新时间',
  `error_info`                          string comment '错误信息',
  `dtk_user_name`                       string comment '钉钉用户名',
  `team_ft`                             string comment '一级部门',
  `team_group`                          string comment '二级部门',
  `team_sub_group`                      string comment '三级部门',
  `emp_position`                        string comment '职位'
) comment 'devops更新流水表'
row format delimited fields terminated by '\t'

-- SQL建表：
DROP TABLE IF EXISTS ads_devops_env_update;
CREATE TABLE `ads_devops_env_update` (
  `id`                                  bigint(32) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `owner_name`                           varchar(50) DEFAULT NULL comment '操作者',
  `env_id`                      bigint(32) DEFAULT NULL comment '环境id',
  `env_deploy_name`             varchar(50) DEFAULT NULL comment '环境名称',
  `server_master_ip`           varchar(50) DEFAULT NULL comment '主服务器',
  `server_slave_ip`                 varchar(50) DEFAULT NULL comment '从服务器',
  `env_update_status`              varchar(50) DEFAULT NULL comment '更新状态',
  `submit_time`            datetime DEFAULT NULL comment '更新时间',
  `error_info`           varchar(50) DEFAULT NULL comment '错误信息',
  `dtk_user_name`                 varchar(50) DEFAULT NULL comment '钉钉用户名',
  `team_ft`            varchar(50) DEFAULT NULL comment '一级部门',
  `team_group`               varchar(50) DEFAULT NULL comment '二级部门',
  `team_sub_group`            varchar(50) DEFAULT NULL comment '三级部门',
  `emp_position`             varchar(50) DEFAULT NULL comment '职位',
  `create_time`                         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                         datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='devops更新流水表'

-- XXL-JOB
#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 监控平台钉钉自动工单表
#-- 注意 ： 
#-- 输入表 : 
#-- 输出表 ：ads.ads_devops_env_update
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2022-08-03 CREATE
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--devops更新流水表 ads_devops_env_update 

with emp as (
 SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as org_role_type,
                    te.is_job,
                    tmp.is_need_fill_manhour,
                    te.hired_date,
                    te.quit_date,
                    te.emp_position
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      m.is_need_fill_manhour,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
)
INSERT overwrite table ${ads_dbname}.ads_devops_env_update
select 
  a.user_cname as owner_name
  ,a.env_id as env_id
  ,a.env_deploy_name
  ,a.server_master_ip
  ,a.server_slave_ip
  ,CASE 
  when env_update_status = 6 then '失败'
  when env_update_status = 5 then '成功'
  END as env_update_status
  ,a.submit_time
  ,a.error_info
  ,nvl(emp.user_name,'钉钉无相关信息') as dtk_user_name
  ,nvl(emp.team_ft,'钉钉无相关信息') as team_ft
  ,nvl(emp.team_group,'钉钉无相关信息') as team_group
  ,nvl(emp.team_sub_group,'钉钉无相关信息') as team_sub_group
  ,nvl(emp.emp_position,'钉钉无相关信息') as emp_position
from 
  ${dwd_dbname}.dwd_devops_env_update_record_info_di a 
  left join emp on a.dingding_id = emp.emp_id;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_devops_env_update;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_devops_env_update （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_devops_env_update \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_devops_env_update \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "owner_name,env_id,env_deploy_name,server_master_ip,server_slave_ip,env_update_status,submit_time,error_info,dtk_user_name,team_ft,team_group,team_sub_group,emp_position"

echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "

