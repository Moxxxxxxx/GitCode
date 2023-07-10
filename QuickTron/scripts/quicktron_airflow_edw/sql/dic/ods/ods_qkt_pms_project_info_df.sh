#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集pms项目基础信息表数据
#-- 注意 ： 每天全量
#-- 输入表 : quality_data.pms_project_card_info_maintable
#-- 输出表 ：ods.ods_qkt_pms_project_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-10-11 CREATE 
#-- 2 wangziming 2022-11-29 modify 增加字段立项日期（项目交接日期）
# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_pms_project_info_df
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=quality_data
mysql_table=pms_project_card_info_maintable
datax_incre_column=datax_update_time
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

## hcatalog不支持文件覆盖，为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table/d=$pre1_date
if [ $? -eq 0 ] ;then 
    hdfs dfs -rm -r $target_dir$table/d=$pre1_date
    echo 'clean up'
else 
    echo 'not clean up' 
fi



 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
id, scrcrq, sfyflxm, ztrq, 
xcjl, gnswzg, tdzj, zbdqrq, gdlj, 
zqrq, swzl, zpm, xmbmwb, scfj, sfshtl, 
xmzzq, fagws, sfjhxz, dhqspd, uatpd, 
sxpd, zypd, zshpd, sfzj, ddlx, zzyh, 
bz, rmbbhshtze, se, ssmj, q, szgj, 
szcs, szsf, rmbhtzje, htzje, mytk,
cj, jtkh, xszz, sfqzxm, khfz, fagw, 
shfwdj, htbh, htmc, xmjjbgcs, cpx1, xt, 
dqy1, zxxmbh, xmmc, xmbm, sqbh, shbh, khmc, 
khdj, xmlx, hylx, xmjl, shfzr, pmo, xmdj, 
jhksrq, jhjsrq, xmzt, xsqy, xsjl, xsqyzj, xmjj, 
gzft, spm, khbm2, xmjd, xmjllz, pmolz, spmlz, shfzrlz, 
fagwlz, xsjllz, xsqyzjlz, sbdhqdrq, uatqdrq, sxqdrq, zybgqdrq, 
xmzshrq, ljbgcs, jhbgcs, jhbzx, pe, swzg, sxspwcrq, ztbh, 
xmnwblb, xmxxzktnbid,lxrq, dt_create_time, dt_update_time
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--hcatalog-partition-keys d \
--hcatalog-partition-values ${pre1_date} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

