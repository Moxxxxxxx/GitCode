#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
project_code=A51118

    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- �豸�б� ads_single_project_equipment_detail 

INSERT overwrite table ${ads_dbname}.ads_single_project_equipment_detail
SELECT '' as id, -- ����
       b.project_code, -- ��Ŀ����
       '����վ' as equiqment_name, -- �豸����
       b.basic_units_qyt as equiqment_num, -- �豸����
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_basic_live_data_offline_info b
WHERE b.basic_code = 0003 -- ����վ
UNION ALL
SELECT '' as id, -- ����
       b.project_code, -- ��Ŀ����
       '����' as equiqment_name, -- �豸����
       b.basic_units_qyt as equiqment_num, -- �豸����
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_basic_live_data_offline_info b
WHERE b.basic_code = 0004 -- ����
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

# �������������ڰ���ȡ�������ڣ����û��������ȡ��ǰʱ���ǰһ��
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi


#����datax����
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_project_view_project_equipment_detail.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo