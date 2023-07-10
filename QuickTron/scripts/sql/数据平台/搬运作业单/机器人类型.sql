#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# �������������ڰ���ȡ�������ڣ����û��������ȡ��ǰʱ���ǰһ��
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- ���������� ads_carry_order_agv_type 

INSERT overwrite table ${ads_dbname}.ads_carry_order_agv_type

SELECT '' as id, -- ����
       t.first_classification as carry_type,
       case when t.first_classification = 'WORKBIN' then '���䳵'
            when t.first_classification = 'STOREFORKBIN' then '�洢һ��ʽ'
            when t.first_classification = 'CARRIER' then 'Ǳ��ʽ������'
            when t.first_classification = 'ROLLER' then '��Ͳ������'
            when t.first_classification = 'FORKLIFT' then '�Ѹ�ȫ��'
            when t.first_classification = 'DELIVER' then 'Ͷ�ݳ�'
            when t.first_classification = 'SC'then '������' 
       end as carry_type_des,
       t.project_code,
       t.agv_type_code as agv_type,
       nvl(a.agv_type,t.agv_type_code) as amr_type,
       nvl(a.agv_type_name,t.agv_type_name) as amr_type_desc,
       t.agv_code,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df t
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
ON t.project_code = a.project_code AND a.agv_code = t.agv_code
WHERE t.d = '${pre1_date}' AND (a.project_code is null OR a.active_status = '��Ӫ��');
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash



#����datax����
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_carry_order_agv_type.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo