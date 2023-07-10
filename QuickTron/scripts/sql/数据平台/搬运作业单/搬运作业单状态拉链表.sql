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
    pre1_date=`date -d "-7 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######��ʼִ��###########--------------------------------------------------------------"
sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- ������ҵ��״̬������ ads_carry_work_analyse_detail 

INSERT overwrite table ${ads_dbname}.ads_carry_work_analyse_detail partition(d,pt)
-- ����
SELECT '' as id, -- ����
       t3.upstream_work_id, -- ���ΰ�����ҵ��ID
       t3.work_id, -- ������ҵ��ID
       t3.first_classification, -- ����������
       t3.first_classification_desc, -- ������������������
       t3.agv_type_code, -- ���������ͱ���
       t3.agv_code, -- �����˱���
       t3.job_state, -- ��ҵ��״̬
       t3.job_state_desc, -- ��ҵ��״̬ӳ��
       t3.work_update_time, -- ����ʱ��
       t3.work_duration, -- ��ʱ
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_created_time,1,10) as d,
       t2.project_code as pt
FROM
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1) 
)t2
JOIN 
(
  -- ������ҵ��ʱ����·
  SELECT brj.project_code,
         brj.upstream_work_id,
         brj.work_id,
         ba.first_classification,
         ba.first_classification_desc,
         ba.agv_type_code,
         jsc.agv_code,
         jsc.job_state,
         CASE when jsc.job_state = 'WAITING_NEXTSTOP' THEN '�ȴ�����Ŀ�������һ������'
              when jsc.job_state = 'WAITING_RESOURCE' THEN '�ȴ���Դ'
              when jsc.job_state = 'WAITING_AGV' THEN '�ȴ�����С��'
              when jsc.job_state = 'WAITING_LIFT' THEN '�ȴ��������'
              when jsc.job_state = 'WAITING_DISPATCHER' THEN '�ȴ��������'
              when jsc.job_state = 'PROCESS' THEN '��ʼ����'
              when jsc.job_state = 'INIT' THEN '��ʼ��'
              when jsc.job_state = 'INIT_JOB' THEN '��ʼ������'
              when jsc.job_state = 'GO_TARGET' THEN '׼��ȥĿ�ĵ�'
              when jsc.job_state = 'ARRIVE_TARGET' THEN '����Ŀ�ĵ�'
              when jsc.job_state = 'WAITING_EXECUTOR' THEN '�ȴ�ʵ��'
              when jsc.job_state = 'START_EXECUTOR' THEN '��ʼʵ��'
              when jsc.job_state = 'LOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'UNLOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'WAIT_LIFT_UP' THEN '�ȴ�����'
              when jsc.job_state = 'LIFT_UP_DONE' THEN '�������'
              when jsc.job_state = 'MOVE_BEGIN' THEN '�����ƶ�'
              when jsc.job_state = 'PUT_DOWN_DONE' THEN '���»���'
              when jsc.job_state = 'DONE' THEN '�������'
              when jsc.job_state = 'ROLLBACK' THEN '�ع�'
              when jsc.job_state = 'PENDING' THEN '����'
              when jsc.job_state = 'CANCEL' THEN 'ȡ��'
              when jsc.job_state = 'ABNORMAL_COMPLETED' THEN '�쳣���'
              when jsc.job_state = 'ABNORMAL_CANCEL' THEN '�쳣ȡ��'
         end as job_state_desc,       
         jsc.rn,
         jsc.job_created_time as work_created_time,
         jsc.job_updated_time as work_update_time,
         unix_timestamp(jsc.job_updated_time) - unix_timestamp(jsc.job_updated_time_next) as work_duration
  FROM 
  (
    SELECT po.project_code,
           pj.job_id as work_id,
           po.picking_order_number as upstream_work_id
    FROM ${dwd_dbname}.dwd_picking_order_info po
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_g2p_picking_job_info pj 
      WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
    )pj
    ON nvl(pj.order_id,'unknown1') = nvl(po.id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(po.project_code,'unknown2')
    WHERE po.d >= '${pre1_date}' 
    
    UNION ALL 
    
    SELECT co.project_code,
           cj.job_id as work_id,
           co.cycle_count_number as upstream_work_id
    FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_info co
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_work_info ck 
      WHERE ck.d >= '${pre1_date}' 
    )ck
    ON nvl(co.id,'unknown1') = nvl(ck.cycle_count_id,'unknown2') AND nvl(ck.project_code,'unknown1') = nvl(co.project_code,'unknown2')
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_g2p_countcheck_job_info cj
      WHERE cj.d >= '${pre1_date}' AND cj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
    )cj
    ON nvl(cj.work_id,'unknown1') = nvl(ck.id,'unknown2') AND nvl(cj.project_code,'unknown1') = nvl(co.project_code,'unknown2')
    WHERE co.d >= '${pre1_date}' 
    
    UNION ALL 
  
    SELECT ro.project_code,
           pj.job_id as work_id,
           ro.replenish_order_number as upstream_work_id
    FROM ${dwd_dbname}.dwd_replenish_order_info ro
    JOIN 
    (
      SELECT *
      FROM  ${dwd_dbname}.dwd_g2p_guided_putaway_job_info_di pj
      WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
    )pj
    ON nvl(ro.id,'unknown1') = nvl(pj.order_id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(ro.project_code,'unknown2')
    WHERE ro.d >= '${pre1_date}'
    
    UNION ALL 
  
    SELECT pj.project_code,
           pj.job_id as work_id,
           NULL as upstream_work_id
    FROM ${dwd_dbname}.dwd_g2p_putaway_job_info_di pj
    WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
  )brj
  JOIN 
  (
    SELECT c1.d,
           c1.project_code,
           c1.job_id,
           c1.job_state,
           c1.agv_code,
           c1.job_created_time,
           c1.job_updated_time,
           c1.rn,
           c2.job_state as job_state_next,
           c2.agv_code as agv_code_next,
           c2.job_created_time as job_created_time_next,
           c2.job_updated_time as job_updated_time_next,
           c2.rn as rn_next
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c1
    LEFT JOIN 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c2
    ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2') AND c2.rn = c1.rn - 1
  )jsc
  ON nvl(brj.project_code,'unknown1') = nvl(jsc.project_code,'unknown2') AND nvl(brj.work_id,'unknown1') = nvl(jsc.job_id,'unknown2')
  LEFT JOIN 
  (
    SELECT ba.project_code,
           ba.d as cur_date,
           ba.agv_code,
           bat.agv_type_code,
           bat.agv_type_name,
           bat.first_classification,
           case when bat.first_classification = 'WORKBIN' then '���䳵'
                when bat.first_classification = 'STOREFORKBIN' then '�洢һ��ʽ'
                when bat.first_classification = 'CARRIER' then 'Ǳ��ʽ������'
                when bat.first_classification = 'ROLLER' then '��Ͳ������'
                when bat.first_classification = 'FORKLIFT' then '�Ѹ�ȫ��'
                when bat.first_classification = 'DELIVER' then 'Ͷ�ݳ�'
                when bat.first_classification = 'SC'then '������' 
           end as first_classification_desc
    FROM ${dwd_dbname}.dwd_rcs_basic_agv_info ba
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info bat
      WHERE bat.d >= '${pre1_date}'
    )bat
    ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
    WHERE ba.d >= '${pre1_date}'
  )ba
  ON nvl(jsc.d,'unknown1') = nvl(ba.cur_date,'unknown2') AND nvl(jsc.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(jsc.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2')
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2')


UNION ALL 

-- ��׼����
SELECT '' as id, -- ����
       t3.upstream_work_id, -- ���ΰ�����ҵ��ID
       t3.work_id, -- ������ҵ��ID
       t3.first_classification, -- ����������
       t3.first_classification_desc, -- ������������������
       t3.agv_type_code, -- ���������ͱ���
       t3.agv_code, -- �����˱���
       t3.job_state, -- ��ҵ��״̬
       t3.job_state_desc, -- ��ҵ��״̬ӳ��
       t3.work_update_time, -- ����ʱ��
       t3.work_duration, -- ��ʱ
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_created_time,1,10) as d,
       t2.project_code as pt
FROM
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code = 4 OR project_product_type IN ('��׼����') OR project_code = 'A51346'
)t2
JOIN 
(
  -- ������ҵ��ʱ����·
  SELECT brj.project_code,
         brj.robot_job_id as upstream_work_id,
         brj.job_id as work_id,
         ba.first_classification,
         ba.first_classification_desc,
         ba.agv_type_code,
         jsc.agv_code,
         jsc.job_state,
         CASE when jsc.job_state = 'WAITING_NEXTSTOP' THEN '�ȴ�����Ŀ�������һ������'
              when jsc.job_state = 'WAITING_RESOURCE' THEN '�ȴ���Դ'
              when jsc.job_state = 'WAITING_AGV' THEN '�ȴ�����С��'
              when jsc.job_state = 'WAITING_LIFT' THEN '�ȴ��������'
              when jsc.job_state = 'WAITING_DISPATCHER' THEN '�ȴ��������'
              when jsc.job_state = 'PROCESS' THEN '��ʼ����'
              when jsc.job_state = 'INIT' THEN '��ʼ��'
              when jsc.job_state = 'INIT_JOB' THEN '��ʼ������'
              when jsc.job_state = 'GO_TARGET' THEN '׼��ȥĿ�ĵ�'
              when jsc.job_state = 'ARRIVE_TARGET' THEN '����Ŀ�ĵ�'
              when jsc.job_state = 'WAITING_EXECUTOR' THEN '�ȴ�ʵ��'
              when jsc.job_state = 'START_EXECUTOR' THEN '��ʼʵ��'
              when jsc.job_state = 'LOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'UNLOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'WAIT_LIFT_UP' THEN '�ȴ�����'
              when jsc.job_state = 'LIFT_UP_DONE' THEN '�������'
              when jsc.job_state = 'MOVE_BEGIN' THEN '�����ƶ�'
              when jsc.job_state = 'PUT_DOWN_DONE' THEN '���»���'
              when jsc.job_state = 'DONE' THEN '�������'
              when jsc.job_state = 'ROLLBACK' THEN '�ع�'
              when jsc.job_state = 'PENDING' THEN '����'
              when jsc.job_state = 'CANCEL' THEN 'ȡ��'
              when jsc.job_state = 'ABNORMAL_COMPLETED' THEN '�쳣���'
              when jsc.job_state = 'ABNORMAL_CANCEL' THEN '�쳣ȡ��'
         end as job_state_desc,       
         jsc.rn,
         jsc.job_created_time as work_created_time,
         jsc.job_updated_time as work_update_time,
         unix_timestamp(jsc.job_updated_time) - unix_timestamp(jsc.job_updated_time_next) as work_duration
  FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di brj
  JOIN 
  (
    SELECT c1.d,
           c1.project_code,
           c1.job_id,
           c1.job_state,
           c1.agv_code,
           c1.job_created_time,
           c1.job_updated_time,
           c1.rn,
           c2.job_state as job_state_next,
           c2.agv_code as agv_code_next,
           c2.job_created_time as job_created_time_next,
           c2.job_updated_time as job_updated_time_next,
           c2.rn as rn_next
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c1
    LEFT JOIN 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c2
    ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2') AND c2.rn = c1.rn - 1
  )jsc
  ON nvl(brj.project_code,'unknown1') = nvl(jsc.project_code,'unknown2') AND nvl(brj.job_id,'unknown1') = nvl(jsc.job_id,'unknown2')
  LEFT JOIN 
  (
    SELECT ba.project_code,
           ba.d as cur_date,
           ba.agv_code,
           bat.agv_type_code,
           bat.agv_type_name,
           bat.first_classification,
           case when bat.first_classification = 'WORKBIN' then '���䳵'
                when bat.first_classification = 'STOREFORKBIN' then '�洢һ��ʽ'
                when bat.first_classification = 'CARRIER' then 'Ǳ��ʽ������'
                when bat.first_classification = 'ROLLER' then '��Ͳ������'
                when bat.first_classification = 'FORKLIFT' then '�Ѹ�ȫ��'
                when bat.first_classification = 'DELIVER' then 'Ͷ�ݳ�'
                when bat.first_classification = 'SC'then '������' 
           end as first_classification_desc
    FROM ${dwd_dbname}.dwd_rcs_basic_agv_info ba
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info bat
      WHERE bat.d >= '${pre1_date}'
    )bat
    ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
    WHERE ba.d >= '${pre1_date}'
  )ba
  ON nvl(jsc.d,'unknown1') = nvl(ba.cur_date,'unknown2') AND nvl(jsc.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(jsc.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2')
  WHERE brj.d >= '${pre1_date}' AND brj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2')

UNION ALL 

-- QP����
SELECT '' as id, -- ����
       t3.upstream_work_id, -- ���ΰ�����ҵ��ID
       t3.work_id, -- ������ҵ��ID
       t3.first_classification, -- ����������
       t3.first_classification_desc, -- ������������������
       t3.agv_type_code, -- ���������ͱ���
       t3.agv_code, -- �����˱���
       t3.job_state, -- ��ҵ��״̬
       t3.job_state_desc, -- ��ҵ��״̬ӳ��
       t3.work_update_time, -- ����ʱ��
       t3.work_duration, -- ��ʱ
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_created_time,1,10) as d,
       t2.project_code as pt
FROM
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type IN ('Quickpick','�������QP') OR project_code IN ('A51274','C35052')
)t2
JOIN 
(
-- ������ҵ��ʱ����·
  SELECT brj.project_code,
         brj.robot_job_id as upstream_work_id,
         qe.work_id,
         ba.first_classification,
         ba.first_classification_desc,
         ba.agv_type_code,
         jsc.agv_code,
         jsc.job_state,
         CASE when jsc.job_state = 'WAITING_NEXTSTOP' THEN '�ȴ�����Ŀ�������һ������'
              when jsc.job_state = 'WAITING_RESOURCE' THEN '�ȴ���Դ'
              when jsc.job_state = 'WAITING_AGV' THEN '�ȴ�����С��'
              when jsc.job_state = 'WAITING_LIFT' THEN '�ȴ��������'
              when jsc.job_state = 'WAITING_DISPATCHER' THEN '�ȴ��������'
              when jsc.job_state = 'PROCESS' THEN '��ʼ����'
              when jsc.job_state = 'INIT' THEN '��ʼ��'
              when jsc.job_state = 'INIT_JOB' THEN '��ʼ������'
              when jsc.job_state = 'GO_TARGET' THEN '׼��ȥĿ�ĵ�'
              when jsc.job_state = 'ARRIVE_TARGET' THEN '����Ŀ�ĵ�'
              when jsc.job_state = 'WAITING_EXECUTOR' THEN '�ȴ�ʵ��'
              when jsc.job_state = 'START_EXECUTOR' THEN '��ʼʵ��'
              when jsc.job_state = 'LOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'UNLOAD_COMPLETED' THEN '�������'
              when jsc.job_state = 'WAIT_LIFT_UP' THEN '�ȴ�����'
              when jsc.job_state = 'LIFT_UP_DONE' THEN '�������'
              when jsc.job_state = 'MOVE_BEGIN' THEN '�����ƶ�'
              when jsc.job_state = 'PUT_DOWN_DONE' THEN '���»���'
              when jsc.job_state = 'DONE' THEN '�������'
              when jsc.job_state = 'ROLLBACK' THEN '�ع�'
              when jsc.job_state = 'PENDING' THEN '����'
              when jsc.job_state = 'CANCEL' THEN 'ȡ��'
              when jsc.job_state = 'ABNORMAL_COMPLETED' THEN '�쳣���'
              when jsc.job_state = 'ABNORMAL_CANCEL' THEN '�쳣ȡ��'
         end as job_state_desc,       
         jsc.rn,
         jsc.job_created_time as work_created_time,
         jsc.job_updated_time as work_update_time,
         unix_timestamp(jsc.job_updated_time) - unix_timestamp(jsc.job_updated_time_next) as work_duration
  FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di brj
  JOIN 
  (
    SELECT qe.project_code,
           qe.job_id,
           qe.d,
           qm.job_id as work_id,
           qm.source_point_code,
           qm.target_point_code,
           qm.agv_code,
           qm.job_state,
           qm.job_created_time,
           qm.job_updated_time
    FROM ${dwd_dbname}.dwd_g2p_si_qp_extend_info qe
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_g2p_si_qp_move_job_info_di qm
      WHERE qm.d >= '${pre1_date}'
    )qm
    ON nvl(qe.project_code,'unknown1') = nvl(qm.project_code,'unknown2') AND nvl(qe.move_job_id,'unknown1') = nvl(qm.id,'unknown2')
    WHERE qe.d >= '${pre1_date}'
    
    UNION ALL 
    
    SELECT qe.project_code,
           qe.job_id,
           qe.d,
           qt.job_id as work_id,
           qt.source_point_code,
           qt.target_point_code,
           qt.agv_code,
           qt.job_state,
           qt.job_created_time,
           qt.job_updated_time
    FROM ${dwd_dbname}.dwd_g2p_si_qp_extend_info qe
    JOIN
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_g2p_si_qp_transfer_job_info qt
      WHERE qt.d >= '${pre1_date}'
    )qt
    ON nvl(qe.project_code,'unknown1') = nvl(qt.project_code,'unknown2') AND nvl(qe.transfer_job_id,'unknown1') = nvl(qt.id,'unknown2')
    WHERE qe.d >= '${pre1_date}'
  )qe
  ON nvl(brj.project_code,'unknown1') = nvl(qe.project_code,'unknown2') AND nvl(brj.robot_job_id,'unknown1') = nvl(qe.job_id,'unknown2') AND nvl(brj.d,'unknown1') = nvl(qe.d,'unknown2')
  JOIN 
  (
    SELECT c1.d,
           c1.project_code,
           c1.job_id,
           c1.job_state,
           c1.agv_code,
           c1.job_created_time,
           c1.job_updated_time,
           c1.rn,
           c2.job_state as job_state_next,
           c2.agv_code as agv_code_next,
           c2.job_created_time as job_created_time_next,
           c2.job_updated_time as job_updated_time_next,
           c2.rn as rn_next
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c1
    LEFT JOIN 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info jsc1
      WHERE jsc1.d >= '${pre1_date}'
    )c2
    ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2') AND c2.rn = c1.rn - 1
  )jsc
  ON nvl(brj.project_code,'unknown1') = nvl(jsc.project_code,'unknown2') AND nvl(qe.work_id,'unknown1') = nvl(jsc.job_id,'unknown2')
  LEFT JOIN 
  (
    SELECT ba.project_code,
           ba.d as cur_date,
           ba.agv_code,
           bat.agv_type_code,
           bat.agv_type_name,
           bat.first_classification,
           case when bat.first_classification = 'WORKBIN' then '���䳵'
                when bat.first_classification = 'STOREFORKBIN' then '�洢һ��ʽ'
                when bat.first_classification = 'CARRIER' then 'Ǳ��ʽ������'
                when bat.first_classification = 'ROLLER' then '��Ͳ������'
                when bat.first_classification = 'FORKLIFT' then '�Ѹ�ȫ��'
                when bat.first_classification = 'DELIVER' then 'Ͷ�ݳ�'
                when bat.first_classification = 'SC'then '������' 
           end as first_classification_desc
    FROM ${dwd_dbname}.dwd_rcs_basic_agv_info ba
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info bat
      WHERE bat.d >= '${pre1_date}'
    )bat
    ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
    WHERE ba.d >= '${pre1_date}'
  )ba
  ON nvl(jsc.d,'unknown1') = nvl(ba.cur_date,'unknown2') AND nvl(jsc.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(jsc.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2')
  WHERE brj.d >= '${pre1_date}' AND brj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2');
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "
