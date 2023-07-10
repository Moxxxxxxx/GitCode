with project_view_detail as 
(
SELECT '' as id, -- ����
       case when b.project_code like 'A%' THEN 'A'
            when b.project_code like 'C%' THEN 'C'
            when b.project_code like 'FH-%' THEN 'FH'
            else 'δ֪' end as project_code_class, -- ��Ŀ��������
       b.project_code, -- ��Ŀ����
       b.project_sale_code, -- ��ǰ����
       b.project_name, -- ��Ŀ����
       CONCAT(b.project_code,'-',b.project_name) as project_info,
       IF(b.project_product_name is null,'δ֪',b.project_product_name) as project_product_name, -- ��Ʒ��
       IF(t2.project_code is not null,'��ʷ��Ŀ','����Ŀ') as project_type, -- ��Ŀ����
       b.project_dispaly_state, -- ��Ŀ�׶�
       b.project_dispaly_state_group, -- ��Ŀ�׶���
       IF(b.project_ft is null,'δ֪',b.project_ft) as project_ft, -- ����/FT => <������������>ft
       b.project_priority, -- ��Ŀ����
       IF(b.project_current_version is null,'δ֪',b.project_current_version) as project_current_version , -- �汾��
       t1.sales_area_director, -- owner
       b.project_area_place as project_area, -- ����-PM
       t1.pm_name, -- PM
       b.sap_counselor, -- ����
       t1.sales_person, -- ����
       t1.pre_sales_consultant, -- ��ǰ����
       t6.contract_amount as amount, -- ��ͬ�����±�
       IF(b.contract_signed_year is null,t2.contract_sign_year,b.contract_signed_year) as contract_signed_year, -- ��ͬ����
       IF(b.contract_signed_date is null,t2.contract_sign_date,b.contract_signed_date) as contract_signed_date, -- ��ͬ����
       IF(t4.fhsl is null and t3.cgsl is null,null,IF(CAST(t4.fhsl / t3.cgsl as decimal(10,4)) is null,0,CAST(t4.fhsl / t3.cgsl as decimal(10,4)))) as deliver_goods_achieving_rate, -- ���������
       b.pre_project_approval_time, -- ǰ���������ʱ��
       b.project_handover_end_time, -- �����������ʱ��
       b.expect_online_date, -- Ԥ������ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ',t7.equitment_arrival_date,b.online_date) as online_date, -- ʵ������ʱ�� => <���߱�����̱�>����ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ',t7.end_time_month,b.online_process_month) as online_process_month, -- ���ߵ������·� => <���߱�����̱�>���ʱ��
       date(CONCAT(IF(b.project_type_name = '��Ӳ����Ŀ',t7.end_time_month,b.online_process_month),'-01')) as online_process_month_begin, -- ���ߵ������³� => <���߱�����̱�>���ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ' and t7.project_code is not null,'������',IF(t2.project_code is not null,'������',b.is_online)) as is_online, -- �Ƿ�����
       b.sap_entry_date, -- ʵʩ�볡ʱ��
       b.online_times, -- ����ʱ��
       IF(t2.project_code is not null,NULL,b.no_online_times) as no_online_times, -- ����δ��������
       b.expect_final_inspection_date, -- Ԥ������ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ',t7.equitment_arrival_date,b.final_inspection_date) as final_inspection_date, -- ʵ������ʱ�� => <���鱨����̱�>��������ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ',t7.end_time_month,b.final_inspection_process_month) as final_inspection_process_month, -- ���鵥�����·� => <���鱨����̱�>���ʱ��
       date(CONCAT(IF(b.project_type_name = '��Ӳ����Ŀ',t7.end_time_month,b.final_inspection_process_month),'-01')) as final_inspection_process_month_begin, -- ���鵥�����³� => <���鱨����̱�>���ʱ��
       IF(b.project_type_name = '��Ӳ����Ŀ' and t7.project_code is not null,'������',IF(t2.project_code is not null,'������',b.is_final_inspection)) as is_final_inspection, -- �Ƿ�����
       IF(b.project_type_name = '��Ӳ����Ŀ' and t7.project_code is not null,0,b.final_inspection_times) as final_inspection_times, -- ����ʱ��
       b.no_final_inspection_times, -- ����δ��������
       CASE when t2.project_code is not null AND b.project_dispaly_state_group != '��Ŀ����' THEN '������δ����' -- ��ʷ��Ŀ+��Ŀδ����
            when t2.project_code is not null AND b.project_dispaly_state_group = '��Ŀ����' THEN '�ѽ���' -- ��ʷ��Ŀ+��Ŀ�ѽ���
            when b.project_type_name = '��Ӳ����Ŀ' AND t7.project_code is null then 'δ����δ����' -- ��Ӳ����Ŀ+�豸�����������
            when b.project_type_name = '��Ӳ����Ŀ' AND t7.project_code is not null then '�ѽ���' -- ��Ӳ����Ŀ+�豸��������δ���
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = 'δ����' AND b.is_final_inspection = 'δ����' AND t4.fhsl is null THEN 'δ����δ����' -- �ⲿ��Ŀ+δ����δ����+δ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = 'δ����' AND b.is_final_inspection = 'δ����' AND t4.fhsl is not null THEN '�ѷ���δ����' -- �ⲿ��Ŀ+δ����δ����+�ѷ���
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = '������' AND b.is_final_inspection = 'δ����' THEN '������δ����' -- �ⲿ��Ŀ+������δ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_final_inspection = '������' AND b.project_dispaly_state_group != '��Ŀ����' THEN '������δ����' -- �ⲿ��Ŀ+������+��Ŀδ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_final_inspection = '������' AND b.project_dispaly_state_group = '��Ŀ����' THEN '�ѽ���' -- �ⲿ��Ŀ+������+��Ŀ�ѽ���
       end as project_stage, -- ��Ŀ�׶�
       CASE when t2.project_code is not null AND b.project_dispaly_state_group != '��Ŀ����' THEN '����׶�' -- ��ʷ��Ŀ+��Ŀδ����
            when t2.project_code is not null AND b.project_dispaly_state_group = '��Ŀ����' THEN '�ѽ���' -- ��ʷ��Ŀ+��Ŀ�ѽ���
            when b.project_type_name = '��Ӳ����Ŀ' AND t7.project_code is null then '�����׶�(Ӳ����Ŀ)' -- ��Ӳ����Ŀ+�豸�����������
            when b.project_type_name = '��Ӳ����Ŀ' AND t7.project_code is not null then '�ѽ���(Ӳ����Ŀ)' -- ��Ӳ����Ŀ+�豸��������δ���
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = 'δ����' AND b.is_final_inspection = 'δ����' AND t4.fhsl is null THEN '�����׶�' -- �ⲿ��Ŀ+δ����δ����+δ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = 'δ����' AND b.is_final_inspection = 'δ����' AND t4.fhsl is not null THEN '���߽׶�' -- �ⲿ��Ŀ+δ����δ����+�ѷ���
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_online = '������' AND b.is_final_inspection = 'δ����' THEN '���ս׶�' -- �ⲿ��Ŀ+������δ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_final_inspection = '������' AND b.project_dispaly_state_group != '��Ŀ����' THEN '����׶�' -- �ⲿ��Ŀ+������+��Ŀδ����
            when b.project_type_name != '��Ӳ����Ŀ' AND b.is_final_inspection = '������' AND b.project_dispaly_state_group = '��Ŀ����' THEN '�ѽ���' -- �ⲿ��Ŀ+������+��Ŀ�ѽ���
       end as project_progress_stage, -- ��Ŀ���Ƚ׶�
       NULL as project_gm, -- ��Ŀë����
       NULL as complain_num, -- �ͻ�Ͷ�ߴ���
       IF(t5.zeroweek_work_num is null,0,t5.zeroweek_work_num) as zeroweek_work_num, -- ��ǰ��
       IF(t5.oneweek_work_num is null,0,t5.oneweek_work_num) as oneweek_work_num, -- ��ǰ��+ǰһ�� 
       IF(t5.twoweek_work_num is null,0,t5.twoweek_work_num) as twoweek_work_num, -- ��ǰ��+ǰ���� 
       IF(t5.threeweek_work_num is null,0,t5.threeweek_work_num) as threeweek_work_num, -- ��ǰ��+ǰ���� 
       IF(t5.fourweek_work_num is null,0,t5.fourweek_work_num) as fourweek_work_num, -- ��ǰ��+ǰ���� 
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name,
         tt.project_type_name,
         tt.project_dispaly_state,
         tt.project_dispaly_state_group,
         tt.project_ft,
         tt.project_priority,
         tt.project_current_version,
         tt.sap_counselor,
         tt.contract_signed_year,
         tt.contract_signed_date,
         tt.expect_online_date,
         tt.online_date,
         tt.is_online,
         tt.sap_entry_date,
         tt.online_times,
         tt.no_online_times,
         tt.expect_final_inspection_date, 
         tt.final_inspection_date,
         tt.is_final_inspection,
         tt.final_inspection_times,
         tt.no_final_inspection_times,
         tt.project_product_name,
         tt.project_area_place,
         tt.online_process_month,
         tt.final_inspection_process_month,
         tt.project_handover_end_time,
         tt.project_handover_start_time,
         tt.pre_project_approval_time
  FROM 
  (
    SELECT b.project_code as true_project_code, -- ��Ŀ����
           b.project_sale_code as true_project_sale_code, -- ��ǰ����
           b2.project_code,
           b2.project_sale_code,
           b.project_name, -- ��Ŀ����
           case when b.project_type_id = 0 then '�ⲿ��Ŀ'
                when b.project_type_id = 1 then '��˾�ⲿ��Ŀ'
                when b.project_type_id = 4 then '��ǰ��Ŀ'
                when b.project_type_id = 7 then 'Ӳ������Ŀ'
                when b.project_type_id = 8 then '��Ӳ����Ŀ'
                when b.project_type_id = 9 then '��Ӫ����Ŀ'
                end as project_type_name,
           IF(b.project_dispaly_state = 'UNKNOWN',NULL,b.project_dispaly_state) as project_dispaly_state, -- ��Ŀ�׶�
           case when b.project_dispaly_state = '0.δ��ʼ' OR b.project_dispaly_state = '1.����/�����׶�' OR b.project_dispaly_state = '2.����ȷ��/�ֽ�' OR b.project_dispaly_state = '3.��ƿ���/����' then '����ȷ��/�ֽ�׶�'
                when b.project_dispaly_state = '4.�ɹ�/����' OR b.project_dispaly_state = '5.����/�ֳ�ʵʩ' then '�����׶�'
                when b.project_dispaly_state = '6.����/����/�û���ѵ' then '����ʵʩ�׶�'
                when b.project_dispaly_state = '7.����' then '���ս׶�'
                when b.project_dispaly_state like '8.�ƽ���ά/ת�ۺ�' then '�ۺ��ƽ��׶�'
                when b.project_dispaly_state = '9.��Ŀ����' then '��Ŀ����'
                when b.project_dispaly_state = '10.��Ŀ��ͣ' then '��Ŀ��ͣ'
                when b.project_dispaly_state = '11.��Ŀȡ��' then '��Ŀȡ��'
                else NULL end as project_dispaly_state_group, -- ��Ŀ�׶���
           IF(length(b.project_attr_ft) = 0,NULL,b.project_attr_ft) as project_ft, -- ����/FT => <������������>ft
           b.project_priority, -- ��Ŀ����
           IF(b.project_current_version = 'UNKNOWN',NULL,b.project_current_version) as project_current_version, -- �汾��
           b.sap_counselor, -- ����
           date_format(b.contract_signed_date,'yyyy') as contract_signed_year, -- ��ͬ�������
           b.contract_signed_date, -- ��ͬ����
           b.expect_online_date, -- Ԥ������ʱ��
           b.online_date, -- ʵ������ʱ�� => <���߱�����̱�>����ʱ��
           IF(b.online_process_approval_time is null,'δ����','������') as is_online, -- �Ƿ�����
           b.sap_entry_date, -- ʵʩ�볡ʱ��
           datediff(b.online_date,b.sap_entry_date) as online_times, -- ����ʱ��
           IF(b.sap_entry_date is not null AND b.online_date is null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.sap_entry_date),NULL) as no_online_times, -- ����δ��������
           b.expect_final_inspection_date, -- Ԥ������ʱ��
           b.final_inspection_date, -- ʵ������ʱ�� => <���鱨����̱�>��������ʱ��
           IF(b.final_inspection_process_approval_time is null,'δ����','������') as is_final_inspection, -- �Ƿ�����
           datediff(b.final_inspection_date,b.online_date) as final_inspection_times, -- ����ʱ��
           IF(b.final_inspection_date is null AND b.online_date is not null,datediff(DATE_ADD(CURRENT_DATE(), -1),b.online_date),NULL) as no_final_inspection_times, -- ����δ��������
           IF(b.project_product_name = 'UNKNOWN',NULL,b.project_product_name) as project_product_name, -- ��Ʒ��
           IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'����',b.project_area_place) as project_area_place, -- ����-PM
           date_format(b.online_process_approval_time,'yyyy-MM') as online_process_month, -- ���ߵ������·� => <���߱�����̱�>���ʱ��
           date_format(b.final_inspection_process_approval_time,'yyyy-MM') as final_inspection_process_month, -- ���鵥�����·� => <���鱨����̱�>���ʱ��
           h.end_time as project_handover_end_time, -- �����������ʱ��
           h.start_time as project_handover_start_time, -- ����������ʼʱ��
           b.pre_project_approval_time, -- ǰ���������ʱ��
           row_number()over(PARTITION by b2.project_sale_code order by b2.project_code,h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_share_project_base_info_df b
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b2
    ON (b.project_code = b2.project_code or b.project_sale_code = b2.project_sale_code) AND b.d =b2.d 
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE b.d = DATE_ADD(CURRENT_DATE(), -1)   
      AND (b.project_code LIKE 'FH-%' OR b.project_code LIKE 'A%' OR b.project_code LIKE 'C%') -- ֻ����FH/A/C��ͷ����Ŀ
      AND b.project_type_id IN (0,1,4,7,8,9) -- ֻ�����ⲿ��Ŀ/��˾�ⲿ��Ŀ/��ǰ��Ŀ/Ӳ������Ŀ/��Ӳ����Ŀ/��Ӫ����Ŀ
      AND (b.is_business_project = 0 OR (b.is_business_project = 1 AND b.is_pre_project = 1)) -- ֻ���������̻��������̻�Ҳ��ǰ�õ���Ŀ
  )tt
  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
)b
-- ��Ŀ������Ϣ
LEFT JOIN 
(
  SELECT p.project_code,
         p.pm_name,
         p.sales_area_director,
         p.sales_person,
         p.pre_sales_consultant
  FROM ${dwd_dbname}.dwd_bpm_project_info_ful p
  WHERE (p.project_code LIKE 'FH-%' OR p.project_code LIKE 'A%' OR p.project_code LIKE 'C%') -- ֻ����FH/A/C��ͷ����Ŀ
    AND p.project_type IN ('�ⲿ��Ŀ','�ⲿ��Ʒ��Ŀ','��ǰ��Ŀ','Ӳ������Ŀ','��Ӳ����Ŀ','��Ӫ����Ŀ')
) t1
ON b.project_code = t1.project_code
-- ��ʷ��Ŀ������Ϣ
LEFT JOIN 
(
  SELECT f.project_code,
         f.contract_sign_date,
         date_format(f.contract_sign_date,'yyyy') as contract_sign_year
  FROM ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful f
) t2
ON b.project_code = t2.project_code
-- ���ϲɹ�����
LEFT JOIN
(
  SELECT tt.true_project_code as project_code,
         SUM(tt.cgsl) as cgsl
  FROM
  (
    SELECT b.project_code as true_project_code,
           b.project_sale_code,
           tmp.project_code,
           h.start_time,
           sum(if(tmp.Number1 is null,0,tmp.Number1)) as cgsl,
           row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
    FROM
    (
      --�ɹ�����
      SELECT a.project_code, -- ��Ŀ����
             b.string22, -- ���ϱ���
             b.string23, -- ��������
             b.string24, -- ����ͺ�
             b.string26, -- ��λ
             b.Number1 -- �ɹ�����
      FROM ${dwd_dbname}.dwd_bpm_materials_purchase_request_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 is not null
      WHERE a.end_time is not null AND a.subscribe_type != '���ϲɹ�����'--�����ѽ���(������ɣ���������)--20220214 �ɹ���������ȥ�����ϲɹ�����BY����
   
      UNION ALL 
      --�ɹ�������
      SELECT a.project_code,
             b.string22,
             null,
             null,
             null,
             b.Number3
      FROM ${dwd_dbname}.dwd_bpm_purchase_request_change_info_ful a
      LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b 
      ON a.flow_id = b.FlowID AND b.string22 is not null
      WHERE end_time is not null --�����ѽ���(������ɣ���������)
    )tmp
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
    ON b.d = DATE_ADD(CURRENT_DATE(), -1) and (b.project_code = tmp.project_code or b.project_sale_code = tmp.project_code)
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE tmp.string22 not like 'R1S9%' 
      AND tmp.string22 not like 'R2S9%'
      AND tmp.string22 not like 'R3S9%'
      AND tmp.string22 not like 'R4S9%'
      AND (tmp.string22 not like 'R5S9%' or tmp.string22 in('R5S90518','R5S90528')) --R5S90518:���������
      AND (tmp.string22 not like 'R6S9%' or tmp.string22 in('R6S90077','R6S90078','R6S90080','R6S90058')) --������������
      AND tmp.string22 not like 'R7S9%'
      AND tmp.string22 not like 'R8S9%'
      AND tmp.string22 not like 'R9S9%'
    GROUP BY b.project_code,b.project_sale_code,tmp.project_code,h.start_time
    HAVING sum(if(tmp.Number1 is null,0,tmp.Number1)) > 0
  )tt
  WHERE tt.rn = 1
  GROUP BY tt.true_project_code
)t3
ON b.project_code = t3.project_code
-- �����ѷ�������
LEFT JOIN
(
  SELECT tt.true_project_code,
         SUM(tt.fhsl) as fhsl
  FROM 
  (
    SELECT a.project_code as string21,
           s.project_code as true_project_code,
           s.project_sale_code,
           h.start_time,
           sum(if(b.Number1 is null,0,b.Number1)) fhsl,
           row_number()over(PARTITION by a.project_code order by h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_project_delivery_approval_info_ful a
    LEFT JOIN ${dwd_dbname}.dwd_bpm_app_k3flowentry_info_ful b  
    ON a.flow_id = b.FlowID AND b.string14 is not null
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = a.project_code or s.project_sale_code = a.project_code)
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON s.project_code = h.project_code AND h.rn = 1
    WHERE a.approve_status != '50' --�����Զ���ֹ
    GROUP BY a.project_code,h.start_time,s.project_code,s.project_sale_code
    )tt
  WHERE tt.rn = 1
  GROUP BY tt.true_project_code
)t4
ON b.project_code = t4.true_project_code
LEFT JOIN 
(
  SELECT tmp.true_project_code,
         SUM(tmp.zeroweek_work_num) as zeroweek_work_num,
         SUM(tmp.oneweek_work_num) as oneweek_work_num,
         SUM(tmp.twoweek_work_num) as twoweek_work_num,
         SUM(tmp.threeweek_work_num) as threeweek_work_num,
         SUM(tmp.fourweek_work_num) as fourweek_work_num
  FROM 
  (
    SELECT tt.*,
           IF(s.project_code is null,tt.project_code,s.project_code) as true_project_code,
           s.project_sale_code,
           h.project_code as external_project_code,
           h.pre_sale_code,
           row_number()over(PARTITION by s.project_sale_code order by h.start_time desc)rn
    FROM
    (
      SELECT w.project_code,
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) THEN 1 ELSE 0 END) as zeroweek_work_num, -- ��ǰ��
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 1 THEN 1 ELSE 0 END) as oneweek_work_num, -- ��ǰ��+ǰһ�� 
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 2 THEN 1 ELSE 0 END) as twoweek_work_num, -- ��ǰ��+ǰ����
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 3 THEN 1 ELSE 0 END) as threeweek_work_num, -- ��ǰ��+ǰ����
             SUM(CASE when weekofyear(w.created_time) <= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) and weekofyear(w.created_time) >= weekofyear(DATE_ADD(CURRENT_DATE(), -1)) - 4 THEN 1 ELSE 0 END) as fourweek_work_num -- ��ǰ��+ǰ����
      FROM ${dwd_dbname}.dwd_ones_work_order_info_df w -- ones����ϵͳ
      WHERE w.d = DATE_ADD(CURRENT_DATE(), -1) and w.project_code is not null and w.work_order_status != '�Ѳ���' and lower(w.project_code) not regexp 'test|tese'    
      GROUP BY w.project_code
    )tt
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
    ON s.d = DATE_ADD(CURRENT_DATE(), -1) and (s.project_code = tt.project_code or s.project_sale_code = tt.project_code)
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON s.project_code = h.project_code AND h.rn = 1
  )tmp
  WHERE tmp.project_sale_code is NULL OR tmp.rn = 1 OR tmp.external_project_code is not null 
  GROUP BY tmp.true_project_code
)t5
ON b.project_code = t5.true_project_code
-- ��Ŀ����
LEFT JOIN
(
  SELECT a.project_code,
         a.contract_amount
  FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
)t6
ON b.project_code = t6.project_code
-- ��Ӳ����Ŀ �豸����ȷ����̱�������
LEFT JOIN
(
  SELECT tmp.project_code,
         tmp.equitment_arrival_date,
         tmp.end_time_month
  FROM 
  (
    SELECT e.project_code,
           date(e.equitment_arrival_date) as equitment_arrival_date, -- �豸����ǩ������
           date_format(e.end_time,'yyyy-MM') as end_time_month, -- ���������������
           row_number()over(PARTITION by e.project_code order by e.start_time desc)rn
    FROM ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
    WHERE e.approve_status = 30 
  )tmp
  WHERE tmp.rn = 1
)t7
ON b.project_code = t7.project_code
WHERE t2.project_code is null -- ֻ��������Ŀ
) 

SELECT  t1.project_code_class, -- ��Ŀ����
        t1.month_scope, -- ͳ���·�
        IF(t2.no_final_inspection_num is null,0,t2.no_final_inspection_num)                       as month_begin_no_final_inspection_num, -- �³��ۼ�δ��������
        IF(t2.no_final_inspection_amount is null,0,t2.no_final_inspection_amount)                 as month_begin_no_final_inspection_amount, -- �³��ۼ�δ���ս��
        total.handover_num                                                                        as this_month_handover_num , -- ���½����������
        IF(total.amount is null,0,total.amount)                                                   as this_month_handover_amount, -- ���½�����ɽ��
        IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as this_month_final_inspection_num, -- ���������������
        IF(final_inspection.amount is null,0,final_inspection.amount)                             as this_month_final_inspection_amount, -- ����������ɽ��
        t1.no_final_inspection_num                                                                as month_end_no_final_inspection_num, -- ��ĩ�ۼ�δ��������
        t1.no_final_inspection_amount                                                             as month_end_no_final_inspection_amount -- ��ĩ�ۼ�δ���ս��
FROM 
-- ��ĩ
(
  SELECT a.project_code_class,
	     a.month_scope,
	     SUM(b.pre_num) as pre_num,
	     SUM(b.handover_num) as handover_num,
	     SUM(b.total_amount) as total_amount,
	     SUM(b.final_inspection_num) as final_inspection_num,
	     SUM(b.final_inspection_amount) as final_inspection_amount,
	     SUM(b.no_final_inspection_num) as no_final_inspection_num,
	     SUM(b.no_final_inspection_amount) as no_final_inspection_amount
  FROM 
  (
	SELECT total.project_code_class,
	       total.month_scope,
	       total.pre_num,
	       total.handover_num,
	       IF(total.amount is null,0,total.amount) as total_amount,
	       IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
	       IF(final_inspection.amount is null,0,final_inspection.amount) as final_inspection_amount,
	       IF(total.project_code_class IN ('A','C'),total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num),total.pre_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num)) as no_final_inspection_num,
	       IF(total.amount is null,0,total.amount) - IF(final_inspection.amount is null,0,final_inspection.amount) as no_final_inspection_amount
	FROM 
	(
	  SELECT b.project_code_class,
	         td.month_scope,
	         SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
	         SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
	         SUM(tmp1.amount) as amount
	  FROM 
	  (
	      SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
	      FROM ${dim_dbname}.dim_day_date
	      WHERE 1 = 1
	        AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
	  ) td
	  LEFT JOIN 
	  (
	    SELECT 'A' as project_code_class
	    union all
	    SELECT 'C' as project_code_class
	    union all
	    SELECT 'FH' as project_code_class
	  )b
	  LEFT JOIN
	  (
		SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp1
	  ON b.project_code_class = tmp1.project_code_class AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
	  GROUP BY b.project_code_class,td.month_scope
	)total
    LEFT JOIN
	(
	  SELECT tmp3.project_code_class,
	         tmp3.final_inspection_process_month,
	         SUM(case when tmp3.is_final_inspection = '������' then 1 else 0 end) as final_inspection_num,
	         SUM(tmp3.amount) as amount
	  FROM 
	  (
	    SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp3
	  WHERE tmp3.is_final_inspection = '������'
	  GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month
	)final_inspection
	ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month
  )a 
  LEFT JOIN 
  (
	SELECT total.project_code_class,
	       total.month_scope,
	       total.pre_num,
	       total.handover_num,
	       IF(total.amount is null,0,total.amount) as total_amount,
	       IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
	       IF(final_inspection.amount is null,0,final_inspection.amount) as final_inspection_amount,
	       IF(total.project_code_class IN ('A','C'),total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num),total.pre_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num)) as no_final_inspection_num,
	       IF(total.amount is null,0,total.amount) - IF(final_inspection.amount is null,0,final_inspection.amount) as no_final_inspection_amount
	FROM 
	(
	  SELECT b.project_code_class,
	         td.month_scope,
	         SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
	         SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
	         SUM(tmp1.amount) as amount
	  FROM 
	  (
	      SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
	      FROM ${dim_dbname}.dim_day_date
	      WHERE 1 = 1
	        AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
	  ) td
	  LEFT JOIN 
	  (
	    SELECT 'A' as project_code_class
	    union all
	    SELECT 'C' as project_code_class
	    union all
	    SELECT 'FH' as project_code_class
	  )b
	  LEFT JOIN
	  (
		SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp1
	  ON b.project_code_class = tmp1.project_code_class AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
	  GROUP BY b.project_code_class,td.month_scope
	)total
    LEFT JOIN
	(
	  SELECT tmp3.project_code_class,
	         tmp3.final_inspection_process_month,
	         SUM(case when tmp3.is_final_inspection = '������' then 1 else 0 end) as final_inspection_num,
	         SUM(tmp3.amount) as amount
	  FROM 
	  (
	    SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp3
	  WHERE tmp3.is_final_inspection = '������'
	  GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month
	)final_inspection
	ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month
  )a 
  ON a.project_code_class = b.project_code_class AND a.month_scope >= b.month_scope
  GROUP BY a.project_code_class,a.month_scope
)t1
-- �³�
LEFT JOIN 
(
  SELECT a.project_code_class,
	     a.month_scope,
	     SUM(b.pre_num) as pre_num,
	     SUM(b.handover_num) as handover_num,
	     SUM(b.total_amount) as total_amount,
	     SUM(b.final_inspection_num) as final_inspection_num,
	     SUM(b.final_inspection_amount) as final_inspection_amount,
	     SUM(b.no_final_inspection_num) as no_final_inspection_num,
	     SUM(b.no_final_inspection_amount) as no_final_inspection_amount
  FROM 
  (
	SELECT total.project_code_class,
	       total.month_scope,
	       total.pre_num,
	       total.handover_num,
	       IF(total.amount is null,0,total.amount) as total_amount,
	       IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
	       IF(final_inspection.amount is null,0,final_inspection.amount) as final_inspection_amount,
	       IF(total.project_code_class IN ('A','C'),total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num),total.pre_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num)) as no_final_inspection_num,
	       IF(total.amount is null,0,total.amount) - IF(final_inspection.amount is null,0,final_inspection.amount) as no_final_inspection_amount
	FROM 
	(
	  SELECT b.project_code_class,
	         td.month_scope,
	         SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
	         SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
	         SUM(tmp1.amount) as amount
	  FROM 
	  (
	      SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
	      FROM ${dim_dbname}.dim_day_date
	      WHERE 1 = 1
	        AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
	  ) td
	  LEFT JOIN 
	  (
	    SELECT 'A' as project_code_class
	    union all
	    SELECT 'C' as project_code_class
	    union all
	    SELECT 'FH' as project_code_class
	  )b
	  LEFT JOIN
	  (
		SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp1
	  ON b.project_code_class = tmp1.project_code_class AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
	  GROUP BY b.project_code_class,td.month_scope
	)total
    LEFT JOIN
	(
	  SELECT tmp3.project_code_class,
	         tmp3.final_inspection_process_month,
	         SUM(case when tmp3.is_final_inspection = '������' then 1 else 0 end) as final_inspection_num,
	         SUM(tmp3.amount) as amount
	  FROM 
	  (
	    SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp3
	  WHERE tmp3.is_final_inspection = '������'
	  GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month
	)final_inspection
	ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month
  )a 
  LEFT JOIN 
  (
	SELECT total.project_code_class,
	       total.month_scope,
	       total.pre_num,
	       total.handover_num,
	       IF(total.amount is null,0,total.amount) as total_amount,
	       IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num) as final_inspection_num,
	       IF(final_inspection.amount is null,0,final_inspection.amount) as final_inspection_amount,
	       IF(total.project_code_class IN ('A','C'),total.handover_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num),total.pre_num - IF(final_inspection.final_inspection_num is null,0,final_inspection.final_inspection_num)) as no_final_inspection_num,
	       IF(total.amount is null,0,total.amount) - IF(final_inspection.amount is null,0,final_inspection.amount) as no_final_inspection_amount
	FROM 
	(
	  SELECT b.project_code_class,
	         td.month_scope,
	         SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
	         SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
	         SUM(tmp1.amount) as amount
	  FROM 
	  (
	      SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
	      FROM ${dim_dbname}.dim_day_date
	      WHERE 1 = 1
	        AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
	  ) td
	  LEFT JOIN 
	  (
	    SELECT 'A' as project_code_class
	    union all
	    SELECT 'C' as project_code_class
	    union all
	    SELECT 'FH' as project_code_class
	  )b
	  LEFT JOIN
	  (
		SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp1
	  ON b.project_code_class = tmp1.project_code_class AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
	  GROUP BY b.project_code_class,td.month_scope
	)total
    LEFT JOIN
	(
	  SELECT tmp3.project_code_class,
	         tmp3.final_inspection_process_month,
	         SUM(case when tmp3.is_final_inspection = '������' then 1 else 0 end) as final_inspection_num,
	         SUM(tmp3.amount) as amount
	  FROM 
	  (
	    SELECT DISTINCT *
	    FROM project_view_detail d
	  )tmp3
	  WHERE tmp3.is_final_inspection = '������'
	  GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month
	)final_inspection
	ON total.project_code_class = final_inspection.project_code_class AND total.month_scope = final_inspection.final_inspection_process_month
  )a 
  ON a.project_code_class = b.project_code_class AND a.month_scope >= b.month_scope
  GROUP BY a.project_code_class,a.month_scope
)t2
ON t1.project_code_class = t2.project_code_class AND date_format(CONCAT(t2.month_scope,'-01'),'yyyy-MM-01') = add_months(date_format(CONCAT(t1.month_scope,'-01'),'yyyy-MM-01'),-1)
-- ���½���
LEFT JOIN 
(
  SELECT b.project_code_class,
	     td.month_scope,
	     SUM(case when tmp1.pre_project_approval_time is not null then 1 else 0 end) as pre_num,
	     SUM(case when tmp1.project_handover_end_time is not null then 1 else 0 end) as handover_num,
	     SUM(tmp1.amount) as amount
  FROM 
  (
    SELECT DISTINCT CONCAT(year_date,'-',LPAD(CAST(month_date as string),2,'0')) as month_scope
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1
      AND days >= '2018-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) td
  LEFT JOIN 
  (
    SELECT 'A' as project_code_class
	union all
	SELECT 'C' as project_code_class
	union all
	SELECT 'FH' as project_code_class
  )b
  LEFT JOIN
  (
	SELECT DISTINCT *
	    FROM project_view_detail d
  )tmp1
  ON b.project_code_class = tmp1.project_code_class AND IF(tmp1.project_code_class IN ('A','C'),td.month_scope = date_format(tmp1.project_handover_end_time,'yyyy-MM'),td.month_scope = date_format(tmp1.pre_project_approval_time,'yyyy-MM'))
  GROUP BY b.project_code_class,td.month_scope
)total
ON t1.project_code_class = total.project_code_class AND t1.month_scope = total.month_scope
-- ��������
LEFT JOIN
(
  SELECT tmp3.project_code_class,
	     tmp3.final_inspection_process_month,
	     SUM(case when tmp3.is_final_inspection = '������' then 1 else 0 end) as final_inspection_num,
	     SUM(tmp3.amount) as amount
  FROM 
  (
	SELECT DISTINCT *
	    FROM project_view_detail d
  )tmp3
  WHERE tmp3.is_final_inspection = '������'
  GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month
)final_inspection
ON t1.project_code_class = final_inspection.project_code_class AND t1.month_scope = final_inspection.final_inspection_process_month;