-- ��������Աͳ�� ads_team_ft_virtual_member_count_info 

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_count_info
SELECT '' as id,
       DATE_ADD(CURRENT_DATE(), -1) as cur_date, -- ͳ������
       i.virtual_org_name, -- ����������
       SUM(if(i.role_type = '��Ʒ',1,0)) as po_qty, --��Ʒ����
       SUM(if(i.role_type = 'UED',1,0)) as ued_qty, -- UED����
       SUM(if(i.role_type = '�з�',1,0)) as dev_qty, -- �з�����
       SUM(if(i.role_type = '����',1,0)) as test_qty, -- ��������
       COUNT(DISTINCT i.emp_code) as total_qty, --������
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i 
WHERE i.is_active = 1 
GROUP BY i.virtual_org_name,DATE_ADD(CURRENT_DATE(), -1)