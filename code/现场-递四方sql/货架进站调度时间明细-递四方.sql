-- ���ܽ�վ����ʱ����ϸ
SET @begin_time = '2021-11-16 00:00:00'; -- ���ÿ�ʼʱ��
SET @end_time = '2021-11-17 00:00:00'; -- ���ý���ʱ��

SELECT 
po.picking_order_number as '��ѡ������',
pj.job_id as '�����',
pj.bucket_code as '���ܱ��',
'��վ' as '��������',
c1.updated_date as '��ʼʱ��',
c2.updated_date as '����ʱ��',
TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date) as 'ʱ��/s'
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wcs_g2p.picking_job pj
ON po.id = pj.order_id
LEFT JOIN evo_wcs_g2p.job_state_change c1
ON pj.job_id = c1.job_id
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id
WHERE po.state = 'DONE' AND c1.state = 'INIT_JOB' AND c2.state = 'START_EXECUTOR' AND po.done_date >= @begin_time AND po.done_date < @end_time