-- -------------------�������ݣ�ÿ�ճ��ⵥ��,ÿ�ճ�������,ÿ�ճ������--------------------- --
-- -------------------��ѡ���ṹ��ÿ�ճ��������,ÿ�ճ�����б�--------------------- --
-- -------------------�鲨���ԣ�ÿ�ռ�ѡ������--------------------- --
INSERT INTO qt_dsf.picking_date_detail(date,picking_order_num,picking_order_linenum,picking_quanlity,quanlity_num_once_rate,quanlity_linenum_once_rate,picking_job_num,picking_bucket_num,picking_bucketface_num)

SELECT DATE(pw.updated_date) as '����',COUNT(DISTINCT pw.order_id) as 'ÿ�ճ��ⵥ��',COUNT(DISTINCT pwd.id) as 'ÿ�ճ�������',SUM(pwd.fulfill_quantity) as 'ÿ�ճ������',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as 'ÿ�ճ��������',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pwd.id) AS decimal(10,2)) as 'ÿ�ճ�����б�',
       COUNT(DISTINCT pj.job_id) as 'ÿ�ռ�ѡ������',COUNT(pj.bucket_code) as 'ÿ�ռ�ѡ����',COUNT(pj.bucket_face_num) as 'ÿ�ռ�ѡ������'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE(pw.updated_date)


-- -------------------�������ݣ��³���������,��ƽ��ÿ�ռ�ѡ����,��ƽ��ÿ�ճ�������,��ƽ��ÿ�ճ������,�³����ܵ���,�³���������,�³����ܼ���--------------------- --
-- -------------------��ѡ���ṹ����ƽ��������,��ƽ���е���,��ƽ�����б�,�µ�Ʒ������ѡ��ռ��,�µ�Ʒ�����ѡ��ռ��,�¶�Ʒ�����ѡ��ռ��--------------------- --
-- -------------------SKU���ݣ�ƽ��ÿ�����SKU��,�����嶯��SKU��--------------------- --
INSERT INTO qt_dsf.picking_month_detail(date,picking_rise_rate,picking_order_avg,picking_order_linenum_avg,picking_quanlity_avg,picking_order_month,picking_order_linenum_month,picking_quanlity_month,quanlity_order_avg,order_linenum_avg,quanlity_linenum_avg ,single_sku_quanlity_rate,single_sku_quanlities_rate,multiple_sku_quanlities_rate,picking_sku_quanlity_avg,picking_sku_num)

SELECT tmp1.`�·�`,
       CAST((tmp1.`ÿ�³������`- tmp2.`ÿ�³������`)/tmp2.`ÿ�³������` AS decimal(10,2)) as '�³���������',
       tmp3.`��ƽ��ÿ�ռ�ѡ����`,tmp3.`��ƽ��ÿ�ճ�������`,tmp3.`��ƽ��ÿ�ճ������`,tmp3.`�³����ܵ���`,tmp3.`�³���������`,tmp3.`�³����ܼ���`,
       tmp3.`��ƽ��������`,tmp3.`��ƽ���е���`,tmp3.`��ƽ�����б�`,
       tmp4.`�µ�Ʒ������ѡ��ռ��`,tmp4.`�µ�Ʒ�����ѡ��ռ��`,tmp4.`�¶�Ʒ�����ѡ��ռ��`,
       tmp3.`ƽ��ÿ�����SKU��`,tmp3.`�����嶯��SKU��`
FROM
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '�·�',SUM(pwd.fulfill_quantity) as 'ÿ�³������'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp1
LEFT JOIN
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '�·�',SUM(pwd.fulfill_quantity) as 'ÿ�³������'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp2
ON TIMESTAMPDIFF(MONTH,tmp2.`�·�`,tmp1.`�·�`) = 1
LEFT JOIN
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '�·�',
       CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '��ƽ��ÿ�ռ�ѡ����',
       CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '��ƽ��ÿ�ճ�������',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '��ƽ��ÿ�ճ������',
       COUNT(DISTINCT pw.order_id) as '�³����ܵ���',
       COUNT(DISTINCT pwd.id) as '�³���������',
       SUM(pwd.fulfill_quantity) as '�³����ܼ���',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as '��ƽ��������',
       CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as '��ƽ���е���',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pwd.id) AS decimal(10,2)) as '��ƽ�����б�',
       CAST(COUNT(DISTINCT pwd.sku_id)/COUNT(DISTINCT DATE(pwd.updated_date)) AS decimal(10,2)) as 'ƽ��ÿ�����SKU��',
       COUNT(DISTINCT pwd.sku_id)as '�����嶯��SKU��'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id 
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp3
ON tmp1.`�·�` = tmp3.`�·�`
LEFT JOIN
(
SELECT t2.`�·�`,
       CAST(SUM(t2.`�µ�Ʒ������ѡ��`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '�µ�Ʒ������ѡ��ռ��',
       CAST(SUM(t2.`�µ�Ʒ�����ѡ��`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '�µ�Ʒ�����ѡ��ռ��',
       CAST(SUM(t2.`�¶�Ʒ�����ѡ��`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '�¶�Ʒ�����ѡ��ռ��'
FROM
(
SELECT t1.`�·�`,t1.order_id,
       IF(SUM(t1.`SKU����`)=1 AND SUM(t1.`SKU�������`)=1,COUNT(DISTINCT t1.order_id),0) as '�µ�Ʒ������ѡ��',
       IF(SUM(t1.`SKU����`)=1 AND SUM(t1.`SKU�������`)>1,COUNT(DISTINCT t1.order_id),0) as '�µ�Ʒ�����ѡ��',
       IF(SUM(t1.`SKU����`)>1 AND SUM(t1.`SKU�������`)>1,COUNT(DISTINCT t1.order_id),0) as '�¶�Ʒ�����ѡ��'
FROM
(
SELECT pw.order_id,pwd.sku_id,COUNT(pwd.sku_id) as 'SKU����',SUM(pwd.fulfill_quantity) as 'SKU�������',DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '�·�'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY pw.order_id,pwd.sku_id
)t1
GROUP BY t1.`�·�`,t1.order_id
)t2
GROUP BY t2.`�·�`
)tmp4
ON tmp1.`�·�` = tmp4.`�·�`


-- -------------------SKU���ݣ���SKU�³�����--------------------- --
SELECT DATE_ADD(DATE(pwd.updated_date),INTERVAL -day(DATE(pwd.updated_date))+1 day) as '�·�',
       pwd.sku_id as 'SKU',SUM(pwd.fulfill_quantity) as '��SKU�³�����'
FROM evo_wcs_g2p.picking_work_detail pwd
WHERE pwd.quantity = pwd.fulfill_quantity AND pwd.updated_date >= '2021-09-22 00:00:00' and pwd.updated_date < @end_time
GROUP BY pwd.sku_id,DATE_ADD(DATE(pwd.updated_date),INTERVAL -day(DATE(pwd.updated_date))+1 day)


-- -------------------������ʹ����\��ѡЧ�ʣ�ÿ�ջ�����ʹ����,ÿ�ջ����˷�ֵʹ����--------------------- --
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d') as '����',
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/86400 AS DECIMAL(10,2)) as 'ÿ�ջ�����ʹ����', -- ��һ��24Сʱ����
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/36000 AS DECIMAL(10,2)) as 'ÿ�ջ����˷�ֵʹ����' -- ��һ��10Сʱ����
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'INIT_JOB' AND c.job_type = 'G2P_BUCKET_MOVE'  -- С��ȡ����
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'DONE' AND c.job_type = 'G2P_BUCKET_MOVE' -- ��ѡ�������
)t2
ON t1.job_id =t2.job_id
WHERE t2.updated_date is not NULL AND DATE(t1.updated_date) = DATE(t2.updated_date)
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')
ORDER BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')


-- -------------------������ʹ���ʣ����ջ����˷�ֵʹ����--------------------- --
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '����',
       SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/3600 as '���ջ�����ʹ����'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'INIT_JOB' AND c.job_type = 'G2P_BUCKET_MOVE'  -- С��ȡ����
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'DONE' AND c.job_type = 'G2P_BUCKET_MOVE' -- ��ѡ�������
)t2
ON t1.job_id =t2.job_id
WHERE t2.updated_date is not NULL AND DATE(t1.updated_date) = DATE(t2.updated_date) AND DATE(t1.updated_date) = '2021-09-22'
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
ORDER BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00');


-- -------------------������ʹ���ʣ�������״̬����--------------------- --
SELECT tmp.`����`,COUNT(DISTINCT tmp.agv_code) as '�����˿���̨��',SUM(tmp.`��һ����ռ����`)as '��һ����ռ����',SUM(tmp.`�ȴ�ռ�ý���ȡ����`)as '�ȴ�ռ�ý���ȡ����',SUM(tmp.`�ӵ�����ȡ����`)as '�ӵ�����ȡ����',SUM(tmp.`�ͻ��ܵ�����վ`) as '�ͻ��ܵ�����վ',SUM(tmp.`����վ�ȴ���ʵ��`) as '����վ�ȴ���ʵ��',SUM(tmp.`�˹���ѡͣ��`) as '�˹���ѡͣ��',SUM(tmp.`״̬�ع�`) as '״̬�ع�',SUM(tmp.`������`) as '������'
FROM
(
SELECT t3.agv_code,DATE_FORMAT(t3.updated_date,'%Y-%m-%d %H:00:00') as '����',
       IF((t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'INIT' AND t3.state1 = 'INIT_JOB'),1,0) as '��һ����ռ����',
       IF((t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'WAITING_AGV' AND t3.state1 = 'GO_TARGET'),1,0) as '�ȴ�ռ�ý���ȡ����',
       IF((t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'INIT_JOB' AND t3.state1 = 'GO_TARGET') OR (t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'INIT_JOB' AND t3.state1 = 'GO_TARGET'),1,0) as '�ӵ�����ȡ����',
       IF( (t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'GO_TARGET' AND t3.state1 = 'WAITING_EXECUTOR'),1,0) as '�ͻ��ܵ�����վ',
       IF(t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'WAITING_EXECUTOR' AND t3.state1 = 'START_EXECUTOR',1,0) as '����վ�ȴ���ʵ��',
       IF(t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'GO_TARGET' AND t3.state1 = 'DONE',1,0) as '������',
       IF(t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'START_EXECUTOR' AND t3.state1 = 'DONE',1,0) as '�˹���ѡͣ��',
       IF(t3.state1 = 'ROLLBACK',1,0) as '״̬�ع�'
FROM
(
SELECT t1.*,t2.*
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE (c.job_type = 'G2P_ONLINE_PICK' OR c.job_type = 'G2P_BUCKET_MOVE')
)t1
LEFT JOIN
(
SELECT c.agv_code as agv_code1,c.job_id as job_id1,c.updated_date as updated_date1,c.state as state1,c.job_type job_type1
FROM evo_wcs_g2p.job_state_change c
WHERE  (c.job_type = 'G2P_ONLINE_PICK' OR c.job_type = 'G2P_BUCKET_MOVE')
)t2
ON t1.job_id = t2.job_id1 AND t1.updated_date < t2.updated_date1
WHERE t2.updated_date1 is not NULL AND t1.agv_code is not NULL AND t1.agv_code != '' 
ORDER BY t1.agv_code,t1.updated_date DESC
LIMIT 10000000
)t3
GROUP BY t3.agv_code,DATE_FORMAT(t3.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.`����`



-- -------------------����������ʱ�䣺ÿ��ÿСʱ��ƽ��ȡ��ʱ��\ÿ��ÿСʱ���ͻ�ʱ��\ÿ��ÿСʱ�ļ�ѡͣ��ʱ��\ÿ��ÿСʱ�Ļ�����ʱ��--------------------- --
SELECT t1.`����`,IFNULL(t1.`ÿ��ÿСʱ��ƽ��ȡ��ʱ��/s`,0) as 'ÿ��ÿСʱ��ƽ��ȡ��ʱ��/s',IFNULL(t2.`ÿ��ÿСʱ���ͻ�ʱ��/s`,0) as 'ÿ��ÿСʱ���ͻ�ʱ��/s',IFNULL(t3.`ÿ��ÿСʱ�ļ�ѡͣ��ʱ��/s`,0) as 'ÿ��ÿСʱ�ļ�ѡͣ��ʱ��/s',IFNULL(t4.`ÿ��ÿСʱ�Ļ�����ʱ��/s`,0) as 'ÿ��ÿСʱ�Ļ�����ʱ��/s'
FROM
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '����',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,IF(DATE_FORMAT(t2.updated_date,'%Y-%m-%d %H:00:00')>DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(t1.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),t2.updated_date)))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as 'ÿ��ÿСʱ��ƽ��ȡ��ʱ��/s'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'INIT_JOB' 
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' 
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t1
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '����',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,IF(DATE_FORMAT(t2.updated_date,'%Y-%m-%d %H:00:00')>DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(t1.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),t2.updated_date)))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as 'ÿ��ÿСʱ���ͻ�ʱ��/s'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' 
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE' 
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t2
ON t1.`����` = t2.`����`
LEFT JOIN
(
SELECT DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') as '����',
      CAST(SUM(TIMESTAMPDIFF(SECOND,tmp.updated_date,IF(DATE_FORMAT(tmp.updated_date1,'%Y-%m-%d %H:00:00')>DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(tmp.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),tmp.updated_date1)))/COUNT(DISTINCT tmp.agv_code) AS DECIMAL(10,2)) as 'ÿ��ÿСʱ�ļ�ѡͣ��ʱ��/s'
FROM
(
SELECT t1.agv_code,t1.job_id,t1.updated_date,t1.state,t1.job_type,t2.agv_code1,t2.job_id1,t2.updated_date1,t2.state1,t2.job_type1
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_ONLINE_PICK' AND c.state = 'START_EXECUTOR' 
)t1
LEFT JOIN
(
SELECT c.agv_code as agv_code1,c.job_id as job_id1,c.updated_date as updated_date1,c.state as state1,c.job_type as job_type1
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_ONLINE_PICK' AND c.state = 'DONE' 
)t2
ON t1.job_id = t2.job_id1
WHERE t2.job_id1 is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:%i:00')
)tmp
GROUP BY DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)t3
ON t1.`����` = t3.`����`
LEFT JOIN
(
SELECT DATE_FORMAT(bmj1.updated_date,'%Y-%m-%d %H:00:00') as '����',
       TIMESTAMPDIFF(SECOND,bmj1.created_date,bmj1.updated_date) as 'ÿ��ÿСʱ�Ļ�����ʱ��/s'
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON pj.bucket_move_job_id = bmj.id
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj1
ON bmj.busi_group_id = bmj1.busi_group_id
WHERE pj.state = 'DONE' AND bmj1.bucket_move_type = 'RESET_JOB'
GROUP BY DATE_FORMAT(bmj.updated_date,'%Y-%m-%d %H:00:00')
)t4
ON t1.`����` = t4.`����`


-- -------------------��ѡЧ�ʣ�ÿ��ƽ�����ܼ��ʱ��--------------------- --
SELECT DATE_FORMAT(b.entry_time,'%Y-%m-%d') as '����',CAST(SUM(b.bucket_wait_time)/COUNT(b.order_id) AS DECIMAL(10,2)) as 'ÿ��ƽ�����ܼ��ʱ��'
FROM
(
SELECT a.order_id,a.entry_time,a.exit_time,a.entry_time1,a.exit_time1,MIN(a.bucket_wait_time) as bucket_wait_time
FROM
(
SELECT tmp1.order_id,tmp1.entry_time,tmp1.exit_time,tmp2.entry_time as entry_time1,tmp2.exit_time as exit_time1,TIMESTAMPDIFF(SECOND,tmp1.exit_time,tmp2.entry_time) as bucket_wait_time
FROM
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp1 
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp2
ON tmp1.order_id = tmp2.order_id AND tmp1.entry_time < tmp2.entry_time
WHERE tmp2.job_id is not NULL
ORDER BY tmp1.order_id,tmp1.entry_time
)a
GROUP BY a.entry_time
)b
GROUP BY DATE_FORMAT(b.entry_time,'%Y-%m-%d')


-- -------------------��ѡЧ�ʣ����˹���ʱ��/�ȴ����ܵ�վ/���ܼ��ʱ��--------------------- --
SELECT t1.`����`,IFNULL(t1.`������ҵʱ��`,0) as '������ҵʱ��',IFNULL(t2.`�ȴ����ܵ�վ`,0) as '�ȴ����ܵ�վ',IFNULL(t3.`���ܼ��ʱ��`,0) as '���ܼ��ʱ��'
FROM
(
SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d') as '����',CAST(SUM(TIMESTAMPDIFF(SECOND,se.entry_time,se.exit_time))/COUNT(DISTINCT se.station_code) AS DECIMAL(10,2)) as '������ҵʱ��'
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON pj.job_id = se.idempotent_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d')
)t1
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d') as '����',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as '�ȴ����ܵ�վ'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET'
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE'
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')
)t2
ON t1.`����` = t2.`����`
LEFT JOIN
(
SELECT DATE_FORMAT(b.entry_time,'%Y-%m-%d') as '����',CAST(SUM(b.bucket_wait_time)/COUNT(b.order_id) AS DECIMAL(10,2)) as '���ܼ��ʱ��'
FROM
(
SELECT a.order_id,a.entry_time,a.exit_time,a.entry_time1,a.exit_time1,MIN(a.bucket_wait_time) as bucket_wait_time
FROM
(
SELECT tmp1.order_id,tmp1.entry_time,tmp1.exit_time,tmp2.entry_time as entry_time1,tmp2.exit_time as exit_time1,TIMESTAMPDIFF(SECOND,tmp1.exit_time,tmp2.entry_time) as bucket_wait_time
FROM
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp1 
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp2
ON tmp1.order_id = tmp2.order_id AND tmp1.entry_time < tmp2.entry_time
WHERE tmp2.job_id is not NULL
ORDER BY tmp1.order_id,tmp1.entry_time
)a
GROUP BY a.entry_time
)b
GROUP BY DATE_FORMAT(b.entry_time,'%Y-%m-%d')
)t3
ON t1.`����` = t3.`����`


-- -------------------��ѡЧ�ʣ��¾���ѡЧ��(��/��λ/h)(��/��λ/h)--------------------- --
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '�·�',CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT pj.station_code)/10 AS DECIMAL(10,2)) as '�¾���ѡЧ��(��/��λ/h)',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pj.station_code)/10 AS DECIMAL(10,2)) as '�¾���ѡЧ��(��/��λ/h)'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)


-- -------------------��ѡЧ�ʣ�ÿ�ռ�ѡЧ��(��/��/h)(��/��/h)(��/��/h)--------------------- --
SELECT DATE(pw.updated_date) as '����',CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as 'ÿ�ռ�ѡЧ��(��/��/h)',CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as 'ÿ�ռ�ѡЧ��(��/��/h)',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as 'ÿ�ռ�ѡЧ��(��/��/h)'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
LEFT JOIN evo_wes_picking.picking_order_fulfill_detail pofd
ON pj.id = pofd.job_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE(pw.updated_date)


-- -------------------��ѡЧ�ʣ�����վ״̬ʱ��ͼ������--------------------- --
SELECT t1.`ʱ���`,t1.`����վ����`,t1.`������`,(1 - t1.`������`) as '������',t2.`������`,IFNULL(t3.`�ȴ����ܵ�վ`,0) as '�ȴ����ܵ�վ'
FROM
(
SELECT 
    seq.station_code AS '����վ����',
		tmp.theDayStartofhour as 'ʱ���',
    cast((60*60-
	  sum(CASE WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,seq.exit_time)
             WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,tmp.theDayEndofhour)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time >= tmp.theDayStartofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,seq.exit_time)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,tmp.theDayEndofhour)
             ELSE 0 END))/(60*60)as decimal(10,2)) as '������'
FROM (
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD('2021-09-22 00:00:00',INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD('2021-09-22 00:00:00',INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tmp
join evo_station.station_entry seq
WHERE seq.entry_time >= '2021-09-22 00:00:00'  AND seq.exit_time < DATE_ADD('2021-09-22 00:00:00' ,INTERVAL 60*24 MINUTE) AND idempotent_id LIKE '%G2PPicking%' 
GROUP BY seq.station_code,tmp.theDayStartofhour
)t1
LEFT JOIN
(
SELECT 
   tt1.ida as 'ʱ���',
	 tt1.station_code AS '����վ����',
     cast(SUM(		
			CASE WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time then tt1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time then tt1.begin_to_lineBegin_time
				  ELSE 0 END
			) /3600 as decimal(10,2)) as '������'
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',if(DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-22 00:00:00','2021-09-22 00:00:00',DATE_ADD(sl.login_time,INTERVAL -3 HOUR))) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',if(sl.logout_time is null,'2021-09-23 00:00:00',if(sl.logout_time <= '2021-09-23 00:00:00',sl.logout_time,'2021-09-23 00:00:00'))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD('2021-09-22 00:00:00',INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD('2021-09-22 00:00:00',INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((DATE_ADD(sl.login_time,INTERVAL -3 HOUR) >= '2021-09-22 00:00:00' and DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-23 00:00:00')
       OR (sl.logout_time >= '2021-09-22 00:00:00' and sl.logout_time <= '2021-09-23 00:00:00')
		   OR (DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-22 00:00:00' and sl.logout_time >= '2021-09-23 00:00:00'))
       AND sl.biz_type = 'PICKING_ONLINE_G2P_B2P'
        ) tt1
    GROUP BY tt1.ida,tt1.station_code
)t2
ON t1.`ʱ���` = t2.`ʱ���` AND t1.`����վ����` = t2.`����վ����` 
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as 'ʱ���',
       t1.station_code as '����վ����',
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/3600 AS DECIMAL(10,2)) as '�ȴ����ܵ�վ'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type,bmj.station_code
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON c.job_id = bmj.job_id
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' AND DATE_FORMAT(c.updated_date,'%Y-%m-%d') = '2021-09-22' AND bmj.bucket_move_type = 'G2P_ONLINE_PICK'
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type,bmj.station_code
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON c.job_id = bmj.job_id
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE' AND DATE_FORMAT(c.updated_date,'%Y-%m-%d') = '2021-09-22' AND bmj.bucket_move_type = 'G2P_ONLINE_PICK'
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY t1.station_code,DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t3
ON t1.`ʱ���` = t3.`ʱ���` AND t1.`����վ����` = t3.`����վ����` 


-- -------------------SKU���ݣ��¿��SKU��ֵ����--------------------- --
SELECT DATE_ADD(DATE(pj.updated_date),INTERVAL -day(DATE(pj.updated_date))+1 day) as '�·�',COUNT(DISTINCT pj.sku_id) as '�¿��SKU��ֵ����'
FROM evo_wcs_g2p.picking_job pj
WHERE pj.state = 'DONE'
GROUP BY DATE_ADD(DATE(pj.updated_date),INTERVAL -day(DATE(pj.updated_date))+1 day)


-- -------------------SKU���ݣ���SKU������--------------------- --
SELECT DATE(MAX(last_updated_date)) as '����',li.sku_id as 'sku',SUM(li.quantity) as '�������'
FROM evo_wes_inventory.level3_inventory li
GROUP BY li.sku_id

-- -------------------�����ת����SKU��ת����--------------------- --
SELECT t1.sku,t1.`�ڳ�ʱ��`,li.last_updated_date as '��ĩʱ��',t1.`�ڳ��������`,li.quantity as '��ĩ�������',t1.`ʱ�������`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as 'ʱ���������',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`ʱ�������`/2*(t1.`�ڳ��������`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as 'ʱ��ο����ת����'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '�ڳ�ʱ��',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as 'ʱ�������',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '�ڳ��������'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`�ڳ�ʱ��` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `ʱ��ο����ת����`


-- -------------------�����ת����ͬ��ת����SKU������--------------------- --
SELECT tt.`��ת��������`,
       COUNT(DISTINCT tt.sku) as 'sku������',
       COUNT(DISTINCT tt.sku)/
(
SELECT COUNT(b.sku)
FROM 
(
SELECT CASE WHEN tmp.`ʱ��ο����ת����` >= 0 AND tmp.`ʱ��ο����ת����` < 30 THEN '0-30'
            WHEN tmp.`ʱ��ο����ת����` >= 30 AND tmp.`ʱ��ο����ת����` < 60 THEN '30-60'
            WHEN tmp.`ʱ��ο����ת����` >= 60 AND tmp.`ʱ��ο����ת����` < 90 THEN '60-90'
            WHEN tmp.`ʱ��ο����ת����` >= 90 AND tmp.`ʱ��ο����ת����` < 150 THEN '90-150'
            WHEN tmp.`ʱ��ο����ת����` >= 150 AND tmp.`ʱ��ο����ת����` < 300 THEN '150-300'
            WHEN tmp.`ʱ��ο����ת����` >= 300 AND tmp.`ʱ��ο����ת����` < 600 THEN '300-600'
            WHEN tmp.`ʱ��ο����ת����` >= 600  THEN '>600' END AS '��ת��������',tmp.*
FROM
(
SELECT t1.sku,t1.`�ڳ�ʱ��`,li.last_updated_date as '��ĩʱ��',t1.`�ڳ��������`,li.quantity as '��ĩ�������',t1.`ʱ�������`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as 'ʱ���������',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`ʱ�������`/2*(t1.`�ڳ��������`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as 'ʱ��ο����ת����'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '�ڳ�ʱ��',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as 'ʱ�������',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '�ڳ��������'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`�ڳ�ʱ��` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `ʱ��ο����ת����`
)tmp
)b
) as 'sku������ռ��',
SUM(tt.`��ĩ�������`) as '������',
SUM(tt.`��ĩ�������`)/
(
SELECT SUM(b.`��ĩ�������`)
FROM 
(
SELECT CASE WHEN tmp.`ʱ��ο����ת����` >= 0 AND tmp.`ʱ��ο����ת����` < 30 THEN '0-30'
            WHEN tmp.`ʱ��ο����ת����` >= 30 AND tmp.`ʱ��ο����ת����` < 60 THEN '30-60'
            WHEN tmp.`ʱ��ο����ת����` >= 60 AND tmp.`ʱ��ο����ת����` < 90 THEN '60-90'
            WHEN tmp.`ʱ��ο����ת����` >= 90 AND tmp.`ʱ��ο����ת����` < 150 THEN '90-150'
            WHEN tmp.`ʱ��ο����ת����` >= 150 AND tmp.`ʱ��ο����ת����` < 300 THEN '150-300'
            WHEN tmp.`ʱ��ο����ת����` >= 300 AND tmp.`ʱ��ο����ת����` < 600 THEN '300-600'
            WHEN tmp.`ʱ��ο����ת����` >= 600  THEN '>600' END AS '��ת��������',tmp.*
FROM
(
SELECT t1.sku,t1.`�ڳ�ʱ��`,li.last_updated_date as '��ĩʱ��',t1.`�ڳ��������`,li.quantity as '��ĩ�������',t1.`ʱ�������`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as 'ʱ���������',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`ʱ�������`/2*(t1.`�ڳ��������`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as 'ʱ��ο����ת����'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '�ڳ�ʱ��',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as 'ʱ�������',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '�ڳ��������'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`�ڳ�ʱ��` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `ʱ��ο����ת����`
)tmp
)b
) as '������ռ��'
FROM
(
SELECT CASE WHEN tmp.`ʱ��ο����ת����` >= 0 AND tmp.`ʱ��ο����ת����` < 30 THEN '0-30'
            WHEN tmp.`ʱ��ο����ת����` >= 30 AND tmp.`ʱ��ο����ת����` < 60 THEN '30-60'
            WHEN tmp.`ʱ��ο����ת����` >= 60 AND tmp.`ʱ��ο����ת����` < 90 THEN '60-90'
            WHEN tmp.`ʱ��ο����ת����` >= 90 AND tmp.`ʱ��ο����ת����` < 150 THEN '90-150'
            WHEN tmp.`ʱ��ο����ת����` >= 150 AND tmp.`ʱ��ο����ת����` < 300 THEN '150-300'
            WHEN tmp.`ʱ��ο����ת����` >= 300 AND tmp.`ʱ��ο����ת����` < 600 THEN '300-600'
            WHEN tmp.`ʱ��ο����ת����` >= 600  THEN '>600' END AS '��ת��������',tmp.*
FROM
(
SELECT t1.sku,t1.`�ڳ�ʱ��`,li.last_updated_date as '��ĩʱ��',t1.`�ڳ��������`,li.quantity as '��ĩ�������',t1.`ʱ�������`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as 'ʱ���������',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`ʱ�������`/2*(t1.`�ڳ��������`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as 'ʱ��ο����ת����'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '�ڳ�ʱ��',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as 'ʱ�������',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '�ڳ��������'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`�ڳ�ʱ��` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `ʱ��ο����ת����`
)tmp
)tt
GROUP BY tt.`��ת��������`
