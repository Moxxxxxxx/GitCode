SET @begin_time = '2021-11-16 07:00:00';
SET @end_time = '2021-11-17 07:00:00';

SELECT DATE(tmp2.times) as '日期',
       SUM(tmp2.sku_num) as '出库总件数',
       CAST(SUM(tmp2.order_num)/COUNT(DISTINCT DATE(tmp2.times)) AS DECIMAL(10,0)) as '平均每日拣选单数',
       CAST(SUM(tmp2.sku_num)/COUNT(DISTINCT DATE(tmp2.times)) AS DECIMAL(10,0)) as '平均每日出库件数',
       CAST(SUM(tmp2.order_linenum)/COUNT(DISTINCT DATE(tmp2.times)) AS DECIMAL(10,0)) as '平均每日出库行数'  
     
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     sum(pj.actual_quantity) sku_num -- picking_job实捡数量
	FROM evo_wcs_g2p.station_task_group stg 
	LEFT JOIN evo_wcs_g2p.picking_job pj 
  ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= @begin_time and stg.updated_date < @end_time AND pj.project_code = 'A51118' AND stg.project_code = 'A51118'
	group BY DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT pw.order_id)order_num,-- picking_work行数
     0 'order_linenum',
     0 'sku_num'
FROM evo_wcs_g2p.picking_work pw
WHERE pw.state = 'DONE' AND pw.updated_date >= @begin_time and pw.updated_date < @end_time AND pw.project_code = 'A51118'
group BY DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num'
    FROM evo_wcs_g2p.picking_work_detail pwd
	  LEFT JOIN evo_wcs_g2p.picking_job pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE' AND pwd.updated_date >= @begin_time and pwd.updated_date < @end_time AND pwd.project_code = 'A51118' AND pj.project_code = 'A51118'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00')
)tmp2 
GROUP BY DATE(tmp2.times)