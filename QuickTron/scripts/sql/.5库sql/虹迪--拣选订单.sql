SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_group_num) as '集合单完成数', -- 集合单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	 SUM(tmp2.station_slot_times) as '命中槽位次数', -- 命中槽位次数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.win_open_times) as '弹窗次数', -- 弹窗次数
     cast(ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站弹窗次数', -- 单次进站弹窗次数
	 cast(ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
     cast(ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0)as decimal(10,2)) as '单次弹窗命中槽位次数', -- 单次弹窗命中槽位次数
	 cast(ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站完成订单行数' -- 单次进站完成订单行数
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
     0 order_num,
     0 order_group_num,
     0 order_linenum,
      sum(pj.actual_quantity) sku_num, -- picking_job实捡数量
      count(pj.station_slot_code) station_slot_times, -- 工作站槽位code
	 0 into_station_times, -- 进出站时间
	 count(distinct stg.group_job_id) win_open_times -- 任务组任务id
	FROM evo_wcs_g2p.station_task_group stg 
	JOIN evo_wcs_g2p.w2p_picking_job_v2 pj 
      ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and stg.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pj.project_code = 'C35052' AND stg.project_code = 'C35052'
	group BY DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT  DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT pw.order_id) order_num,
     COUNT(DISTINCT pw.picking_order_group_id) order_group_num,
     0 'order_linenum',
     0 'sku_num', 
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.w2p_picking_work_v2 pw
	WHERE pw.state = 'DONE' AND pw.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pw.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pw.project_code = 'C35052' 
group BY DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_group_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num',
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.w2p_picking_work_detail_v2 pwd
	JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE'  AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'C35052' AND pj.project_code = 'C35052' 
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_group_num',
     0 'order_linenum',
     0 'sku_num',
     0 'station_slot_times',
       count(se.id) into_station_times,
     0 'win_open_times'
    FROM evo_station.station_entry se
	WHERE biz_type = 'PICKING_ONLINE_G2P_W2P' and entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'C35052' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')
	  
UNION ALL

SELECT 
     tmp_line.ida times,
     0 'order_num',
     0 'order_group_num',
     0 'order_linenum',
	 0 'sku_num',
	 0 'station_slot_times',
     0 'into_station_times', 
     0 'win_open_times'
	FROM (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    GROUP BY tmp_line.ida
) tmp2 
GROUP BY tmp2.times