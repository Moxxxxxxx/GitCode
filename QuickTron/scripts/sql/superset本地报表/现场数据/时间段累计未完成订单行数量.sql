SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);

SELECT tt.theDayStartofhour,
       count(distinct case when pwd.created_date <= theDayEndofhour then pwd.picking_work_detail_id end) - count(distinct case when pwd.updated_date <= theDayEndofhour then pwd.picking_work_detail_id end) as value, -- T-1所处时间段累计未完成的订单行
       pwd.project_code as project_code
FROM  
(
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tt
join evo_wcs_g2p.picking_work_detail pwd
left join
(
 SELECT *
 FROM 
 (
  SELECT pwd.project_code,pwd.picking_work_detail_id,pwd.updated_date
  FROM evo_wcs_g2p.picking_work pw 
  left join evo_wcs_g2p.picking_work_detail pwd 
  on pwd.picking_work_id = pw.picking_work_id and pw.project_code = pwd.project_code
  where 1 = 1 -- and pw.pt in $project_code -- and pw.pt in ('A51118', 'C35052', 'A51264')
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pw.state in ('CANCEL_DONE', 'DONE') -- 完成或取消的
  UNION ALL    
  SELECT pwd.project_code,pwd.picking_work_detail_id,pwd.updated_date
  FROM evo_wcs_g2p.picking_work_detail pwd 
  left join evo_wcs_g2p.picking_job pj
  on pj.picking_work_detail_id = pwd.picking_work_detail_id and pj.project_code = pwd.project_code
  where 1 = 1 -- and t1.pt in $project_code -- and t1.pt in ('A51118', 'C35052', 'A51264') 
  and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY) and pwd.quantity = pwd.fulfill_quantity and pj.state = 'DONE'
 )t
 group by t.project_code,t.picking_work_detail_id
)t1
on t1.project_code = pwd.project_code and t1.picking_work_detail_id = pwd.picking_work_detail_id
where 1 = 1 -- and t.pt in $project_code -- and t.pt in ('A51118', 'C35052', 'A51264')
and pwd.updated_date >= @begin_time and pwd.updated_date < DATE_ADD(@begin_time,INTERVAL 1 DAY)
group by pwd.project_code,TIMESTAMPDIFF(HOUR,@begin_time,tt.theDayStartofhour)

