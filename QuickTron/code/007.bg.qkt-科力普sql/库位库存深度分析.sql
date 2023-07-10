SELECT tt.`项目`,
       COUNT(DISTINCT tt.bucket_slot_code) as '货位数',
       CONCAT(CAST((COUNT(DISTINCT tt.bucket_slot_code)/(SELECT COUNT(DISTINCT li.bucket_slot_code)FROM evo_wes_inventory.level3_inventory li WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' AND li.project_code = 'A51118' AND li.quantity > 0)*100) as DECIMAL(10,2)),'%') as '货位数占比'
FROM
(
SELECT CASE WHEN SUM(li.quantity) = 1 THEN '其中1件一货位的货位数占比'
            WHEN SUM(li.quantity) = 2 THEN '其中2件一货位的货位数占比'
            WHEN SUM(li.quantity) > 2 THEN '其中>2件一货位的货位数占比' END AS '项目',
       li.bucket_slot_code
FROM evo_basic.basic_bucket bb
LEFT JOIN evo_wes_inventory.level3_inventory li
ON bb.bucket_code = li.bucket_code 
WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' 
  AND li.project_code = 'A51118' AND bb.bucket_type_id = '57' AND li.quantity > 0
GROUP BY li.bucket_slot_code
)tt
GROUP BY tt.`项目` 
ORDER BY tt.`项目`