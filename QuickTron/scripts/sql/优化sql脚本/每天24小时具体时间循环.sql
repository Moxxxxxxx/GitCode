------------------------------------------------------ mysql 24小时循环 ------------------------------------------------------ 
SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(now(), '%Y-%m-%d 00:00:00'), INTERVAL (-(@u:=@u+1)) HOUR), '%Y-%m-%d %H:00:00') as ids
FROM
(SELECT a
FROM(SELECT '1' AS a UNION SELECT '2'UNION SELECT '3'UNION SELECT '4') AS a
JOIN(SELECT '1' UNION SELECT '2' UNION SELECT '3' UNION SELECT '4' UNION SELECT '5' UNION SELECT '6') AS b ON 1) AS b,
(SELECT @u:=-1 ) AS i

------------------------------------------------------- Hive 24小时循环 ------------------------------------------------------- 
SELECT from_unixtime(unix_timestamp(DATE_FORMAT(DATE_ADD(CURRENT_DATE(), -1),'yyyy-MM-dd 00:00:00')) + (tmp.rn-1)*3600) as ids
FROM
(SELECT ROW_NUMBER() over(order by num) rn
FROM(SELECT '1' AS num UNION SELECT '2'UNION SELECT '3'UNION SELECT '4') AS a
JOIN(SELECT '1' UNION SELECT '2' UNION SELECT '3' UNION SELECT '4' UNION SELECT '5' UNION SELECT '6') AS b 
) AS tmp