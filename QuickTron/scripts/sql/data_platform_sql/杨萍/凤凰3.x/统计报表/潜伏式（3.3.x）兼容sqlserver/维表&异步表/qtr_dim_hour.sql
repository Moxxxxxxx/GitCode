-- part1：mysql逻辑

select 
sysdate() as create_time,
sysdate() as update_time,
substr(DATE_add(DATE_FORMAT(now(), '%Y-%m-%d 00:00:00'),INTERVAL tq.seq_list-1 hour),11,9) as hour_start_time,
substr(DATE_add(DATE_FORMAT(now(), '%Y-%m-%d 00:00:00'),INTERVAL tq.seq_list hour),11,9) as next_hour_start_time
from 
(select 
@num:=@num+1 as seq_list
from 
(SELECT seq_list FROM (SELECT '1' AS seq_list UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
JOIN(SELECT '1' UNION SELECT '2' UNION SELECT '3' UNION SELECT '4' UNION SELECT '5' UNION SELECT '6') AS b ON 1)t,(SELECT @num := 0) as i)tq


-- part2：sqlserver逻辑

select 
sysdatetime() as create_time,
sysdatetime() as update_time,
SUBSTRING(FORMAT(DATEADD(hh,tq.seq_list-1,DATEADD(dd, DATEDIFF(dd,0,sysdatetime()), 0)),'yyyy-MM-dd HH:mm:ss'),12,8) as hour_start_time,
SUBSTRING(FORMAT(DATEADD(hh,tq.seq_list,DATEADD(dd, DATEDIFF(dd,0,sysdatetime()), 0)),'yyyy-MM-dd HH:mm:ss'),12,8) as next_hour_start_time
from 
(select number as seq_list
from master.dbo.spt_values  
where type = 'P' and number between 1 and 24)tq



-- part3：异步表兼容逻辑


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间


{% if db_type=="MYSQL" %}
select 
{{ now_time }} as create_time,
{{ now_time }} as update_time,
substr(DATE_add(DATE_FORMAT({{ now_time }}, '%Y-%m-%d 00:00:00'),INTERVAL tq.seq_list-1 hour),11,9) as hour_start_time,
substr(DATE_add(DATE_FORMAT({{ now_time }}, '%Y-%m-%d 00:00:00'),INTERVAL tq.seq_list hour),11,9) as next_hour_start_time
from 
(select 
@num:=@num+1 as seq_list
from 
(SELECT seq_list FROM (SELECT '1' AS seq_list UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
JOIN(SELECT '1' UNION SELECT '2' UNION SELECT '3' UNION SELECT '4' UNION SELECT '5' UNION SELECT '6') AS b ON 1)t,(SELECT @num := 0) as i)tq
{% elif db_type=="SQLSERVER" %}
select 
{{ now_time }} as create_time,
{{ now_time }} as update_time,
SUBSTRING(FORMAT(DATEADD(hh,tq.seq_list-1,DATEADD(dd, DATEDIFF(dd,0,{{ now_time }}), 0)),'yyy-MM-dd HH:mm:ss'),12,8) as hour_start_time,
SUBSTRING(FORMAT(DATEADD(hh,tq.seq_list,DATEADD(dd, DATEDIFF(dd,0,{{ now_time }}), 0)),'yyy-MM-dd HH:mm:ss'),12,8) as next_hour_start_time
from 
(select number as seq_list
from master.spt_values  
where type = 'P' and number between 1 and 24)tq
{% endif %}