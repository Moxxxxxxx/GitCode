#!/bin/bash


dbname=quicktronft_db
start_date=2020-01-01
end_date=2021-12-31
hive=/opt/module/hive/bin/hive


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

add jar /data/hive/jar/com.quicktron-1.0-SNAPSHOT.jar;
create temporary function udf_holiday as 'com.quicktron.controll.HolidayUDF';
use $dbname;
insert overwrite table dim_day_date
select 
 d as days
,year(d) as year_date
,month(d) as month_date
,day(d) as day_date
,quarter(d) as quarter_date
,case when dayofweek(d)=1 then '7'
      else dayofweek(d)-1 end as week_date
,weekofyear(d) as week_year_date
, if( trunc(d,'MM')=d,'1',0 ) as is_month_begin
,if(last_day(d)=d,'1',0) as is_month_end
,quicktronft_db.udf_holiday(cast (d as string))
from 
(

select date_add('$start_date', a.pos) as d
  from (select posexplode(split(repeat('o', datediff('$end_date', '$start_date')), 'o'))) a
) t
  
"  

$hive -e "$sql"