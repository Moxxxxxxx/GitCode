#!/bin/bash


dbname=quicktronft_db
start_second=23:59:59
end_second=00:00:00
init_date="2021-08-11 00:00:00"
hive=/opt/module/hive/bin/hive


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;



use $dbname;
insert overwrite table dim_day_of_second
select 
from_unixtime(unix_timestamp('$init_date')+a.pos,'HH') as start_hour,
from_unixtime(unix_timestamp('$init_date')+a.pos,'mm') as  start_minute,
from_unixtime(unix_timestamp('$init_date')+a.pos,'HH:mm:ss') as second_of_day
from 
(select posexplode(split(repeat('o', cast(unix_timestamp('$start_second','HH:mm:ss')-unix_timestamp('$end_second','HH:mm:ss') as int)), 'o'))) a

  
"  
echo "$sql"

$hive -e "$sql"

