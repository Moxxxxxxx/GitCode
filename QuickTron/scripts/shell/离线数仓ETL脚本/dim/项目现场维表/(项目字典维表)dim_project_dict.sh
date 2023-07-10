#!/bin/bash


dbname=quicktronft_db
hive=/opt/module/hive/bin/hive


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;


use $dbname;


insert into table dim_project_dict values('A51118','上海科力普'),('C35052','虹迪'),('A51091','腾隆浦东'),('A51142','腾隆成都')
,('A51149','鸿星尔克'),('A51264','中储三期扩建项目')
；


"  

$hive -e "$sql"