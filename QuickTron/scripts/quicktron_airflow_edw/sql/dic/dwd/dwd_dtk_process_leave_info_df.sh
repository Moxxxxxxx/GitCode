#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉人员请假信息记录
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_leave_di
#-- 输出表 ：dwd.dwd_dtk_process_leave_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-21 CREATE 
#-- 2 wangziming 2022-05-12 modify 增加分表（系统中台）
#-- 3 wangziming 2022-07-05 modify 由于撤销的值会新生成一条数据，要把撤销和原来的那个删除
#-- 4 wangziming 2022-08-22 modify 根据business_id进行排序，取finish_time最晚的一条数据，并把撤销的为0 其它的为1
#-- 5 wangziming 2022-12-26 modify 宝仓的人员请假数据人员名称字段补全
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_dtk_process_leave_info_df partition(d='${pre1_date}')
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid, 
result as process_result, 
status as process_status, 
title as process_title, 
applicant_name, 
leave_dept_name, 
split(start_time,' ')[0] as start_date, 
split(end_time,' ')[0] as end_date,
case upper(nvl(split(start_time,' ')[1],'')) when '上午' then '上午'
							  when 'AM' then '上午'
							  when '下午' then '下午'
							  when 'PM' then '下午'
							  when '' then '上午'
							  else null end as start_time_period,
case upper(nvl(split(end_time,' ')[1],'')) when '下午' then '下午'
							  when 'PM' then '下午'
							  when '上午' then '上午'
							  when 'AM' then '上午'
							  when '' then '下午'
							  else null end as end_time_period,
start_time as source_start_time,
end_time as source_end_time,							  	
regexp_replace(duration,'[\\\"\\s+]','') as leave_days,
leave_type, 
leave_reasons
from 
(
select
*,
row_number() over(partition by org_name,process_instance_id order by d desc) as rn
from 
${ods_dbname}.ods_qkt_dtk_process_leave_di
) t
where t.rn=1
and duration is not null
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_dtk_process_leave_info_df partition(d='${pre1_date}')
select 
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
if(nvl(applicant_name,'')='',split(process_title,'提交')[0],applicant_name) as applicant_name, 
leave_dept_name, 
start_date, 
end_date,
start_time_period,
end_time_period,
source_start_time,
source_end_time,	
leave_days,
leave_type, 
leave_reasons,
if(biz_action='REVOKE','0','1') as is_valid
from 
(
select 
*,
row_number() over(partition by org_name,business_id order by finish_time desc) as rnt
from 
(
select 
rt.org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
rt.business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
applicant_name, 
leave_dept_name, 
start_date, 
end_date,
start_time_period,
end_time_period,
source_start_time,
source_end_time,	
leave_days,
leave_type, 
leave_reasons,
is_valid
from 
(
select 
*,
row_number() over(partition by org_name,process_instance_id order by flag desc) as rn

from 
(
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_userid as originator_user_id, 
result as process_result, 
status as process_status, 
title as process_title, 
applicant_name, 
leave_dept_name, 
split(start_time,' ')[0] as start_date, 
split(end_time,' ')[0] as end_date,
case upper(nvl(split(start_time,' ')[1],'')) when '上午' then '上午'
							  when 'AM' then '上午'
							  when '下午' then '下午'
							  when 'PM' then '下午'
							  when '' then '上午'
							  else null end as start_time_period,
case upper(nvl(split(end_time,' ')[1],'')) when '下午' then '下午'
							  when 'PM' then '下午'
							  when '上午' then '上午'
							  when 'AM' then '上午'
							  when '' then '下午'
							  else null end as end_time_period,
start_time as source_start_time,
end_time as source_end_time,							  	
regexp_replace(duration,'[\\\"\\s+]','') as leave_days,
leave_type, 
leave_reasons,
2 as flag,
'1' as is_valid
from 
${ods_dbname}.ods_qkt_dtk_process_leave_di
where d='${pre1_date}'
and duration is not null

union all
select 
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
applicant_name, 
leave_dept_name, 
start_date, 
end_date, 
start_time_period,
end_time_period,
source_start_time,
source_end_time,
leave_days,
leave_type, 
leave_reasons,
1 as flag,
is_valid
from 
${dwd_dbname}.dwd_dtk_process_leave_info_df
where d='${pre2_date}'
) t
) rt
where rn=1
) rt1
) rt2
where rnt=1

;



-- 回流请假数据到指定表
with tmp_dtk_process_leave_str1 as (
select 
*,
datediff(end_date,start_date) as array_size
from 
dwd.dwd_dtk_process_leave_info_df 
where d='${pre1_date}' 
 and leave_dept_name ='系统中台'

)
insert overwrite table tmp.tmp_dtk_process_leave_info
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
process_result, 
process_status, 
process_title, 
applicant_name, 
leave_dept_name, 
leave_type, 
start_date,
end_date,
start_time_period,
end_time_period,
total_leave_days,
leave_reasons,
leave_date,
every_days
from 
(
select
a.org_name, 
a.process_instance_id, 
a.attached_process_instance_ids,
a.biz_action, 
a.business_id, 
a.cc_userids, 
a.create_time, 
a.finish_time, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_user_id, 
a.process_result, 
a.process_status, 
a.process_title, 
a.applicant_name, 
a.leave_dept_name, 
a.leave_type, 
a.start_date,
a.end_date,
a.start_time_period,
a.end_time_period,
a.leave_days as total_leave_days,
REGEXP_REPLACE(a.leave_reasons,'\\s+','') as leave_reasons,
date_add(a.start_date,b.pos) as leave_date,
case when a.leave_days like '%.5%' then (
		case when a.start_time_period ='上午' and a.leave_days>1 then if(b.pos=a.array_size,0.5,1)
			 when a.start_time_period in('上午','下午') and a.leave_days<1 then 0.5
			 when a.start_time_period='下午'  and a.leave_days>1 then if(b.pos=0,0.5,1)
			 else 999 end )
     when a.leave_days not like '%.5%'  then (
     	case when a.start_time_period ='上午' then 1
     	     when a.start_time_period='下午' then if(b.pos=0 or b.pos=a.array_size,0.5,1)
     	     else 888 end )
     
	 
     else 777 end as every_days
from 
tmp_dtk_process_leave_str1 a
lateral view posexplode(split(repeat('o',datediff(a.end_date,a.start_date)),'o')) b
) t
left join ${dim_dbname}.dim_day_date c on c.days=t.leave_date
where c.day_type in ('0','3')
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"





#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="kc_collect"                                     


ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table tmp_dtk_process_leave_info;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/kc_collect?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table tmp_dtk_process_leave_info \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/tmp.db/tmp_dtk_process_leave_info \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "org_name, process_instance_id, attached_process_instance_ids, biz_action, business_id, cc_userids, create_time, finish_time, originator_dept_id, originator_dept_name, originator_user_id, process_result, process_status, process_title, applicant_name, leave_dept_name, leave_type, start_date, end_date, start_time_period, end_time_period, total_leave_days, leave_reasons, leave_date, every_days"