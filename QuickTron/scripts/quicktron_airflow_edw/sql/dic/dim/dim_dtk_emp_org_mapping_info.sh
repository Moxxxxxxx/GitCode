#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉组织维度表
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_dtk_user_info_df、dim.dim_dtk_org_info、ods.ods_qkt_dtk_user_roster_df
#-- 输出表 ：dim.dim_dtk_emp_org_mapping_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 
#-- 2 wangziming 2021-12-30 modify 新增org_company_name 字段
#-- 3 wangziming 2022-01-21 modify 新增字段 org_role_type 字段（判断此员工是属于哪个角色）
#-- 4 wangziming 2022-01-25 modify 新增逻辑，根据模糊匹配远程进行匹配判断员工属于什么角色
#-- 5 wangziming 2022-01-25 modify 新增字段 岗位匹配角色逻辑开发
#-- 6 wangziming 2022-02-22 modify 新增字段 email 员工公司邮箱
#-- 7 wangziming 2022-03-15 modify 新增逻辑（员工同时在AMR FT 和硬件自动化的部门，选择硬件自动化的部门）
#-- 8 wangziming 2022-03-16 modify 新增字段 所属部门id和部门名称
#-- 9 wangziming 2022-04-11 modify 熙增字段is_need_fill_manhour
#-- 10 wangziming 2022-04-21 modify 修改逻辑is_need_fill_manhour（组织leader取其一，其中有部门包含 嵌入式硬/软件组 or 智能驾驶组-运动控制小组 则判定为 硬件自动化部门的组织架构，其他情况都归属到对应FT上。这样这六个需要统计的一级部门里面每个人就只有一个组织架构）
#-- 11 wangziming 2022-04-22 modify 修改is_need_fill_manhour 取值逻辑（ 嵌入式硬/软件组 or 智能驾驶组-运动控制小组 则判定为 硬件自动化部门的组织架构，如果不是 嵌入式硬/软件组 or 智能驾驶组-运动控制小组 则判定为 硬件自动化部门的组织架构还包含其他的组织的则硬件自动化的全部为0 其他ft组织根据逻辑判断是否为0）
#-- 12 wangziming 2022-04-24 modify 修改is_need_fill_manhour 取值逻辑（多组织的1： 嵌入式硬/软件组 or 智能驾驶组-运动控制小组 则判定为 硬件自动化部门的组织架构，如（存在于硬件自动化与制造部，归属处理原则：归为硬件自动化，）果不是 嵌入式硬/软件组 or 智能驾驶组-运动控制小组 则判定为 硬件自动化部门的组织架构还包含其他的组织的则硬件自动化的全部为0 单组织的：其他ft组织根据逻辑判断是否为0）
#-- 13 wangziming 2022-05-05 modify 增加多组织的取值逻辑 
#-- 14 wangziming 2022-05-18 modify 增加 多组织的取值逻辑（同时属于制造部和其他部门，都统一归属为制造部。另外，如果改员工不是制造部-工程组的人员，可以不填写工时）
#-- 15 wangziming 2022-05-19 modify 修改 多组织的取值逻辑》 制造部高于硬件自动化 高于其他ft
#-- 16 wangziming 2022-06-09 modify 增加 多组织的取值逻辑 》》 一个人如果属于2个以及2个以上的部门，其中有个是产品技术委员会，则取另一个部门
#-- 17 wangziming 2022-06-13 modify 修正 org_role_type中pe和pm的取值，从花名册中获取（ods_qkt_dtk_user_roster_df）,增加小组字段的map集合
#-- 18 wangziming 2022-07-05 modify 箱式FT李道俊 不填写工时
#-- 19 wangziming 2022-08-04 modify 修改amr ft组织取消导致的部门组织is_valid 不是有效的代码修改
#-- 20 wangziming 2022-09-08 modify 修改 如果 存在一级部门 “硬件自动化” ，则获取员工花名册中的主组织字段的组织
#-- 21 wangziming 2022-12-08 modify 增加字段is_rd_emp 是否研发人员，并增加相应逻辑
#-- 22 wangziming 2022-12-13 modify 修改逻辑，如果研发人员为重复，优先取各ft的数据(智能搬运ft > 箱式ft > 硬件自动化 > 系统中台)
#-- 23 wangziming 2022-12-28 modify 去掉 is_org_role='5'的逻辑
#-- 24 wangziming 2023-01-04 modify 增加 研发人员组的逻辑is_rd_emp
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
tmp_dbname=tmp
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



sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;




with tmp_dtk_emp_org_mapping_str1 as (
select 
p1.user_id as emp_id,
p1.name as emp_name,
p1.org_id,
p2.org_name,
p1.org_company_name,
case when nvl(p6.project_role,'')<>'' then p6.project_role
	 when p5.org_path_name is not null then p5.org_role_tye
	 when p4.org_path_name is not null then p4.org_role_tye 

--	 when p6.org_role_tye is not null then p6.org_role_tye
	 else null end as org_role_type,
p1.email,
p3.org_id_2,
1 as is_valid,
p3.org_id_2 as dept_id,
p3.org_name_2 as dept_name,
if(p7.org_name is not null and p3.org_path_name not like '上海快仓智能科技有限公司/AMR FT/AMR项目交付%' and p3.org_path_name not like '上海快仓智能科技有限公司/箱式FT/箱式FT经营支持中心%','1','0') as is_need_fill_manhour,
-- case when p9.org_name is not null then p9.org_role_tye
--     when p8.org_name is not null then p8.org_role_tye
--     else '0' end as is_rd_emp

case when p8.org_name is not null then p8.org_role_tye
     else '0' end as is_rd_emp
from 
(
select 
a.user_id,
a.name,
a.org_name as org_company_name,
a.position,
b.org_id,
a.email
from 
${ods_dbname}.ods_qkt_dtk_user_info_df a
lateral view explode(split(a.department,',')) b as org_id
where d='${pre1_date}'
) p1
left join ${dim_dbname}.dim_dtk_org_info p2 on p1.org_id =p2.org_id and p1.org_company_name=p2.org_company_name
left join ${dim_dbname}.dim_dtk_org_level_info p3 on p1.org_id=p3.org_id and p1.org_company_name =p3.org_name_1
left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='1') p4 on p3.org_path_name like concat(p4.org_path_name,'%')
left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='0') p5 on p5.org_path_name=p3.org_path_name and p1.user_id=p5.emp_id
-- left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='2') p6 on array_contains(split(p6.positions,','),p1.position)
left join (select user_id,split(project_role,'##')[0] as project_role,org_name from ${ods_dbname}.ods_qkt_dtk_user_roster_df where d='${pre1_date}') p6 on p1.user_id=p6.user_id and p1.org_company_name=p6.org_name
left join (select * from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='3') p7 on p3.org_path_name like concat(p7.org_path_name,'%')
left join (select org_name,org_role_tye,org_path_name from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='4') p8 on p3.org_path_name like concat(p8.org_path_name,'%')
-- left join (select org_name,org_role_tye,org_path_name,emp_id from ${dim_dbname}.dim_dtk_org_role_info_offline where is_org_role='5') p9 on p1.org_id=p9.org_name and p1.user_id=p9.emp_id
),
tmp_rd_map_remove as (
select 
*
from 
(
select 
*,row_number() over(partition by emp_id order by fg ) as rn
from 
(
select 
b.emp_id,
b.emp_name,
b.dept_id,
b.dept_name,
case when b.dept_id='491043050' then 1
     when b.dept_id='479976580' then 2
     when b.dept_id='490452880' then 3
     when b.dept_id='491844029' then 4
     else 888 end as fg
from 
(
select 
emp_id,
emp_name,
count(1)
from 
tmp_dtk_emp_org_mapping_str1
where is_rd_emp='1'
group by emp_id,emp_name having count(1)>1
) a
left join tmp_dtk_emp_org_mapping_str1 b on a.emp_id=b.emp_id
) t
) rt
where rt.rn=1
),
tmp_dtk_emp_org_mapping_str2 as (
select 
t1.emp_id,
case when array_contains(depts,'491043018') or array_contains(depts,'490867617') then '1'
	 when t2.emp_id is not null  and t3.emp_id is not null then '2' 
	 when t2.emp_id is not null and t3.emp_id is null then '3' 
	 when t3.emp_id is not null and t2.emp_id is null then '4' 
	 else '0'  end  as is_valid   
from 
(
select 
user_id as emp_id,
split(department,',') as depts
from 
${ods_dbname}.ods_qkt_dtk_user_info_df
where d='${pre1_date}' and size(split(department,','))>1 
) t1
left join (select distinct emp_id from tmp_dtk_emp_org_mapping_str1 where dept_id<>'490452880' and dept_id <>'491041052' ) t2 on t1.emp_id=t2.emp_id
left join (select distinct emp_id from tmp_dtk_emp_org_mapping_str1 where dept_id='490452880' ) t3 on t1.emp_id=t3.emp_id
),
tmp_dtk_emp_org_mapping_str3 as (
select 
t1.emp_id,
'1' as is_valid
from 
(
select 
user_id as emp_id,
name as emp_name,
split(department,',') as depts
from 
${ods_dbname}.ods_qkt_dtk_user_info_df
where d='${pre1_date}' and size(split(department,','))>1 
) t1
-- left join (select distinct emp_id from tmp_dtk_emp_org_mapping_str1 where dept_id='490452880' ) t2 on t1.emp_id=t2.emp_id
left join (select distinct emp_id from tmp_dtk_emp_org_mapping_str1 where dept_id='115899065' ) t3 on t1.emp_id=t3.emp_id
where 
t3.emp_id is not null 
--and t2.emp_id is not null
),
tmp_dtk_emp_org_mapping_str4 as ( -- 产品技术委员会
select 
t1.emp_id,
'491041052' as dept_id
from 
(
select 
user_id as emp_id,
split(department,',') as depts
from 
${ods_dbname}.ods_qkt_dtk_user_info_df
where d='${pre1_date}' and size(split(department,','))>1
) t1
left join (select distinct emp_id from tmp_dtk_emp_org_mapping_str1 where dept_id='491041052' ) t2 on t1.emp_id=t2.emp_id
where t2.emp_id is not null
),
tmp_dtk_emp_org_mapping_str5 as ( -- 硬件自动化的相关人员以及获取主组织
select 
distinct
a.emp_id,
split(b.main_dept_id,'##')[1] as main_org_id
from 
tmp_dtk_emp_org_mapping_str1 a
left join ${ods_dbname}.ods_qkt_dtk_user_roster_df b on a.emp_id=b.user_id and b.d='${pre1_date}'
where dept_id='490452880'
)

insert overwrite table ${dim_dbname}.dim_dtk_emp_org_mapping_info
select 
t.emp_id,
t.emp_name,
t.org_id,
t.org_name,
t.org_company_name,
t.org_role_type,
email,
case when t4.emp_id is not null and t.org_id=t4.main_org_id then '1'
	 when t4.emp_id is not null and t.org_id<>t4.main_org_id then '0'
	 when t2.emp_id is not null then '0'
	 when t1.emp_id is not null then if(t.dept_id='115899065',t1.is_valid,'0')
	 else t.is_valid end as is_valid,
t.dept_id,
t.dept_name,
is_need_fill_manhour,
map('team1',org_id_3,'team2',org_id_4,'team3',org_id_5,'team4',org_id_6,'team5',org_id_7,'team6',org_id_8,'team7',org_id_9,'team8',org_id_10) as team_org_id_map,
map('team1',org_name_3,'team2',org_name_4,'team3',org_name_5,'team4',org_name_6,'team5',org_name_7,'team6',org_name_8,'team7',org_name_9,'team8',org_name_10) as team_org_name_map,
case when t.emp_id='270702304023207614' then '0'
	 when t5.emp_id is not null and  t.dept_id <> t5.dept_id then '0'
	 else t.is_rd_emp end as is_rd_emp
from 
(
select 
a.emp_id,
a.emp_name,
a.org_id,
a.org_name,
a.org_company_name,
a.org_role_type,
a.email,
case when  a.emp_id in('270702304023207614','02530930072616','03321360662124','02535513248661','02532242113101','186219674824150353') then '1'
	when b.emp_id is not null 
	 then ( case when b.is_valid='1' then if(a.org_id in('491043018','490867617'),'1','0')
	 			 when b.is_valid='2' then if(a.dept_id='490452880','0','1')
	 			 when b.is_valid='0' then '0'
	 			 else '1' end )
	 else '1' end as is_valid,
a.dept_id,
a.dept_name,
case when a.emp_name='吴海贤' then if(a.org_name='系统中台',1,0)
	 when a.emp_name='冯峻' then if(a.org_name='产品支撑组',1,0)
	 when a.emp_name='奚静思' then if(a.org_name='箱式FT',1,0)
	 when a.emp_name='倪菲' then if(a.org_name='硬件自动化',1,0)
	 when a.emp_name='张子宁' then if(a.org_name='智能搬运FT',1,0)
	 when a.emp_id in ('02532242113101','231352005426580389') then '0'
	 else a.is_need_fill_manhour end as is_need_fill_manhour,
a.is_rd_emp
from 
tmp_dtk_emp_org_mapping_str1 a
left join tmp_dtk_emp_org_mapping_str2 b on a.emp_id =b.emp_id
) t
left join tmp_dtk_emp_org_mapping_str3 t1 on t.emp_id=t1.emp_id
left join tmp_dtk_emp_org_mapping_str4 t2 on t.emp_id=t2.emp_id and t.dept_id=t2.dept_id
left join ${dim_dbname}.dim_dtk_org_level_info  t3 on t.org_id=t3.org_id and t.org_name=t3.org_name
left join tmp_dtk_emp_org_mapping_str5 t4 on t.emp_id = t4.emp_id
left join tmp_rd_map_remove t5 on t.emp_id = t5.emp_id
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

