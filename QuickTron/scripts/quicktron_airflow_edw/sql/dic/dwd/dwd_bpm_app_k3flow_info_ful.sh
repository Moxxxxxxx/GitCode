#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm dwd层  流程表单信息数据
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_bpm_app_k3flow_df、
#-- 输出表 ：dwd.dwd_bpm_app_k3flow_info_ful、dwd_bpm_final_verification_report_milestone_info_ful、dwd_bpm_equipment_arrival_confirmation_milestone_info_ful、dwd_bpm_external_project_handover_info_ful、dwd_bpm_technical_scheme_review_info_ful、dwd_bpm_contract_review_info_ful、dwd_bpm_supplementary_contract_review_info_ful、dwd_bpm_contract_change_review_info_ful、dwd.dwd_bpm_external_project_pre_apply_info_ful 、dwd_bpm_personal_expense_account_item_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-28 CREATE 
#-- 2 wangziming 2022-03-24 modify 增加dwd_bpm_technical_scheme_review_info_ful 字段 project_ft 项目ft
#-- 3 wangziming 2022-03-25 modify 增加字段状态值（部分表）,增加字段正式项目编码dwd_bpm_contract_review_info_ful
#-- 4 wangziming 2022-03-28 modify 增加表 dwd_bpm_external_project_pre_apply_info_ful
#-- 5 wangziming 2022-04-19 modify 增加表 dwd_bpm_project_delivery_approval_info_ful、dwd_bpm_materials_purchase_request_info_ful、dwd_bpm_purchase_request_change_info_ful
#-- 6 wangziming 2022-04-20 modify 增加表字段 dwd_bpm_online_report_milestone_info_ful 实际上线时间
#-- 7 wangziming 2022-05-06 modify 增加表 dwd_bpm_personal_expense_account_info_ful
#-- 8 wangziming 2022-05-26 modify 增加设备到货里程碑新的字段
#-- 9 wangziming 2022-06-07 modify 增加bpm 项目暂停申请表
#-- 10 wangziming 2022-06-29 modify 增加 dwd_bpm_personal_expense_account_item_info_ful（个人报销明细行表）
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
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

columns=id,FlowID,FlowStatus,FlowModelID,ApplyID,ApplyName,DeptID,DeptName,OrgID,OrgName,FlowName,StartDate,EndDate,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12,date13,date14,date15,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,Number1,Number2,Number3,Number4,Number5,Number6,Number7,Number8,Number9,Number10,Number11,Number12,Number13,Number14,Number15,Number16,Number17,Number18,Number19,Number20,Number21,Number22,Number23,Number24,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,Description,BackSucess,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string51,string52,string53,string54,string55,string56,string57,string58,string59,string60,string61,string62,string63,string64,string65,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,string91,string92,string93,string94,string95,string96,string97,string98,string99,string100,bool11,bool12,bool13,bool14,bool15,bool16,bool17,bool18,bool19,bool20,text1,oFlowModelID,ApplyAcc,ErpMsgID,VoucherID,CheckID,text2,text3,text4,text5,cash,budget,cashflow,zdbh,cashflow2,cashflow1,GUID,PrintCount,Office1,FileType,FileSize,Number25,Number26,Number27,Number28,Number29,Number30,Number31,Number32,Number33,Number34,Number35,Number36,Number37,Number38,Number39,Number40,Number41,Number42,Number43,Number44,Number45,backSql


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful
select 
${columns}
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where d='${pre1_date}'
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"

