
---
select
happen_time as countTime,
length(groupUniqArrayArray(splitByChar(',',concat(breakdown_id,',')))) as length1,
(length1-1) as errorNum,
sum(carry_order_num) as carryOrderNum ,
sum(carry_task_num) as  carryTaskNum,
round((sum(theory_time)-sum(error_duration))/sum(theory_time),4) as  oee,
(sum(theory_time)-sum(error_duration)) as k,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0 THEN '0 / 0'
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN CONCAT('0',' / ', toString(carryOrderNum))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryOrderNum / (length1-1))))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN concat(toString(round((length1-1) / carryOrderNum)),' / ',toString(round(carryOrderNum / carryOrderNum)))
END as orderErrorRate,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0
THEN 0
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN 0
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryOrderNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN round(toFloat64((length1-1) / carryOrderNum)/ toFloat64(carryOrderNum / carryOrderNum),6)
END as orderErrorRate1,
CASE WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN '0 / 0'
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN CONCAT('0',' / ', toString(carryTaskNum))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryTaskNum / (length1-1))))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN concat(toString(round((length1-1) / carryTaskNum)),' / ',toString(round(carryTaskNum / carryTaskNum)))
END as taskErrorRate,
CASE  WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN 0
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN 0
WHEN  (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN  round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryTaskNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN  round(toFloat64((length1-1) / carryTaskNum)/toFloat64(carryTaskNum / carryTaskNum),6)
END as taskErrorRate1,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(k)/toFloat64(length1-1),2)
END as mtbf,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(sum(mttr_error_duration))/toFloat64(length1-1),2)
END as mttr,
sum(error_duration) as errorTime,
Max(add_mtbf) as addMtbf
from
ads.ads_amr_breakdown ss final
where
project_code = #{bean.projectCode}
<if test="bean.typeClass != null and bean.typeClass != ''">
    and
    type_class = #{bean.typeClass}
</if>
<if test="bean.amrTypeList != null and bean.amrTypeList.size() != 0">
    and
    amr_type  in
    <foreach collection="bean.amrTypeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>

<if test="bean.agvCodeList != null and bean.agvCodeList.size() != 0">
    and
    amr_code  in
    <foreach collection="bean.agvCodeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>
<if test="bean.startTime != null and bean.startTime !=  '' and bean.endTime != null and bean.endTime !=  ''  ">
    and happen_time between #{bean.startTime} and #{bean.endTime}
</if>
group by
happen_time



----



select
toDate(happen_time)  as countTime,
length(groupUniqArrayArray(splitByChar(',',concat(breakdown_id,',')))) as length1,
(length1-1) as errorNum,
sum(carry_order_num) as carryOrderNum ,
sum(carry_task_num) as  carryTaskNum,
round((sum(theory_time)-sum(error_duration))/sum(theory_time),4) as  oee,
(sum(theory_time)-sum(error_duration)) as k,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0 THEN '0 / 0'
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN CONCAT('0',' / ', toString(carryOrderNum))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryOrderNum / (length1-1))))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN concat(toString(round((length1-1) / carryOrderNum)),' / ',toString(round(carryOrderNum / carryOrderNum)))
END as orderErrorRate,
CASE WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN '0 / 0'
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN CONCAT('0',' / ', toString(carryTaskNum))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryTaskNum / (length1-1))))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN concat(toString(round((length1-1) / carryTaskNum)),' / ',toString(round(carryTaskNum / carryTaskNum)))
END as taskErrorRate,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0 THEN 0
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN 0
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryOrderNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN round(toFloat64((length1-1) / carryOrderNum)/ toFloat64(carryOrderNum / carryOrderNum),6)
END as orderErrorRate1,
CASE  WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN 0
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN 0
WHEN  (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN  round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryTaskNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN  round(toFloat64((length1-1) / carryTaskNum)/toFloat64(carryTaskNum / carryTaskNum),6)
END as taskErrorRate1,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(k)/toFloat64(length1-1),2)
END as mtbf,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(sum(mttr_error_duration))/toFloat64(length1-1),2)
END as mttr,
sum(error_duration) as errorTime,
Max(add_mtbf) as addMtbf
from
ads.ads_amr_breakdown ss final
where
project_code = #{bean.projectCode}
<if test="bean.typeClass != null and bean.typeClass != ''">
    and
    type_class = #{bean.typeClass}
</if>
<if test="bean.amrTypeList != null and bean.amrTypeList.size() != 0">
    and
    amr_type  in
    <foreach collection="bean.amrTypeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>

<if test="bean.agvCodeList != null and bean.agvCodeList.size() != 0">
    and
    amr_code  in
    <foreach collection="bean.agvCodeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>
<if test="bean.startTime != null and bean.startTime !=  '' and bean.endTime != null and bean.endTime !=  ''  ">
    and happen_time between #{bean.startTime} and #{bean.endTime}
</if>
group by
toDate(happen_time)



-----


select
length(groupUniqArrayArray(splitByChar(',',concat(breakdown_id,',')))) as length1,
(length1-1) as errorNum,
sum(carry_order_num) as carryOrderNum ,
sum(carry_task_num) as  carryTaskNum,
round((sum(theory_time)-sum(error_duration))/sum(theory_time),4) as  oee,
(sum(theory_time)-sum(error_duration)) as k,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0 THEN '0 / 0'
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN CONCAT('0',' / ', toString(carryOrderNum))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryOrderNum / (length1-1))))
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN concat(toString(round((length1-1) / carryOrderNum)),' / ',toString(round(carryOrderNum / carryOrderNum)))
END as orderErrorRate,
CASE WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN '0 / 0'
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN CONCAT(toString((length1-1)),' / ','0')
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN CONCAT('0',' / ', toString(carryTaskNum))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN concat(toString(round((length1-1) / (length1-1))),' / ',toString(round(carryTaskNum / (length1-1))))
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN concat(toString(round((length1-1) / carryTaskNum)),' / ',toString(round(carryTaskNum / carryTaskNum)))
END as taskErrorRate,
CASE WHEN (length1-1) = 0 AND carryOrderNum = 0 THEN 0
WHEN (length1-1) != 0 AND carryOrderNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryOrderNum != 0
THEN 0
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1)  <![CDATA[<=]]> carryOrderNum
THEN round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryOrderNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryOrderNum != 0 AND (length1-1) > carryOrderNum
THEN round(toFloat64((length1-1) / carryOrderNum)/ toFloat64(carryOrderNum / carryOrderNum),6)
END as orderErrorRate1,
CASE  WHEN (length1-1) = 0  AND carryTaskNum = 0
THEN 0
WHEN (length1-1) != 0 AND carryTaskNum = 0
THEN toFloat64(length1-1)
WHEN (length1-1) = 0 AND carryTaskNum != 0
THEN 0
WHEN  (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) <![CDATA[<=]]> carryTaskNum
THEN  round(toFloat64((length1-1) / (length1-1))/ toFloat64(carryTaskNum / (length1-1)),6)
WHEN (length1-1) != 0 AND carryTaskNum != 0 AND (length1-1) > carryTaskNum
THEN  round(toFloat64((length1-1) / carryTaskNum)/toFloat64(carryTaskNum / carryTaskNum),6)
END as taskErrorRate1,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(k)/toFloat64(length1-1),2)
END as mtbf,
CASE WHEN (length1-1) = 0
then  -1
when (length1-1) != 0
then round(toFloat64(sum(mttr_error_duration))/toFloat64(length1-1),2)
END as mttr,
sum(error_duration) as errorTime,
Max(add_mtbf) as addMtbf
from
ads.ads_amr_breakdown ss final
where
project_code = #{bean.projectCode}
<if test="bean.typeClass != null and bean.typeClass != ''">
    and
    type_class = #{bean.typeClass}
</if>
<if test="bean.amrTypeList != null and bean.amrTypeList.size() != 0">
    and
    amr_type  in
    <foreach collection="bean.amrTypeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>

<if test="bean.agvCodeList != null and bean.agvCodeList.size() != 0">
    and
    amr_code  in
    <foreach collection="bean.agvCodeList" item="target" index="index" open="(" close=")" separator=",">
        #{target}
    </foreach>
</if>
<if test="bean.startTime != null and bean.startTime !=  '' and bean.endTime != null and bean.endTime !=  ''  ">
    and happen_time between #{bean.startTime} and #{bean.endTime}
</if>