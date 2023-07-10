##step1:建表
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_sage_test
(
    `id`           int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `analyse_date` datetime  NOT NULL COMMENT '分析日期',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='测试表';	
	
	
	
##step2:删除当天相关数据	
DELETE
FROM qt_smartreport.qt_sage_test
WHERE analyse_date = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  
   	
	
##step3:插入当天相关数据	
insert into qt_smartreport.qt_sage_test(analyse_date)
SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'), INTERVAL (-(@u := @u + 1)) HOUR),'%Y-%m-%d %H:00:00') as analyse_date
FROM (SELECT a
      FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
               JOIN(SELECT '1'
                    UNION
                    SELECT '2'
                    UNION
                    SELECT '3'
                    UNION
                    SELECT '4'
                    UNION
                    SELECT '5'
                    UNION
                    SELECT '6') AS b ON 1) AS b,
     (SELECT @u := -1) AS i

----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------
问题：
1、在rms的总作业单（phoenix_rms.transport_order  ）里最好也记下起始点和目标点，目前是在rss中的各类车型的order里才能对应取到
2、暂时没看到有相关数据可以做到：小车执行作业单的行驶距离、速度
3、暂时没有看到小车状态埋点数据（带载作业、空车作业、空闲、充电、锁定、异常、离线）
4、暂时没有看到小车充电次数、电量、充电时长数据





--凤凰3.x中控数据-搬运作业统计



###页面1：搬运作业单单量统计

--搬运作业单趋势统计
select `日期`,
       sum(`创建作业单数`) as `创建作业单数`,
       sum(`完成作业单数`) as `完成作业单数`
from (select date_format(create_time, '%Y-%m-%d') as `日期`,
             count(distinct order_id)             as `创建作业单数`,
             null                                 as `完成作业单数`
      from phoenix_rms.transport_order
      group by 1
      union all
      select date_format(update_time, '%Y-%m-%d') as `日期`,
             null                                 as `创建作业单数`,
             count(distinct order_id)             as `完成作业单数`
      from phoenix_rms.transport_order
      where state = 'DONE'
      group by 1) t
group by 1




--搬运作业单24小时单量分布
select `日期`,
       `小时`,
       sum(`创建作业单数`) as `创建作业单数`,
       sum(`完成作业单数`) as `完成作业单数`
from (select date_format(create_time, '%Y-%m-%d') as `日期`,
             HOUR(create_time)                    as `小时`,
             count(distinct order_id)             as `创建作业单数`,
             null                                 as `完成作业单数`
      from phoenix_rms.transport_order
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `日期`,
             HOUR(update_time)                    as `小时`,
             null                                 as `创建作业单数`,
             count(distinct order_id)             as `完成作业单数`
      from phoenix_rms.transport_order
      where state = 'DONE'
      group by 1, 2) t
group by 1, 2





--搬运作业单单量统计明细 
select `统计时间`,
       `作业单类型`,
       sum(`新增单量`)     as `新增单量`,
       sum(`完成单量`)     as `完成单量`,
       sum(`作业机器人数量`)  as `作业机器人数量`,
       sum(`当日累计单量`)   as `当日累计单量`,
       sum(`当日累计完成单量`) as `当日累计完成单量`,
       '天'             as `统计维度`
from (select date_format(create_time, '%Y-%m-%d') as `统计时间`,
             order_type                           as `作业单类型`,
             count(distinct order_id)             as `新增单量`,
             null                                 as `完成单量`,
             count(distinct robot_code)           as `作业机器人数量`,
             count(distinct order_id)             as `当日累计单量`,
             null                                 as `当日累计完成单量`
      from phoenix_rms.transport_order
      group by 1, 2
      union all
      select date_format(update_time, '%Y-%m-%d') as `统计时间`,
             order_type                           as `作业单类型`,
             null                                 as `新增单量`,
             count(distinct order_id)             as `完成单量`,
             null                                 as `作业机器人数量`,
             null                                 as `当日累计单量`,
             count(distinct order_id)             as `当日累计完成单量`
      from phoenix_rms.transport_order
      where state = 'DONE'
      group by 1, 2) t
group by 1, 2
union all
select `统计时间`,
       `作业单类型`,
       sum(`新增单量`)     as `新增单量`,
       sum(`完成单量`)     as `完成单量`,
       sum(`作业机器人数量`)  as `作业机器人数量`,
       sum(`当日累计单量`)   as `当日累计单量`,
       sum(`当日累计完成单量`) as `当日累计完成单量`,
       '小时'            as `统计维度`
from (select t1.`统计时间`,
             t1.`作业单类型`,
             t1.`新增单量`,
             null           as `完成单量`,
             t1.`作业机器人数量`,
             sum(t2.`新增单量`) as `当日累计单量`,
             null           as `当日累计完成单量`
      from (select date_format(create_time, '%Y-%m-%d %H:00:00') as `统计时间`,
                   order_type                                    as `作业单类型`,
                   count(distinct robot_code)                    as `作业机器人数量`,
                   count(distinct order_id)                      as `新增单量`
            from phoenix_rms.transport_order
            group by 1, 2) t1
               left join
           (select date_format(create_time, '%Y-%m-%d %H:00:00') as `统计时间`,
                   order_type                                    as `作业单类型`,
                   count(distinct order_id)                      as `新增单量`
            from phoenix_rms.transport_order
            group by 1, 2) t2
           on t1.`作业单类型` = t2.`作业单类型` and date_format(t1.`统计时间`, '%Y-%m-%d') = date_format(t2.`统计时间`, '%Y-%m-%d')
      where t2.`统计时间` <= t1.`统计时间`
      group by 1, 2, 3, 4, 5

      union all

      select t1.`统计时间`,
             t1.`作业单类型`,
             null           as `新增单量`,
             t1.`完成单量`,
             null           as `作业机器人数量`,
             null           as `当日累计单量`,
             sum(t2.`完成单量`) as `当日累计完成单量`
      from (select date_format(update_time, '%Y-%m-%d %H:00:00') as `统计时间`,
                   order_type                                    as `作业单类型`,
                   count(distinct order_id)                      as `完成单量`
            from phoenix_rms.transport_order
            where state = 'DONE'
            group by 1, 2) t1
               left join
           (select date_format(update_time, '%Y-%m-%d %H:00:00') as `统计时间`,
                   order_type                                    as `作业单类型`,
                   count(distinct order_id)                      as `完成单量`
            from phoenix_rms.transport_order
            where state = 'DONE'
            group by 1, 2) t2
           on t1.`作业单类型` = t2.`作业单类型` and date_format(t1.`统计时间`, '%Y-%m-%d') = date_format(t2.`统计时间`, '%Y-%m-%d')
      where t2.`统计时间` <= t1.`统计时间`
      group by 1, 2, 3, 4, 5) t
group by 1, 2



###页面2：搬运作业单效率统计

SELECT date_format(t.update_time, '%Y-%m-%d')                                                            as `日期`,
       case
           when t1.order_id is not null then concat(t1.start_point, '-', t1.target_point)
           when t2.order_id is not null then concat(t2.source_point_code, '-', t2.target_point_code) end as `起始点-目标点`,
       t.order_type                                                                                      as `作业单类型`,
       count(distinct t.order_id)                                                                        as `搬运次数`,
       sum(unix_timestamp(t.update_time) - unix_timestamp(t.create_time))                                as `总搬运耗时`,
       avg(unix_timestamp(t.update_time) - unix_timestamp(t.create_time))                                as `平均搬运耗时`,
       max(unix_timestamp(t.update_time) - unix_timestamp(t.create_time))                                as `最长搬运耗时`,
       min(unix_timestamp(t.update_time) - unix_timestamp(t.create_time))                                as `最短搬运耗时`
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
         left join phoenix_rss.rss_fork_order t2 on t2.order_id = t.order_id
where t.state = 'DONE'
group by 1, 2, 3












SELECT AVG(DISTINCT a.salary) AS median_salary
FROM
	(SELECT a.salary
	 FROM salaries AS a, salaries AS b
	 GROUP BY a.salary
	 HAVING SUM(CASE WHEN b.salary >= a.salary THEN 1 ELSE 0 END) >= COUNT(*) / 2 
	 AND SUM(CASE WHEN b.salary <= a.salary THEN 1 ELSE 0 END) >= COUNT(*) / 2 ) AS a;
	 
	 
-------------------------------------------------------------------------------
算中位数


用例数据：
select 
create_time,
update_time,
unix_timestamp(update_time) - unix_timestamp(create_time) as cost_time
from phoenix_rms.transport_order
where state = 'DONE'
order by date_format(update_time, '%Y-%m-%d')  ,cost_time	 



-------------------------------------------
phoenix_rms.transport_order_link.state 
LIFT_UP_START    #顶升
LIFT_UP_DONE

MOVE_START  #空车移动
MOVE_DONE

RACK_MOVE_START  #带载移动
RACK_MOVE_DONE


PUT_DOWN_START  #放下
PUT_DOWN_DONE

-------------------------------------------


select 
	a.nodeId,
	a.cpuCharge-b.cpuCharge cpuCharge, 
	a.chargeTime from
(select @arownum:=@arownum+1 rownum,nodeId,cpuCharge,chargeTime from rn_ext_vir_instance_charge_history,(select @arownum:=0) t where nodeId = 318263374 order by chargeTime) a,
(select @brownum:=@brownum+1 rownum,nodeId,cpuCharge,chargeTime from rn_ext_vir_instance_charge_history,(select @brownum:=1) t where nodeId = 318263374 order by chargeTime) b
where a.rownum = b.rownum;  


-----------------------------
select 
t1.order_id ,t1.state,t1.create_time  as done_time,max(t2.create_time) as start_time
from 
(select 
order_id ,state,create_time 
from phoenix_rms.transport_order_link
where 1=1
and order_id in ('SIRack_164585032049400009','SIRack_164585313954700011')
and state ='RACK_MOVE_DONE')t1 
left join 
(select 
order_id ,state,create_time 
from phoenix_rms.transport_order_link
where 1=1
and order_id in ('SIRack_164585032049400009','SIRack_164585313954700011')
and state ='RACK_MOVE_START')t2 on t2.order_id=t1.order_id and t2.create_time <t1.create_time 
group by 1,2,3 




-------------------------------------------------------------
--胡斌给
SELECT
  cj.job_sn
FROM
  phoenix_rss.rss_carrier_job cj,
  phoenix_rss.rss_carrier_order co 
WHERE
  cj.busi_group_id = co.id 
  AND co.order_id = 'SIRack_164448484379200003'
  
  
------------------------------------------------
/**
 * 机器人状态变更类型
 *
 * @author Charles
 */
public enum RobotStateChangeType {
    /**
     * 未连接
     *
     * @see com.kc.phoenix.common.data.enums.RobotNetworkState
     */
    DISCONNECTED,
    /**
     * 已连接
     *
     * @see com.kc.phoenix.common.data.enums.RobotNetworkState
     */
    CONNECTED,
    /**
     * 注册
     *
     * @see com.kc.phoenix.common.data.enums.RobotOnlineState
     */
    REGISTER,
    /**
     * 下线
     *
     * @see com.kc.phoenix.common.data.enums.RobotOnlineState
     */
    DEREGISTER,
    /**
     * 机器人作业状态：空闲
     *
     * @see com.kc.phoenix.common.data.enums.RobotWorkState
     */
    IDLE,
    /**
     * 机器人作业状态：繁忙
     *
     * @see com.kc.phoenix.common.data.enums.RobotWorkState
     */
    BUSY,
    /**
     * 机器人作业状态：错误
     *
     * @see com.kc.phoenix.common.data.enums.RobotWorkState
     */
    ERROR,
    /**
     * 机器人作业状态：充电中
     *
     * @see com.kc.phoenix.common.data.enums.RobotWorkState
     */
    CHARGING,
    /**
     * 机器人作业状态：暂停
     *
     * @see com.kc.phoenix.common.data.enums.RobotWorkState
     */
    PAUSE,
    /**
     * 机器人锁定
     */
    LOCK,
    /**
     * 机器人释放
     */
    UNLOCK,
    /**
     * 机器人故障离场
     */
    FAULT_LEAVE;
}


-------------------------------------------------------
select t1.*,
       t2.network_state as next_network_state,
       t2.online_state  as next_online_state,
       t2.work_state    as next_work_state,
       t2.job_sn        as next_job_sn,
       t2.cause         as next_cause
from (select a.*,
             min(b.id)          as next_id,
             min(b.create_time) as next_time
      from (select id,
                   robot_code,
                   create_time,
                   network_state,
                   online_state,
                   work_state,
                   job_sn,
                   cause
            from phoenix_rms.robot_state_history) a
               left join
           (select id, robot_code, create_time
            from phoenix_rms.robot_state_history) b on b.robot_code = a.robot_code and b.create_time > a.create_time
      group by 1, 2, 3, 4, 5, 6, 7, 8) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.create_time = t1.next_time 
		 
-----------------------------------------------------------------
	 
select t.*,
       case
           when t.create_time < date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00')
               then date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00')
           else t.create_time end as start_time,
       case
           when t.next_time >= date_format(now(), '%Y-%m-%d 00:00:00') then date_format(now(), '%Y-%m-%d 00:00:00')
           else t.next_time end   as end_time
from (select t.*,
             coalesce(t.next_create_time,
                      date_format(date_add(t.create_time, interval 1 day), '%Y-%m-%d 00:00:00')) as next_time
      from (select a.*,
                   min(b.id)          as next_id,
                   min(b.create_time) as next_create_time
            from (select id,
                         robot_code,
                         create_time,
                         network_state,
                         online_state,
                         work_state,
                         job_sn,
                         cause
                  from phoenix_rms.robot_state_history
                  where date_format(create_time, '%Y-%m-%d') >=
                        date_format(date_add(now(), interval -7 day), '%Y-%m-%d')
                 ) a
                     left join
                 (select id, robot_code, create_time
                  from phoenix_rms.robot_state_history
                  where date_format(create_time, '%Y-%m-%d') >=
                        date_format(date_add(now(), interval -7 day), '%Y-%m-%d')
                 ) b on b.robot_code = a.robot_code and b.create_time > a.create_time
            group by 1, 2, 3, 4, 5, 6, 7, 8) t) t
where 1 = 1
  and ((create_time >= date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00') and
        create_time < date_format(now(), '%Y-%m-%d 00:00:00') and next_time < date_format(now(), '%Y-%m-%d 00:00:00'))
    or
       (create_time >= date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00') and
        create_time < date_format(now(), '%Y-%m-%d 00:00:00') and next_time >= date_format(now(), '%Y-%m-%d 00:00:00'))
    or
       (create_time < date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00') and
        next_time >= date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00') and
        next_time < date_format(now(), '%Y-%m-%d 00:00:00'))
    or
       (create_time < date_format(date_add(now(), interval -1 day), '%Y-%m-%d 00:00:00') and
        next_time >= date_format(now(), '%Y-%m-%d 00:00:00')))     

---------------------------------------------------------------------------------------------
select br.robot_code,
       brt.first_classification,
       case
           when brt.first_classification = 'WORKBIN' then '料箱车'
           when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
           when brt.first_classification = 'CARRIER' then '潜伏式'
           when brt.first_classification = 'ROLLER' then '辊筒'
           when brt.first_classification = 'FORKLIFT' then '堆高全向车'
           when brt.first_classification = 'DELIVER' then '投递车'
           when brt.first_classification = 'SC' then '四向穿梭车'
           else brt.first_classification end as first_classification_name
from phoenix_basic.basic_robot br
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id      

      
---------------------------------------------------------------------------------------------------
select rsh.*,
rcj.job_type ,
rco.order_type 
from phoenix_rms.robot_state_history rsh 
left join phoenix_rss.rss_carrier_job rcj on rcj.job_sn =rsh.job_sn 
left join phoenix_rss.rss_carrier_order rco on rco.id=rcj.busi_group_id
where rcj.job_sn is not null
----------------------------------------------------------------------------------
select 
date_format(start_time, '%Y-%m-%d 00:00:00') as time_value,
date(start_time) as date_value,
Hour(start_time) as hour_value,
start_time,
robot_code,
alarm_module,
id
from phoenix_basic.basic_notification
where date_format(start_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')

--------------------------------------------------------------------------------
##step6-3:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='小时'
#alarm_module = 'system' and alarm_type = 'job'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.order_id                                                                                                                                as object_code,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(t.the_hour_cost_seconds), 0)                                                                                                  as notification_time,
       cast(coalesce(sum(t.the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                   as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 - coalesce(sum(t.the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                                       as time_type
from (select hour_start_time,
             next_hour_start_time,
             order_id,
             order_create_time,
             order_update_time,
             order_state
      from (select t1.hour_start_time,
                   t1.next_hour_start_time,
                   t2.order_id,
                   t2.order_create_time,
                   t2.order_update_time,
                   t2.order_state
            from (select th.day_hours                               as hour_start_time,
                         DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                  from (SELECT DATE_FORMAT(
                                       DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                                INTERVAL
                                                (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
                        FROM (SELECT a
                              FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                                       JOIN(SELECT '1'
                                            UNION
                                            SELECT '2'
                                            UNION
                                            SELECT '3'
                                            UNION
                                            SELECT '4'
                                            UNION
                                            SELECT '5'
                                            UNION
                                            SELECT '6') AS b ON 1) AS b,
                             (SELECT @u := -1) AS i) th) t1
                     left join (select order_id,
                                       order_type,
                                       robot_code,
                                       create_time as order_create_time,
                                       update_time as order_update_time,
                                       state       as order_state
                                from phoenix_rms.transport_order
                                where 1 = 1
                                  and ((create_time >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time <
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time <
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00')))
            ) t2 on 1) ta
      where 1 = 1
        and ((order_create_time >= hour_start_time and order_create_time < next_hour_start_time and
              date_format(coalesce(order_update_time, sysdate()), '%Y-%m-%d %H:%i:%s') < next_hour_start_time)
          or (order_create_time >= hour_start_time and order_create_time < next_hour_start_time and
              date_format(coalesce(order_update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >= next_hour_start_time) or
             (order_create_time < hour_start_time and
              date_format(coalesce(order_update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >= hour_start_time and
              date_format(coalesce(order_update_time, sysdate()), '%Y-%m-%d %H:%i:%s') < next_hour_start_time)
          or (order_create_time < hour_start_time and
              date_format(coalesce(order_update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >= next_hour_start_time))) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'job' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.job_order = tt.order_id and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.order_id
;

-----------------------------------------------------------------

select t.order_id,
       t.create_time                                                 as order_create_time,
       t.update_time                                                 as order_done_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.update_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as total_time_consuming

from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
		 
where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
------------------------------------------------------------------------------------------------------------------------
select h.hour_start_time,
       h.next_hour_start_time
from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'), INTERVAL
                                        (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
            FROM (SELECT a
                  FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                           JOIN(SELECT '1'
                                UNION
                                SELECT '2'
                                UNION
                                SELECT '3'
                                UNION
                                SELECT '4'
                                UNION
                                SELECT '5'
                                UNION
                                SELECT '6') AS b ON 1) AS b,
                 (SELECT @u := -1) AS i) th) h
where h.hour_start_time <= sysdate()		 

---------------------------------------------------------------------------
