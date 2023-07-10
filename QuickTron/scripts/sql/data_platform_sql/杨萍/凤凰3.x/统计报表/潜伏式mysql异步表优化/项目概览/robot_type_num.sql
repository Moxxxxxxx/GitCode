-- 用于：统计报表->项目概览->机器人类型（数量）

select brt.robot_type_code,  -- 机器人类型
       brt.robot_type_name,  -- 机器人类型名称
       count(distinct br.robot_code)                                              as robot_num,  -- 机器人数量
       count(distinct case when tm.robot_code is not null then br.robot_code end) as offline_maintenance_num  -- 下线维修机器人数量
from phoenix_basic.basic_robot br
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
         left join (select distinct robot_code
                    from phoenix_rms.robot_maintain_record  
                    where end_time is null) tm on tm.robot_code = br.robot_code
where br.usage_state = 'using'
group by brt.robot_type_code, brt.robot_type_name