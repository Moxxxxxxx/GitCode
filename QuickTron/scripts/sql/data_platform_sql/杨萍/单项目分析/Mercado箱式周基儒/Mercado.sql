参考 A51495	陌桑FAT	货架到人	2.9.2  -- 没数据
A51149	鸿星尔克AGV货到人项目	货架到人	2.8.2 


-- 机器人状态变更记录
select
create_time as `机器人状态切换时间`,
robot_code as `机器人`,
network_state as `网络连接状态`,
online_state as `在线状态`,
work_state as `作业状态`,
cooperate_state as `交互状态（空闲、充电、工作等）`,
job_sn as `执行任务`,
is_error as `是否故障`,
error_codes as `故障码`,
duration as `状态持续时长`
from phoenix_rms.robot_state_history
where duration is not null 


-- 机器人进出工作站，有货架
select 
biz_type,
count(0)
from evo_station.station_entry
group by biz_type

CYCLECOUNT_ONLINE_G2P_B2P
PICKING_ONLINE_G2P_B2P
PUTAWAY_ONLINE_G2P_DIRECT_B2P
PUTAWAY_ONLINE_G2P_GUIDED_B2P


-- 

select t1.biz_type,count(0)
from evo_station.station_entry t1
left join evo_wcs_g2p.picking_job t2 on t2.project_code=t1.project_code and t1.idempotent_id =t2.job_id
where t1.project_code='A51149'
group by t1.biz_type


evo_wcs_g2p.picking_job
evo_wcs_g2p.putaway_job
evo_wcs_g2p.countcheck_job
evo_wcs_g2p.tally_picking_job
evo_wcs_g2p.tally_putaway_job
evo_wcs_g2p.w2p_picking_job
evo_wcs_g2p.w2p_putaway_job
evo_wcs_g2p.w2p_countcheck_job



evo_rcs.agv_job



针对这个ppt的后4页可能涉及到的数据进行盘点，主要是有3处数据没有，其他的数据基本可以直接或间接得到：
1、机器人利用时长数据，暂时没有记录器人上线、下线、空闲、充电、工作等时间数据
2、异常订单（未完全分配库存）暂时未看到相关异常原因标识概念数据
