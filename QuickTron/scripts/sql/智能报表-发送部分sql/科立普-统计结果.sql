-- ########################################################################
/*
* 统计-效率指标
*/
-- 明细
SELECT 
	`time` as "时间段"
	, order_num as "完成订单数"
	, order_linenum as "完成订单行数"
	, sku_num as "完成货品件数"
	, station_slot_times as "命中槽位次数"
	, into_station_times as "进站次数"
	, win_open_times as "弹窗次数"
	, once_win_open_times as "单次进站弹窗次数"
	, once_picking_quantity as "单次进站完成货品件数"
	, once_station_slot_times as "单次弹窗命中槽位次数"
	, once_order_linenum as "单次进站完成订单行数"
FROM 
	eff_index
WHERE
	project_code = "A51118"
	AND
	DATE(created_date) = DATE(sysdate())

union all

-- 计算合计
SELECT 
	"合计" as "时间段"
	, SUM(order_num) as "完成订单数"
	, SUM(order_linenum) as "完成订单行数"
	, SUM(sku_num) as "完成货品件数"
	, SUM(station_slot_times) as "命中槽位次数"
	, SUM(into_station_times) as "进站次数"
	, SUM(win_open_times) as "弹窗次数"
	, SUM(once_win_open_times) as "单次进站弹窗次数"
	, SUM(once_picking_quantity) as "单次进站完成货品件数"
	, SUM(once_station_slot_times) as "单次弹窗命中槽位次数"
	, SUM(once_order_linenum) as "单次进站完成订单行数"
FROM 
	eff_index
WHERE
	project_code = "A51118"
	AND
	DATE(created_date) = DATE(sysdate())
group by
	DATE(created_date)


union all

-- 计算平均值
SELECT 
	"平均值" as "时间段"
	, AVG(nullif(order_num,0)) as "完成订单数"
	, AVG(nullif(order_linenum,0)) as "完成订单行数"
	, AVG(nullif(sku_num,0)) as "完成货品件数"
	, AVG(nullif(station_slot_times,0)) as "命中槽位次数"
	, AVG(nullif(into_station_times,0)) as "进站次数"
	, AVG(nullif(win_open_times,0)) as "弹窗次数"
	, SUM(win_open_times) / SUM(into_station_times) as "单次进站弹窗次数"   --  弹窗次数 / 进站次数
	, SUM(sku_num) / SUM(into_station_times) as "单次进站完成货品件数"  -- 完成货品件数 / 进站次数 
	, SUM(station_slot_times) / SUM(win_open_times) as "单次弹窗命中槽位次数"  -- 命中槽位次数 / 弹窗次数
	, SUM(order_linenum) / SUM(into_station_times) as "单次进站完成订单行数"  -- 完成订单行数 / 进站次数
FROM 
	eff_index
WHERE
	project_code = "A51118"
	AND
	DATE(created_date) = DATE(sysdate())
;

-- ########################################################################
/*
* 统计-工作站分时效率指标
*/
SELECT
    work_time as "时间段"
    , station_code as "工作站"
   	, into_station_times as "进站次数"
    , order_linenum as "完成订单行数"
    , sku_num as "完成拣货件数"
    , station_slot_times as "命中槽位次数"
    , win_open_times as "弹窗次数"
    , once_win_open_times as "单次进站弹窗次数"
    , once_picking_quantity as "单次进站完成货品件数"
    , once_station_slot_times as "单次弹窗命中槽位次数"
    , once_order_linenum as "单次进站完成订单行数"
	, station_used_rate AS "工作站利用率"
	, station_busy_rate AS "工作站繁忙率"
	, station_online_rate AS "工作站在线率"
	, picking_time AS "平均人工拣货耗时"
FROM
    (
        SELECT 
            1 as sta_type
            , `time` as work_time
            , station_code
            , into_station_times
            , order_linenum
            , sku_num
            , station_slot_times
            , win_open_times
            , once_win_open_times
            , once_picking_quantity
            , once_station_slot_times
            , once_order_linenum
			, station_used_rate -- 工作站利用率
     		, station_busy_rate -- 工作站繁忙率
     		, station_online_rate -- 工作站在线率
			, picking_time -- 平均人工拣货耗时
        FROM 
            eff_index_time
		WHERE
			project_code = "A51118"
			AND
			DATE(created_date) = DATE(sysdate())

        union all

        SELECT 
            2 as sta_type
            , `time` as work_time
            , "平均值" as station_code
            , AVG(nullif(into_station_times,0)) as into_station_times
            , AVG(nullif(order_linenum,0)) as order_linenum
            , AVG(nullif(sku_num,0)) as sku_num
            , AVG(nullif(station_slot_times,0)) as station_slot_times
            , AVG(nullif(win_open_times,0)) as win_open_times

			, SUM(win_open_times) / SUM(into_station_times) as once_win_open_times   --  弹窗次数 / 进站次数
			, SUM(sku_num) / SUM(into_station_times) as once_picking_quantity  -- 完成货品件数 / 进站次数 
			, SUM(station_slot_times) / SUM(win_open_times) as once_station_slot_times  -- 命中槽位次数 / 弹窗次数
			, SUM(order_linenum) / SUM(into_station_times) as once_order_linenum  -- 完成订单行数 / 进站次数

			, AVG(nullif(station_used_rate,0)) AS station_used_rate -- 工作站利用率
     		, AVG(nullif(station_busy_rate,0)) AS station_busy_rate -- 工作站繁忙率
     		, AVG(nullif(station_online_rate,0)) AS station_online_rate -- 工作站在线率
			, AVG(nullif(picking_time,0)) AS picking_time -- 平均人工拣货耗时
        FROM 
            eff_index_time
		WHERE
			project_code = "A51118"
			AND
			DATE(created_date) = DATE(sysdate())
        group by
            `time`
    ) tmp
ORDER BY
    case when work_time between "00:00" and "06:00"  then CONCAT("24",work_time) else work_time END
    ,sta_type
    ,station_code
;

-- ########################################################################
/*
* 统计-工作站空闲率
*/
SELECT 
	station_code as "工作站编号"
    , `07:00`
	, `08:00`
	, `09:00`
	, `10:00`
	, `11:00`
	, `12:00`
	, `13:00`
	, `14:00`
	, `15:00`
	, `16:00`
	, `17:00`
	, `18:00`
	, `19:00`
	, `20:00`
	, `21:00`
	, `22:00`
	, `23:00`
	, `00:00`
	, `01:00`
	, `02:00`
	, `03:00`
	, `04:00`
	, `05:00`
	, `06:00`
FROM 
	station_free
WHERE
	project_code = "A51118"
	AND
	DATE(created_date) = DATE(sysdate())

union all

SELECT 
	"平均值" as "工作站编号"
    , AVG(nullif(`07:00`,1)) as `07:00`
	, AVG(nullif(`08:00`,1)) as `08:00`
	, AVG(nullif(`09:00`,1)) as `09:00`
	, AVG(nullif(`10:00`,1)) as `10:00`
	, AVG(nullif(`11:00`,1)) as `11:00`
	, AVG(nullif(`12:00`,1)) as `12:00`
	, AVG(nullif(`13:00`,1)) as `13:00`
	, AVG(nullif(`14:00`,1)) as `14:00`
	, AVG(nullif(`15:00`,1)) as `15:00`
	, AVG(nullif(`16:00`,1)) as `16:00`
	, AVG(nullif(`17:00`,1)) as `17:00`
	, AVG(nullif(`18:00`,1)) as `18:00`
	, AVG(nullif(`19:00`,1)) as `19:00`
	, AVG(nullif(`20:00`,1)) as `20:00`
	, AVG(nullif(`21:00`,1)) as `21:00`
	, AVG(nullif(`22:00`,1)) as `22:00`
	, AVG(nullif(`23:00`,1)) as `23:00`
	, AVG(nullif(`00:00`,1)) as `00:00`
	, AVG(nullif(`01:00`,1)) as `01:00`
	, AVG(nullif(`02:00`,1)) as `02:00`
	, AVG(nullif(`03:00`,1)) as `03:00`
	, AVG(nullif(`04:00`,1)) as `04:00`
	, AVG(nullif(`05:00`,1)) as `05:00`
	, AVG(nullif(`06:00`,1)) as `06:00`
FROM 
	station_free
WHERE
	project_code = "A51118"
	AND
	DATE(created_date) = DATE(sysdate())
;
