## 智能报表 xxjob
    这是描述智能报表的调度任务脚本的一些说明，这个脚本目前是使用 xxjob 进行调度，每天*7：30*计算数据
### 代码说明
- **eff_index_sql** 效率指标概览，往 eff_index 表存入数据
- **eff_index_time_sql** 效率指标详表，往 eff_index_time 表存入数据
- **station_free_sql** 效率指标概览，往 station_free 表存入数据，这里需要注意转义

### 执行步骤
1. 清空当天的相关项目的数据，避免多次重跑出现问题
```
eff_index
eff_index_time
station_free
```
2. 执行插入脚本
```
eff_index_sql
eff_index_time_sql
station_free_sql
```

### 任务链接
- xxjob 名称
```
<中储>智能报表基础数据
<智选云仓>智能报表基础数据
<科立普>智能报表基础数据
```
