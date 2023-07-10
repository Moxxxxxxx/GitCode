select 
current_date() as date_value,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') as next_hour_start_time,
unix_timestamp(date_format(DATE_ADD(sysdate(), INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(sysdate(), '%Y-%m-%d %H:00:00'))  as sys_run_duration,
count(distinct se.seq_list) as the_hour_cost_seconds,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<date_format(sysdate(), '%Y-%m-%d %H:00:00') then date_format(sysdate(), '%Y-%m-%d %H:00:00') else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') then date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and start_time < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and start_time < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'))
    )
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
















############################################################################################################


set @now_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;

select 
current_date() as date_value,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') as next_hour_start_time,
unix_timestamp(date_format(DATE_ADD(sysdate(), INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(sysdate(), '%Y-%m-%d %H:00:00'))  as sys_run_duration,
count(distinct se.seq_list) as the_hour_cost_seconds,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_start_time then @now_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@next_start_time then @next_start_time else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_start_time and start_time < @next_start_time and
         coalesce(end_time, sysdate()) < @next_start_time) or
        (start_time >= @now_start_time and start_time < @next_start_time and
         coalesce(end_time, sysdate()) >= @next_start_time) or
        (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @now_start_time and
         coalesce(end_time, sysdate()) < @next_start_time) or
        (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @next_start_time)
    )
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag











###########################老版逻辑###########################

select current_date() as date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       sum(case
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.stat_end_time) -
                                                                    UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end)          the_hour_cost_seconds

from (select th.hour_start_time,
             th.next_hour_start_time
      from (select date_format(concat(current_date(), ' ', hour_start_time),
                               '%Y-%m-%d %H:00:00') as hour_start_time,
                   date_format(case
                                   when hour_start_time = '23:00:00' then concat(
                                           date_add(current_date(), interval 1 day), ' ',
                                           next_hour_start_time)
                                   else concat(current_date(), ' ', next_hour_start_time) end,
                               '%Y-%m-%d %H:00:00')    next_hour_start_time
            from qt_smartreport.qt_dim_hour) th
      where th.hour_start_time <= sysdate()) t1
         inner join(select t3.error_id,
                           t3.start_time,
                           t3.end_time,
                           t3.next_error_id,
                           t3.next_error_start_time,
                           case
                               when t3.start_time < {now_start_time} then {now_start_time}
                               else t3.start_time end                                              stat_start_time,
                           case
                               when COALESCE(t3.end_time, sysdate()) <=
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate())
                                   then COALESCE(t3.end_time, sysdate())
                               when COALESCE(t3.end_time, sysdate()) >
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) and
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) < {now_start_time}
                                   then {now_start_time}
                               else COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) end stat_end_time
                    from (select t1.error_id,
                                 t1.start_time,
                                 t1.end_time,
                                 t2.error_id   as next_error_id,
                                 t2.start_time as next_error_start_time,
                                 t2.end_time   as next_error_end_time
                          from (select error_id,
                                       start_time,
                                       end_time,
                                       @rn := @rn + 1 as current_id,
                                       @rn + 1        as next_id
                                from (select id as error_id,
                                             start_time,
                                             end_time
                                      from phoenix_basic.basic_notification
                                      where alarm_module in ('system', 'server')
                                        and alarm_level>=3
                                        and (
                                              (start_time >= {now_start_time} and start_time < {next_start_time} and
                                               coalesce(end_time, sysdate()) < {next_start_time}) or
                                              (start_time >= {now_start_time} and start_time < {next_start_time} and
                                               coalesce(end_time, sysdate()) >= {next_start_time}) or
                                              (start_time < {now_start_time} and
                                               coalesce(end_time, sysdate()) >= {now_start_time} and
                                               coalesce(end_time, sysdate()) < {next_start_time}) or
                                              (start_time < {now_start_time} and
                                               coalesce(end_time, sysdate()) >= {next_start_time})
                                          )) t,
                                     (SELECT @rn := '') tmp
                                order by start_time asc) t1
                                   left join(select error_id,
                                                    start_time,
                                                    end_time,
                                                    @rm := @rm + 1 as current_id,
                                                    @rm + 1        as next_id
                                             from (select id as error_id,
                                                          start_time,
                                                          end_time
                                                   from phoenix_basic.basic_notification
                                                   where alarm_module in ('system', 'server')
                                                     and alarm_level>=3
                                                     and (
                                                           (start_time >= {now_start_time} and
                                                            start_time < {next_start_time} and
                                                            coalesce(end_time, sysdate()) < {next_start_time}) or
                                                           (start_time >= {now_start_time} and
                                                            start_time < {next_start_time} and
                                                            coalesce(end_time, sysdate()) >= {next_start_time}) or
                                                           (start_time < {now_start_time} and
                                                            coalesce(end_time, sysdate()) >= {now_start_time} and
                                                            coalesce(end_time, sysdate()) < {next_start_time}) or
                                                           (start_time < {now_start_time} and
                                                            coalesce(end_time, sysdate()) >= {next_start_time})
                                                       )) t,
                                                  (SELECT @rm := '') tmp
                                             order by start_time asc) t2
                                            ON t1.next_id = t2.current_id) t3) t2 on
    ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
      t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
            t2.stat_end_time >= t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
            t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))
group by t1.hour_start_time,
         t1.next_hour_start_time







---------------------新版--------------------------------------------------------------------------------------------------------------------

set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');



select current_date() as date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       sum(case
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.stat_end_time) -
                                                                    UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end)                                     the_hour_cost_seconds

from (select th.hour_start_time,
             th.next_hour_start_time
      from (select date_format(concat(current_date(), ' ', hour_start_time),
                               '%Y-%m-%d %H:00:00') as hour_start_time,
                   date_format(case
                                   when hour_start_time = '23:00:00' then concat(
                                           date_add(current_date(), interval 1 day), ' ',
                                           next_hour_start_time)
                                   else concat(current_date(), ' ', next_hour_start_time) end,
                               '%Y-%m-%d %H:00:00')    next_hour_start_time
            from qt_smartreport.qt_dim_hour) th
      where th.hour_start_time <= sysdate()) t1
         inner join(select t3.error_id,
                           t3.start_time,
                           t3.end_time,
                           t3.next_error_id,
                           t3.next_error_start_time,
                           case
                               when t3.start_time < @now_start_time then @now_start_time
                               else t3.start_time end                                              stat_start_time,
                           case
                               when COALESCE(t3.end_time, sysdate()) <=
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate())
                                   then COALESCE(t3.end_time, sysdate())
                               when COALESCE(t3.end_time, sysdate()) >
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) and
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) < @now_start_time
                                   then @now_start_time
                               else COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) end stat_end_time
                    from (select t1.error_id,
       t1.start_time,
       t1.end_time,
       t2.error_id   as next_error_id,
       t2.start_time as next_error_start_time,
       t2.end_time   as next_error_end_time
from (select error_id,
             start_time,
             end_time,
             @rn := @rn + 1 as current_id,
             @rn + 1        as next_id
      from (select id as error_id,
                   start_time,
                   end_time
            from phoenix_basic.basic_notification
            where alarm_module in ('system', 'server')
              and alarm_level in (3, 4, 5)
              and (
                    (start_time >= @now_start_time and start_time < @next_start_time and
                     coalesce(end_time, sysdate()) < @next_start_time) or
                    (start_time >= @now_start_time and start_time < @next_start_time and
                     coalesce(end_time, sysdate()) >= @next_start_time) or
                    (start_time < @now_start_time and
                     coalesce(end_time, sysdate()) >= @now_start_time and
                     coalesce(end_time, sysdate()) < @next_start_time) or
                    (start_time < @now_start_time and
                     coalesce(end_time, sysdate()) >= @next_start_time)
                )) t,
           (SELECT @rn := '') tmp
      order by start_time asc) t1
         left join(select error_id,
                          start_time,
                          end_time,
                          @rm := @rm + 1 as current_id,
                          @rm + 1        as next_id
                   from (select id as error_id,
                                start_time,
                                end_time
                         from phoenix_basic.basic_notification
                         where alarm_module in ('system', 'server')
                           and alarm_level in (3, 4, 5)
                           and (
                                 (start_time >= @now_start_time and start_time < @next_start_time and
                                  coalesce(end_time, sysdate()) < @next_start_time) or
                                 (start_time >= @now_start_time and start_time < @next_start_time and
                                  coalesce(end_time, sysdate()) >= @next_start_time) or
                                 (start_time < @now_start_time and
                                  coalesce(end_time, sysdate()) >= @now_start_time and
                                  coalesce(end_time, sysdate()) < @next_start_time) or
                                 (start_time < @now_start_time and
                                  coalesce(end_time, sysdate()) >= @next_start_time)
                             )) t,
                        (SELECT @rm := '') tmp
                   order by start_time asc) t2
                  ON t1.next_id = t2.current_id) t3) t2 on
    ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
      t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
            t2.stat_end_time >= t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
            t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))
group by t1.hour_start_time,
         t1.next_hour_start_time



------------------老版-----------------------------------------------------------------------------------------------------------------------
set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');



select current_date() as date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       sum(case
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.stat_end_time) -
                                                                    UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                    t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.stat_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                    t2.stat_end_time < t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
               when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time
                   then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end)                                     the_hour_cost_seconds

from (select th.hour_start_time,
             th.next_hour_start_time
      from (select date_format(concat(current_date(), ' ', hour_start_time),
                               '%Y-%m-%d %H:00:00') as hour_start_time,
                   date_format(case
                                   when hour_start_time = '23:00:00' then concat(
                                           date_add(current_date(), interval 1 day), ' ',
                                           next_hour_start_time)
                                   else concat(current_date(), ' ', next_hour_start_time) end,
                               '%Y-%m-%d %H:00:00')    next_hour_start_time
            from qt_smartreport.qt_dim_hour) th
      where th.hour_start_time <= sysdate()) t1
         inner join(select t3.error_id,
                           t3.start_time,
                           t3.end_time,
                           t3.next_error_id,
                           t3.next_error_start_time,
                           case
                               when t3.start_time < @now_start_time then @now_start_time
                               else t3.start_time end                                              stat_start_time,
                           case
                               when COALESCE(t3.end_time, sysdate()) <=
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate())
                                   then COALESCE(t3.end_time, sysdate())
                               when COALESCE(t3.end_time, sysdate()) >
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) and
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) < @now_start_time
                                   then @now_start_time
                               else COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) end stat_end_time
                    from (select t1.error_id,
                                 t1.start_time,
                                 t1.end_time,
                                 min(t2.error_id)   as next_error_id,
                                 min(t2.start_time) as next_error_start_time
                          from (select id as error_id,
                                       start_time,
                                       end_time
                                from phoenix_basic.basic_notification
                                where alarm_module in ('system', 'server')
                                  and alarm_level in (3, 4, 5)
                                  and (
                                        (start_time >= @now_start_time and start_time < @next_start_time and
                                         coalesce(end_time, sysdate()) < @next_start_time) or
                                        (start_time >= @now_start_time and start_time < @next_start_time and
                                         coalesce(end_time, sysdate()) >= @next_start_time) or
                                        (start_time < @now_start_time and
                                         coalesce(end_time, sysdate()) >= @now_start_time and
                                         coalesce(end_time, sysdate()) < @next_start_time) or
                                        (start_time < @now_start_time and
                                         coalesce(end_time, sysdate()) >= @next_start_time)
                                    )) t1
                                   left join
                               (select id as error_id,
                                       start_time,
                                       end_time
                                from phoenix_basic.basic_notification
                                where alarm_module in ('system', 'server')
                                  and alarm_level in (3, 4, 5)
                                  and (
                                        (start_time >= @now_start_time and start_time < @next_start_time and
                                         coalesce(end_time, sysdate()) < @next_start_time) or
                                        (start_time >= @now_start_time and start_time < @next_start_time and
                                         coalesce(end_time, sysdate()) >= @next_start_time) or
                                        (start_time < @now_start_time and
                                         coalesce(end_time, sysdate()) >= @now_start_time and
                                         coalesce(end_time, sysdate()) < @next_start_time) or
                                        (start_time < @now_start_time and
                                         coalesce(end_time, sysdate()) >= @next_start_time)
                                    )) t2
                               on t2.start_time > t1.start_time
                          group by t1.error_id, t1.start_time, t1.end_time) t3) t2 on
    ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
      t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
            t2.stat_end_time >= t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
            t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))
group by t1.hour_start_time,
         t1.next_hour_start_time