select bn.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       bn.id                                                                            as error_id,
       bn.error_code,
       bn.start_time                                                                    as error_start_time,
       bn.end_time                                                                      as error_end_time,
       unix_timestamp(COALESCE(bn.end_time, sysdate())) - unix_timestamp(bn.start_time) as error_time,
       bn.alarm_level,
       bn.alarm_detail,
       bn.alarm_service,
       bn.warning_spec
from (select distinct robot_code,
                      error_id
      from (select robot_code, error_id, start_time
            from qt_smartreport.qt_day_robot_error_detail_his
            union all
            select robot_code, error_id, start_time
            from ({tb_day_robot_error_detail}) tb -- day_robot_error_detail.sql
           ) t) t1
         left join phoenix_basic.basic_notification bn on bn.id = t1.error_id
         left join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id