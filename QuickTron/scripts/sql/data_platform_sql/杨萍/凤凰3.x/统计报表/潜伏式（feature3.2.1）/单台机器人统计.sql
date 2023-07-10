
data1:机器人基础信息

select br.robot_code,                                                      #机器人编码
       case
           when brt.first_classification = 'WORKBIN' then '料箱车'
           when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
           when brt.first_classification = 'CARRIER' then '潜伏式'
           when brt.first_classification = 'ROLLER' then '辊筒'
           when brt.first_classification = 'FORKLIFT' then '堆高全向车'
           when brt.first_classification = 'DELIVER' then '投递车'
           when brt.first_classification = 'SC' then '四向穿梭车'
           else brt.first_classification end as first_classification_name, #机器人类型（一级分类）
       br.ip,                                                              #IP地址
       brt.robot_type_name,                                                #机器人类型名称
       brt.size_information,                                               #尺寸信息
       brt.noise,                                                          #噪音
       brt.no_load_rated_speed,                                            #空载额定速度
       brt.full_load_rated_speed,                                          #满载额定速度
       brt.battery_capacity,                                               #电池容量
       brt.battery_life,                                                   #电池寿命
       brt.battery_type,                                                   #电池类型
       brt.charger_port_type,                                              #充电接口类型
       brt.charging_time,                                                  #充电时间
       brt.rated_battery_life,                                             #额定电池寿命
       brt.ditch_capacity,                                                 #过沟能力
       brt.crossing_hom_capacity,                                          #过坎能力
       brt.crossing_slope_capacity,                                        #过坡能力
       brt.jacking_height,                                                 #顶升高度
       brt.self_weight,                                                    #自重
       brt.operating_temperature,                                          #使用温度
       brt.positioning_accuracy,                                           #定位精度
       brt.stop_accuracy,                                                  #停止精度
       brt.stop_angle_accuracy,                                            #停止角度精度
       brt.navigation_method                                               #导航方式
from phoenix_basic.basic_robot br
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where br.usage_state='using'
