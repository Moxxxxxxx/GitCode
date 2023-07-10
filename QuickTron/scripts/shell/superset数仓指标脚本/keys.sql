<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
PMS和DIC对接进程，什么时候DIC大表可以动起来

dic_get_dailyreport_data_maintable
dic_get_dailyreport_data_detailone
dic_get_dailyreport_data_detailtwo
dic_get_dailyreport_data_detailthree
这是dic日志采集的表，位置在172.31.237.5  db：quality_data

PMS项目基础数据大部分已采集入库，部分字段数据因PMS接口未提供，导致部分数据缺失，将可能影响整体PMS入库的进度。
具体如下：
1.缺少PMS用户映射表
2.接口部分字段有缺失
3.接口部分字段没有正确传递值

【PMS&DIC主题】：
1.现场日志（PE，劳务）接口采集完成
2.PMS人员信息表-正在采集
3.项目基础信息接口部分字段有缺失（接口提供数据小于页面展示数据）-待PMS处理
4.PMS项目经理计划接口-待与PMS交接

DIC项目大表逻辑迁移前提条件：
1.PMS项目基础信息数据完备
2.PMS项目计划时间节点数据完备
3.PMS用户信息数据完备

【工时主题（下周可排）】：
1.宝仓考勤数据接口采集（宝仓花名册&宝仓考勤）-勇军正在规划
2.宝仓数据入仓清洗-待分配（博文or莹莹）

【DIC&项目概览 详情页面 主题】：
1.机器人故障统计页面指标可行性分析-博文
2.机器人故障趋势统计页面指标可行性分析-博文
3.搬运作业单效率统计指标开发&前后端开发-（莹莹,少华,王亮）
4.搬运作业单分析统计指标开发&前后端开发-（莹莹,少华,王亮）
5.机器人故障统计页面指标开发&前后端开发-（莹莹,少华,王亮）
6.机器人故障趋势统计页面指标开发&前后端开发-（莹莹,少华,王亮）




需求：


【2022-10-18 会议纪要】
会议主题：3.0版本系统智能搬运指标
会议地点：鸿鹄
会议时间：10月18日 15:00--16:00
参会人员：道颠、朱春林、艾纯亮、谭如余、王伟、马婧、查博文

会议内容：
1. 需要提供现场业务数据导出脚本&导入入口--需求需要和海贤老师评审
2. 3.x效率统计中超时格位需要颜色标识
3. ETA机器学习预估搬运耗时功能
4. 需要有结论性的指标--待FT线给出具体评估维度
5. 门户项目概览页面点击漏斗图可以展示工单明细--需确定是否与页面scope吻合
6. 可根据配置阈值/同比进行告警，告警功能可开启可关闭
7. 需要有一个项目总表引导用户关注问题项目

Action:
1.智能搬运各位小伙伴先熟悉一下20环境的统计报表，后续再提出具体需求。
2.整体需求方向为：结论数据+异常告警。

导入导出功能
效率统计超时变颜色 阈值
ETA预估搬运耗时（机器学习）

没有结论性的东西，运维指导意见报告书？

点击漏斗图可以展示工单明细

自定义报表

可配数值和昨天对比（同比），功能可关闭/开启

需要一个大表，看哪个项目正常不正常

（结论 + 告警（通知））




=======
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
{
        "dingqo24cjhcersqtdj4": {
            "auth": {
                "appkey": "dingqo24cjhcersqtdj4",
                "CorpId": "ding224163effbb3c31bee0f45d8e4f7c288",
                "AgentId": "1033048531",
                "appsecret": "xFxfbTygxCNSYwfJKSEISxCz2OrMrtkdcPnB-ifmj-n-jFx9Fdxi6LZB96HhtmPa",
            },
            "meta": {
                "org_name": "快仓实施运维平台"
            }
        },
        "dingbbsgiedpcqgjnchd": {
            "auth": {
                "appkey": "dingbbsgiedpcqgjnchd",
                "appsecret": "hvZCaFrUQUxJtXFBCyPOfuVZnLtaXBiOqgKbF2ZRX43NbBE95HW2g-b9qMzfN2T6",
            },
            "meta": {
                "org_name": "上海快仓智能科技有限公司"
            }
        },
        "dingvjeyecucohc6zc8h": {
            "auth": {
                "appkey": "dingvjeyecucohc6zc8h",
                "appsecret": "W1GehmnO00vIGNqDeQ2-H6s0Afu94DhpKyHFr5s1NP9K6U3T8f2H1QI9kAGagqhZ",
                "AgentId": "1415755044",
                "CorpId": "dingf175640c69b2a5df35c2f4657eb6378f",
            },
            "meta": {
                "org_name": "上海快仓自动化科技有限公司"
            }
        },
        "dinggg50n5red3yw3q2x": {
            "auth": {
                "appkey": "dinggg50n5red3yw3q2x",
                "appsecret": "VMVX6mQQS2wS0_cNzX8heAurgOHFdDVME7oZrz1ikaVyc52qlEkbs-INkOgebCjR",
                "AgentId": "1415682700",
                "CorpId": "ding6ed12742d4c3bfc5a39a90f97fcb1e09",
            },
            "meta": {
                "org_name": "福建"
            }
        },
        "dingzngdctwqlu1hzc3j": {
            "auth": {
                "appkey": "dingzngdctwqlu1hzc3j",
                "appsecret": "LJy_B45Lhr_qHlMoYe2OvQWwK6SRswDdIaLe5xgRNsbJ6_8Exqqn1C1QfU-N9mEU",
                "CorpId": "ding80722f1b93084496f2c783f7214b6d69",
                "AgentId": "1415628913",
                "MiniAppId": "5000000001692368",
            },
            "meta": {
                "org_name": "深圳"
            }
        },
        "dingvwnge4eqrk5oibf9": {
            "auth": {
                "appkey": "dingvwnge4eqrk5oibf9",
                "appsecret": "jqk9KCBkn43VjAs8aOIEjWowXblVZC9OrJDaDO-mzx6Hb7PTMKIu5Wd4FKkUlhd6",
                "AgentId": "1415741267",
                "CorpId": "dingbf88bd774c5c9a5c35c2f4657eb6378f"
            },
            "meta": {
                "org_name": "宝仓"
            }
        },
        "dingvwnge4eqrk5oibf9": {
            "auth": {
                "appkey": "dingnsidsfwkrhgn8khx",
                "appsecret": "pCUl5JOtRE_9jv6JjXxw8iok6pEPrEMwtHnMbDAZqP2rIIj8HKVUwMqzbNAaVquG",
                "AgentId": "1818302883"
            },
            "meta": {
                "org_name": "宝仓-考勤"
            }
        },
    }
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
