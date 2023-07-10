# INSERT INTO `data_quality`.`check_result_template` (`id`, `source_system`, `check_item`, `target_table`, `risk_market_item`, `problem_type`, `check_sql`, `check_code`, `problem_id`, `item_count`, `problem_count`, `problem_per`, `db`, `note`, `status`, `update_flag`, `check_date`, `check_version`) VALUES ('2', '', NULL, 'game2', NULL, '一致性检验', 'select *from ', NULL, NULL, NULL, NULL, NULL, 'data_quality', NULL, b'1', 'N', NULL, '1');
from mysite import db_config

def initData():

    conn = db_config.mysql_connect()
    curs = conn.cursor()
    curs.execute('set autocommit=0')
    # 计算问题占比
    # 处理item_count和problem_count都是null或=0的行
    sql = f"""update check_result_{productname}
                            set problem_per=100
                            where (item_count is null or item_count=0)
                            and check_version={version}"""
    curs.execute(sql)

    # 计算正常的问题占比
    sql = f"""update check_result_{productname} set problem_per=problem_count/item_count*100\
                            where problem_per is null
                            and check_version={version}"""
    curs.execute(sql)
    conn.commit()

if __name__ == "__main__":
    initData()