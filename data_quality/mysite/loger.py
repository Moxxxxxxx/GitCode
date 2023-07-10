# 读取日志配置文件内容
import logging

# 日志设置
import  logging
from logging import handlers
#
logger = logging.getLogger("test")
logger.setLevel(level=logging.DEBUG)

# formatter = logging.Formatter('%(t)s %(p)s %(h)s "%(r)s" %(s)s %(L)s %(b)s %(f)s" "%(a)s"')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
# stream_handler.setFormatter(formatter)

time_rotating_file_handler = handlers.TimedRotatingFileHandler(filename='dataquality.log', when='D')
time_rotating_file_handler.setLevel(logging.DEBUG)
# time_rotating_file_handler.setFormatter(formatter)

logger.addHandler(time_rotating_file_handler)
logger.addHandler(stream_handler)

if __name__ == "__main__":
    x = 111
    # logging.info( f"初始化 check_result_{x}表 ...完成")

    logger.info('*' * 50 + f"初始化 check_result_{x}表 ...完成" + '*' * 50)