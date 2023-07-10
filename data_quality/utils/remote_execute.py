import json
import logging
from urllib.parse import quote
import requests

# global

'''
实现远程主机执行shell命令
'''
logger = logging.getLogger('')

def execute_by_agent(agent_url, cmd):
    """ 远程执行命令

    :param agent_url: agent访问接口
    :param cmd: 要指定的命令
    :return: 执行的结果（stdout）

    throw exception: 出现网络错误将抛出异常。
    """
    url = agent_url + "?cmd=" + quote(cmd, 'utf-8')
    logger.info("start execute by agent. request[{}]".format(url))
    result = requests.get(url)
    content = result.text.strip()
    logger.info("end execute by agent. response[{}]".format(content))
    return content

if __name__ == '__main__':
    print(execute_by_agent("http://localhost:8080", "find /data/h5-x8 -mmin -15 -mmin +1 -name \"*.txt\" | wc -l"))
