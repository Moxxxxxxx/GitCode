import json
import logging

import requests

# global
from mysite import settings

try:
    logger = logging.getLogger()
except:
    print('logging.getLogger error')

'''
实现钉钉消息通知
'''

ROBOT_SESSION = settings.robot_session

#后续废弃
def dingding_notice(title, content='', link_url=''):
    dingding_notice(title, content, content, link_url)


def dingding_notice(title, template_name, template_desc, execute_content):
    r"""
    send markdown text notify to dingding.
    :param title:
    :param template_name:
    :param template_desc:
    :param execute_content:
    :return: True - send succeed.
             False - send failed.
    """
    bnotice_result = False
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    url = 'https://oapi.dingtalk.com/robot/send?access_token=' + ROBOT_SESSION
    content = "### {}({})\n> {}\n\n##### {}".format(title, template_name, template_desc, execute_content)
    _data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': title,
            'text': content
        }
    }
    try:
        data = json.dumps(_data)
        response = requests.post(url=url, data=data, headers=headers)
        result = response.json()
        logger.info(result)
        if result['errcode'] != 0:
            # todo 调用错误处理
            logger.info("result.errcode", result)
        else:
            bnotice_result = True
    except Exception as e:
        # todo 网络错误处理
        logger.error('occur error!', str(e))
    # 发送成功
    return bnotice_result

#test
if __name__ == '__main__':
    print(dingding_notice("数据核查预警", "check error ta", "某某数据超过10分钟没有同步。",
                    'find /data/tmp -mmin -20 -mmin +10 -name "*done.txt" | wc -l; !=; 0'))
