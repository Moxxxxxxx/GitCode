import logging

import paramiko

DEFAULT_REMOTE_HOST = '172.31.237.5'
DEFAULT_REMOTE_PORT = '2208'
DEFAULT_REMOTE_USER = 'root'
DEFAULT_REMOTE_PWD  = 'asd1234567@'

logger = logging.getLogger('')

def excute_shell(script, remote_host, remote_port, remote_user, remote_pwd):
    excute_result = script
    err = ''
    try:
        # 新建一个ssh客户端对象
        ssh = paramiko.SSHClient()
        # 设置成默认自动接受密钥
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # 连接远程主机
        ssh.connect(remote_host, remote_port, remote_user, remote_pwd ,timeout=5)
        # 在远程机执行shell命令
        stdin, stdout, stderr = ssh.exec_command(script)
        # 读返回结果
        excute_result = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        # 在远程机执行python脚本命令
        # stdin, stdout, stderr = ssh.exec_command("python /home/test.py")
    except Exception as e:
        logger.error(e)
    return (excute_result, err)

def excute_shell_by_db(script, remote_ip_config=None):
    if remote_ip_config and len(remote_ip_config) > 2:
        return excute_shell(script, remote_ip_config[0], remote_ip_config[1], remote_ip_config[2], remote_ip_config[3])
    else:
        return excute_shell(script, DEFAULT_REMOTE_HOST, DEFAULT_REMOTE_PORT,  DEFAULT_REMOTE_USER, DEFAULT_REMOTE_PWD)

def getRemoteInfo(remoteip):
    return ''

if __name__ == '__main__':
    print(excute_shell('/data/report-center/data-collection/d2/device/opcode_212_06_01.sh')[0])


