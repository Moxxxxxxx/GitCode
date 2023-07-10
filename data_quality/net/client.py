# -*- coding: UTF-8 -*-
import logging

'''
客户端，
输入参数，需要连接的server ip address, 
命令格式：python server-ip
'''

import sys,datetime,threading
from _socket import socket, AF_INET, SOCK_STREAM, gaierror

HOST = 'localhost'
PORT = 9999
BUFSIZ = 8192
SERIPADDRS = []

logger = logging.getLogger('')

def isopen(ip):
    tcpclisock = socket(AF_INET, SOCK_STREAM)
    try:
        tcpclisock.connect((ip, PORT))
        tcpclisock.shutdown(2)
        print(f'The IP is {ip}: {PORT} is open \n')
        return True
    except  Exception as err:
        logger.error(err)
        return False

class clientThread(threading.Thread):
    
    _CLASSNAME = 'net.Client.clientThread'
    StartTime = ''      #server统一执行时间
    
    def __init__(self, params, ip):
        threading.Thread.__init__(self)
        self.setDaemon(True)       
        self.params = params
        self.ip     = ip
        self.testresultdict = {}
        self.haveconnected  = False
    
    def run(self):  
        if isopen(self.ip):
            self.haveconnected = True
            self.connect(self.params, self.ip)
            
    def connect(self, params, host=HOST):
        logger.debug('start: connect', self._CLASSNAME)
        '''
        argv: concurrent!
        the first argument is ip address.
        the second argument is concurrent argument.
        '''
        tcpclisock = socket(AF_INET, SOCK_STREAM)
        try:
            tcpclisock.connect((host, PORT))
        except gaierror:
            logger.error('error ip address, ', host)
            sys.exit()
        except KeyboardInterrupt:
            sys.exit()
        except Exception:
            logger.error('server does not start or error!')
            sys.exit()
        if tcpclisock:
            print('have connected')
            self.senddata(tcpclisock, params)
        logger.debug('end: connect', self._CLASSNAME)
    
    def senddata(self, tcpclisock, params):
        testArgs = self.__class__.StartTime
        
        print('send data')
        tcpclisock.send("hello")
        data = tcpclisock.recv(BUFSIZ)
        result = tcpclisock.recv(BUFSIZ)
        logger.info(result)
        tcpclisock.close()

if __name__ == '__main__':

    clientThread.StartTime = str(datetime.datetime.now())

    try:
        ip = sys.argv[1]
        param = sys.argv[:]
        client = clientThread(param, ip)
        client.start()
        client.join()
    except KeyboardInterrupt:
        print('ctrl + c, exit')
        sys.exit()