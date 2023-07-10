# -*- coding: UTF-8 -*-
'''
@author: water
'''
from mysite.loger import logging
import sys
import threading
import time
from _socket import socket, AF_INET, SOCK_STREAM


HOST = ''
PORT = 9999
BUFSIZ = 2048

class ExcuteThread(threading.Thread):

    def __init__(self, script):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.script = script


    def run(self):
        self.excute()

    def excute(self):
        pass


class acceptThread(threading.Thread):

    _CLASSNAME = 'acceptThread'

    '''
    '''
    def __init__(self, tcpsersock):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.tcpclisock = ''
        self.addr       = ''
        self.tcpsersock = tcpsersock

    def run(self):
        try:
            while True:
                self.tcpclisock, self.addr = self.tcpsersock.accept()
        except:
            sys.exit()

    def getTcpsock(self):
        tcpclisock = self.tcpclisock
        addr       = self.addr
        self.tcpclisock = None
        self.addr       = None
        return tcpclisock, addr


class Server(object):
    
    _CLASSNAME = 'server.Server'
    
    def startServer(self, port=PORT):
        '''启动server'''
        tcpsersock = socket(AF_INET, SOCK_STREAM)
        try:
            tcpsersock.bind(('', port))
        except:
            logging.error('socket error [errno 10048]')
            sys.exit()
        tcpsersock.listen(10)
        acceptTh = acceptThread(tcpsersock)
        print('waiting for connection>>>')
        try:
            acceptTh.start()
            while True:
                tcpclisock, addr = acceptTh.getTcpsock()
                if not tcpclisock:
                    time.sleep(2)
                    continue
                print('...connected from:' + addr)
                #接收数据
                while True:
                    if tcpclisock:
                        data = tcpclisock.recv(BUFSIZ)
                    if not data or data == '':
                        break
                    time.sleep(1)
                    tcpclisock.send('server receive data ok,server will start test')
                    try:
                        if tcpclisock:
                            tcpclisock.send("bye")
                            break
                    except:
                        break
                tcpclisock.close()
            tcpsersock.close()
        except KeyboardInterrupt:
            self.exit([tcpclisock, tcpsersock])

    def exit(self, closeobj):
        for obj in closeobj:
            if obj:
                obj.close()
        sys.exit(0)

if __name__ == '__main__':
    port = PORT
    if len(sys.argv) == 2:
        port = sys.argv[1]
    print('start server')
    ser = Server()
    ser.startServer(port)