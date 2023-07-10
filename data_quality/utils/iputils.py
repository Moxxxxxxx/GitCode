# -*- coding: UTF-8 -*-
import logging

logger = logging.getLogger('')

def get_local_ip():
    import socket
    myaddr = ''
    try:
        myname = socket.getfqdn(socket.gethostname())
        myaddr = socket.gethostbyname(myname)
        ipList = socket.gethostbyname_ex(socket.gethostname())
        myaddr += ' ' + ipList[0]
    except Exception as err:
        logger.error(err)
    return myaddr

if __name__ == '__main__':
    print(get_local_ip())