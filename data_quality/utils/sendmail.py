# -*- coding: UTF-8 -*-
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import os
import smtplib



def genAttchment(attachment, name='report.html'):
    att = MIMEText(open(attachment, 'rb').read(), 'html', 'utf-8')  
    att["Content-Type"] = 'application/octet-stream'  
    att["Content-Disposition"] = 'attachment; filename=%s'%name 
    return att


def sendmail(subject, text, attachment1=None, attachment2=None, receiver='zhangzhijun@falshhold.com'):
    sender =  'zhangzhijun@falshhold.com'
    smtpserver = 'smtp.falshhold.com'
    username = 'zhangzhijun@falshhold.com'
    password = 'fffff'
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    content = MIMEText('<b>'+ text + '</b>', 'html')  
    msgRoot.attach(content) 
    if attachment1 and os.path.exists(attachment1):
        msgRoot.attach(genAttchment(attachment1))
    if attachment2 and os.path.exists(attachment2):
        msgRoot.attach(genAttchment(attachment2, 'cpu-linechart.png'))
    smtp = smtplib.SMTP_SSL()
    try:
        smtp.connect(smtpserver)
    except:
        print('can not connect')
        return
    smtp.login(username, password)
    if receiver.find('@') > -1:
        receivers = receiver.split(';')
    else:
        return
    smtp.sendmail(sender, receivers, msgRoot.as_string())  
    print('send ok, please check your mail')
    #time.sleep(10)
    smtp.quit()

if __name__ == '__main__':
    sendmail(' report', 'This E-mail sent  platform!', \
             attachment1 = 'E:\\report_demmo_0.html', \
             attachment2 = 'E:\\de.png')