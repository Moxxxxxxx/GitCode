#!/usr/bin/python
#coding=utf-8
import sys
import MySQLdb
import commands
files="/opt/gitlab/data/authorinfo/authorinfo.txt"
(status, cmd_rs_output) = commands.getstatusoutput("rm -f %s" %(files))

def db_mysql(file):

        conn = MySQLdb.connect('172.31.237.5','root', 'tClEDdt6', 'quality_data',3306, charset='utf8')
        print "写入中，请等待..............................."
        cur=conn.cursor() # 获取cursor对象
        sql="""select email from dingtalk_user_info where email is not null"""
        cur.execute(sql)
        conn.commit()
        row = cur.fetchall() # 获取结果及的所有数据
        fp=open(file,"w")
        loan_count=0
        for rowNumber in row:
        	loan_count += 1
        	fp.write(str(rowNumber[0]) + "\n")
        fp.close()
        cur.close()
        conn.close()
        print "读取钉钉数据库员工信息完成,共写入%s条数据" %(loan_count)
        (status, cmd_rs_output) = commands.getstatusoutput("chmod 777 %s" %(files))
db_mysql(files)

