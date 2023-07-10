# coding=utf-8

import commands
import datetime
import MySQLdb

print  "staring gitstats code......."

(ds,do) = commands.getstatusoutput('date +%Y-%m-%d')
curent_date = do

(status, output) = commands.getstatusoutput('find  /opt/gitlab/data/repositories -name *.git')
if status == 0:
   gitrepo_list = output.split('\n')


fo = open("./config.txt", "r+")

db = MySQLdb.connect("172.31.238.14", "root", "flashhold#123", "devops", charset='utf8')

rdsdb = MySQLdb.connect("rm-uf6m8025g360143qfto.mysql.rds.aliyuncs.com", "superuser", "7YcVuA04JJTm", "devops", charset='utf8')

rdsdbcd = MySQLdb.connect("rm-uf6m8025g360143qfto.mysql.rds.aliyuncs.com", "superuser", "7YcVuA04JJTm", "zentao", charset='utf8')

global config_date

config_date = fo.readline().strip()
print config_date

#git log --format='%aN' | sort -u | while read name; do echo -en "$name\t"; git log --since =="2017-10-11" --until="2020-11-18"  --author="$name" --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }' -; done

while config_date < curent_date:

   since_date = config_date + " 00:00:01"

   until_date = config_date + " 23:59:59"

   for repo_git in gitrepo_list:
      git_stats_cmd01 = '''cd {0} && git log --all --format='%aN' | sort -u | while read name; do echo -en "$name\\t"; git log --all --no-merges --since =="{1}" --until="{2}"  --author="$name" --pretty=tformat: --numstat'''.format(repo_git,since_date,until_date)

      git_stats_cmd = git_stats_cmd01 + '''| awk '{ add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\\n", add, subs, loc }' -; done '''
      print git_stats_cmd
      (status, cmd_rs_output) = commands.getstatusoutput(git_stats_cmd)
      if "fatal" in cmd_rs_output:
         continue
      cmd_rs_list = cmd_rs_output.split('\n')
      for  cmd_rs in cmd_rs_list:
           mysql_date = config_date
           mysql_repo = '/'.join(repo_git.split('/')[5:])
           mysql_author = cmd_rs.split('\t')[0]
           mysql_add_lines = cmd_rs.split(',')[0].split(':')[1].strip()
           if mysql_add_lines == '':
              mysql_add_lines = 0
           mysql_removed_lines = cmd_rs.split(',')[1].split(':')[1].strip()
           if mysql_removed_lines == '':
              mysql_removed_lines = 0
           mysql_total_lines = cmd_rs.split(',')[2].split(':')[1].strip()
           if mysql_total_lines == '':
              mysql_total_lines = 0
           #mysql_sql = """INSERT INTO app_gitstats(ctime,repo,author,add_lines,removed_lines,total_lines) VALUES ('%s','%s','%s',%s,%s,%s) ON DUPLICATE KEY UPDATE add_lines=VALUES(add_lines),removed_lines=VALUES(removed_lines),total_lines=VALUES(total_lines)"""  %(mysql_date,mysql_repo,mysql_author,mysql_add_lines,mysql_removed_lines,mysql_total_lines)
           mysql_sql = """INSERT INTO app_gitstats(ctime,repo,author,add_lines,removed_lines,total_lines) VALUES ('%s','%s','%s',%s,%s,%s)"""  %(mysql_date,mysql_repo,mysql_author,mysql_add_lines,mysql_removed_lines,mysql_total_lines)
           print mysql_sql
           cursor = db.cursor()
           try:
               cursor.execute(mysql_sql)
               db.commit()
           except Exception as e:
               print "error:%s" %(str(e))
               db.rollback()
           ###rsyc rdsdb
           rdscursor = rdsdb.cursor()
           try:
               rdscursor.execute(mysql_sql)
               rdsdb.commit()
           except Exception as e:
               print "error:%s" %(str(e))
               rdsdb.rollback()
           ####rsyc rdsdbcd
           rdscdcursor = rdsdbcd.cursor()
           try:
               rdscdcursor.execute(mysql_sql)
               rdsdbcd.commit()
           except Exception as e:
               print "error:%s" %(str(e))
               rdsdbcd.rollback()




   dateTime_p = datetime.datetime.strptime(config_date,'%Y-%m-%d')
   datedelta = dateTime_p + datetime.timedelta(days=1)
   config_date = datetime.datetime.strftime(datedelta,'%Y-%m-%d')

#(status, cmd_rs_output) = commands.getstatusoutput("cat /dev/null > ./config.txt")

#print str(config_date).strip()
#fo.write(config_date.strip())
fo.close()
db.close()
rdsdb.close()

(status, cmd_rs_output) = commands.getstatusoutput("rm -f ./config.txt")

(status, cmd_rs_output) = commands.getstatusoutput("echo '%s' >> ./config.txt" %(config_date))
