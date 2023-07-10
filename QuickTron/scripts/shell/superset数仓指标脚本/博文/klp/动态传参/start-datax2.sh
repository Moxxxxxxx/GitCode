#!/bin/bash


#每天生成log日志文件，并删除一周前的文件
function write_log(){
 pre_date=`date +"%Y%m%d"`
 pre7_date=`date -d "-7 day" +"%Y%m%d"`
 log_dir_1=/home/datax/jsonjob/dataxlogs/datax-$pre_date.log
 log_dir_7=/home/datax/jsonjob/dataxlogs/datax-$pre7_date.log

 if [ ! -f "$log_dir_1" ];then  ## 判断是否存在文件，不存在则创建
  sudo touch $log_dir_1
  echo "create file $log_dir_1" 
 fi
 sudo chmod 777 $log_dir_1  ## log日志文件赋值

 if [ -f "$log_dir_7" ];then
 sudo  rm  $log_dir_7
 echo "delete file $log_dir_7"
 else
 echo "no file ,  no need delete"
 fi
 
#调用采集函数，执行命令
 read_dir $json_path $1 >> $log_dir_1

}



# json脚本参数
json_path=/home/datax/jsonjob/dataxjsons

##执行datax执行json命令
function read_dir(){
for file in `ls $1` #注意此处这是两个反引号，表示运行系统命令
do
 if [ -d $1"/"$file ] #注意此处之间一定要加上空格，否则会报错
 then
 read_dir $1"/"$file $2
 else
 echo "------------------------{start###$1"/"$file###start}-------------------------"
 sudo /home/datax/bin/datax.py -p "-Dproject_code='$2' -Dhost_port='58.34.1.38:3306'" $1"/"$file # 执行对应的datax采集对应脚本命令
 echo "------------------------{end###$1"/"$file###end}-------------------------" 
fi
done
}




### 开始任务并绑定定时任务
function start_job_crontab(){
 crontab_file=/home/datax/crontab_config
 if [ ! -f "$crontab_file" ];then #判断此文件是否存在，不存在则创建，并把crontable原定时任务和现有的采集定时任务都重新加上crontab
 sudo touch $crontab_file
 sudo chmod 777 $crontab_file
 sudo crontab -l > $crontab_file
 echo "30 0 * * * sudo sh /home/datax/start-datax.sh $1 > /dev/null 2>&1" >> $crontab_file
 sudo crontab $crontab_file

 sudo chmod 777 /home/datax/bin/*  ## 给bin下的所有文件赋上可执行权限 
 sudo chmod -R 766 $json_path/*  ## json脚本赋值可读可写权限
 fi
 ## 调用脚本
 write_log $1
}

## 开启函数
start_job_crontab $1


