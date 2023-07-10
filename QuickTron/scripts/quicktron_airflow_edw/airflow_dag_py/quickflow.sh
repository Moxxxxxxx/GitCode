#!/bin/bash

# 更改文件从dos 到unix格式
find /data/quick-airflow/sql -type f | xargs dos2unix -o

for host in 002.bg.qkt 003.bg.qkt
do
 echo =============== $host ===============
 ssh hadoop@$host "rm -rf /home/hadoop/wzm/sql/"
 scp -r /data/quick-airflow/sql/ hadoop@$host:/home/hadoop/wzm
done
