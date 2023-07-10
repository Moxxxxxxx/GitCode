#!/bin/bash
docker ps -a | grep "data_quality" | awk '{print $1 }'|xargs docker stop |xargs docker rm

docker images|grep data_quality|awk '{print $3 }'|xargs docker rmi
#编译新镜像
docker build -t data_quality:v1.0 .


#读取配置环境变量，生成新的docker执行命令

docker run  -p 8000:8000 --name data_quality -v $PWD/log:/data_quality/log -d data_quality:v1.0
#查看容器启动是否正常
docker ps -a |grep data_quality
