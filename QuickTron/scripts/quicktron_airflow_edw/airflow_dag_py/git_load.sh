#!/bin/bash

current_time=`date "+%Y-%m-%d %T"`

echo "${current_time}........loading............"

# 进入git仓库的相应目录
cd /data/git/scripts

#检测当前的分支名称
curr_branch=$(git symbolic-ref --short -q HEAD)

#需要的分支目录
pub_branch="dev"

#判断当前分支是否，pull的分支
if [ ${curr_branch} != ${pub_branch} ]; then
   git checkout ${pub_branch} 
fi

# 拉取代码
git pull


# 判断sql目录是否存在
if [ ! -d "/data/quick_airflow/sql" ]
 then
 	ehco "dir not exists.......skip"
else
	echo "dir exists...........delete"
	
	rm -rf /data/quick_airflow/sql
fi


# 拷贝最新的git，sql目录到目标
cp -r /data/git/scripts/quicktron_airflow_edw/sql/ /data/quick_airflow/

echo "dir cp success..........."



echo "----------------------ftp传输文件到生产服务器start----------------------"


lftp -u quickftp,quicktron123456 58.34.1.4 <<EOF

cd /data/quick_airflow

command rm -rf sql/

lcd /data/quick_airflow

mirror -R sql/

by
EOF


echo "----------------------ftp传输文件到生产服务器end----------------------"
