
# demo
http://ip:8000
登录用户名密码：admin/123456

# 项目结构
```
项目
│  gconfig.py           gunicorn配置文件
│  manage.py            Django管理文件
│  README.md            readme
|  nginx.conf           nginx.conf
│
├─api                   ajax接口
│
├─authorize             身份认证模块
|
├─check                 自动检核模块
|
├─data                  检核明细模块
|
├─docs                  文档目录
│
├─files                 上传下载文件模块
│
├─logs                  日志目录
|
├─mysite                Django配置目录
│
├─standard              查看、更新数据标准模块
|
├─utils                 一些工具
│
└─static                css、js、附件等静态文件目录
```



# 启停项目
```

# 启动项目
gunicorn mysite.wsgi -c gconfig.py &

```
详细如下：

系统基于安装py3.6及其以上版本运行或不用docker 跑的话，需要手动安装以下
一、需要安装的组件如下：
1、install pip
3、pip install django 3.2.6
5、pip install python-cronTab  2.5.1
6、pip install sqlalchemy  1.4.22
7、pip install pandas 1.3.1
8、pip install uwsgi 2.0.15
9、pip install  django
10、pip install gunicorn 20.1.0
11、pip install requests 2.22.0
12、pip install paramiko 2.7.2
# pip install pycrypto
13、pip install schedule 0.6.0
#支持SSL
pip install django-extensions 3.1.3
pip install django-werkzeug-debugger-runserver 0.3.1
pip install pyOpenSSL 20.0.1
# all in one
pip3 install django pandas  python-crontab sqlalchemy requests django-extensions django-werkzeug-debugger-runserver pyOpenSSL paramiko
pip install MySQL-python 1.2.5
二、开发工具
1、ideal
2、调试可以启用python manage.py runserver_plus --cert server.crt 0.0.0.0:8000

三、系统初始化
1、初始化sql  sql/init.sql  [业务表]
2、python manage.py migrate 【系统自带】
3、创建管理员
python manage.py createsuperuser

四、启动
1、gunicorn mysite.wsgi -c gconfig.py &
2、http://ip:port


注意事项：
若打开首页：无图片
请运行python manage.py collectstatic 

pip install MySQL-python 1.2.5