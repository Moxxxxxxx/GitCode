FROM python:3.9.5

#设置时区
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

RUN mkdir -p data_quality

COPY . data_quality

WORKDIR data_quality
COPY crontab /usr/bin
RUN ls -h
RUN  /usr/local/bin/pip3  install -r requirements.txt -i https://pypi.doubanio.com/simple

#ENTRYPOINT ["/bin/sh","-c","python3 -u manage.py runserver 0.0.0.0:8000"] runserver_plus --cert server.crt
#RUN python monitor/start_schedule_task.py
RUN python manage.py migrate

#ENTRYPOINT ["/bin/sh","-c","gunicorn mysite.wsgi -c ./gconfig.py"]
#ENTRYPOINT ["/bin/sh","-c","python3 -u manage.py runserver 0.0.0.0:8000"]
ENTRYPOINT ["/bin/sh","-c","python3 -u manage.py runserver_plus --cert server.crt --key-file server.key 0.0.0.0:8000"]
