FROM python:3.8.2 AS uat
WORKDIR /monitor
RUN apt update
RUN apt install -y cron
RUN service cron start
RUN python -m pip install --upgrade pip
RUN pip install django pandas mysqlclient python-crontab sqlalchemy requests django-extensions django-werkzeug-debugger-runserver pyOpenSSL paramiko schedule
RUN pip install uwsgi gunicorn
COPY . .
COPY ./mysite/settings_uat.py ./mysite/settings.py
RUN mkdir log
EXPOSE 80
#CMD ["gunicorn", "mysite.wsgi", "-c", "gconfig.py"]
CMD ["/usr/local/bin/python", "manage.py", "runserver", "0.0.0.0:80"]
#CMD ["/usr/local/bin/python", "manage.py", "runserver", "runserver_plus", "--cert", "server.crt", "0.0.0.0:80"]
FROM python:3.8.2 AS prod
WORKDIR /monitor
RUN apt update
RUN apt install -y cron
RUN service cron start
RUN pip install django pandas mysqlclient python-crontab sqlalchemy requests django-extensions django-werkzeug-debugger-runserver pyOpenSSL paramiko schedule
COPY . .
COPY ./mysite/settings_prod.py ./mysite/settings.py
RUN mkdir log
EXPOSE 80
CMD ["/usr/local/bin/python", "manage.py", "runserver", "0.0.0.0:80"]
#CMD ["/usr/local/bin/python", "manage.py", "runserver", "runserver_plus", "--cert", "server.crt", "0.0.0.0:80"]
