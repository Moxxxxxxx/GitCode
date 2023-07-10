#!/bin/bash
echo off
THISDIR=/home/gm-data-quality-system
export PYTHONPATH=$PYTHONPATH:$THISDIR:$THISDIR/src
echo "start gm-data-quality-system"
#start gm-data-quality-system port 8000
#nohup gunicorn -w 6 -b 0.0.0.0:8000 gm-data-quality-system .wsgi:application>testgun.log&
nohup gunicorn mysite.wsgi -c /data/data-quality/gconfig.py &
echo "game over"
