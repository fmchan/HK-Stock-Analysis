#!/bin/bash
PID=$(cat /usr/local/apps/stock-analysis/stock-app.pid)

if ! ps -p $PID > /dev/null
then
  rm -rf /usr/local/apps/stock-analysis/stock-app.pid
  nohup /root/miniconda3/envs/stock-analysis/bin/python3.6 /usr/local/apps/stock-analysis/app.py >> stock-app.log & echo $! >> /usr/local/apps/stock-analysis/stock-app.pid
fi

#*/1 * * * * /usr/local/apps/stock-analysis/restart_app.sh
