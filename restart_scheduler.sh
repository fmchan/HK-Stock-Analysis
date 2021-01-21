#!/bin/bash
PID=$(cat /usr/local/apps/stock-analysis/app.pid)

if ! ps -p $PID > /dev/null
then
  rm -rf /usr/local/apps/stock-analysis/app.pid
  nohup /root/miniconda3/envs/stock-analysis/bin/python3.6 /usr/local/apps/stock-analysis/scheduler.py >> stock-scheduler.log & echo $! >> /usr/local/apps/stock-analysis/app.pid
fi

#*/1 * * * * /usr/local/apps/stock-analysis/restart_scheduler.sh
