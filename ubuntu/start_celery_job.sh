#!/bin/sh

worker_name=JobTracker
./set_env.sh

celery -A cherry worker --loglevel=info --pool=prefork -n $worker_name -Q Cherry_Task_Group1
