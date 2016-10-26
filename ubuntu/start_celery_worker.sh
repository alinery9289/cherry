#!/bin/sh

worker_name=worker1
./set_env.sh

celery -A cherry worker --loglevel=info --pool=solo -n $worker_name -Q Cherry_Task_Group1
