#!/bin/sh

worker_name=worker1
./set_env.sh
cd ../../
celery -A cherry worker --loglevel=info --pool=solo -n $worker_name -Q cherry_task_group1
