@echo off

set worker_name=worker1
call .\set_env.cmd

celery -A cherry worker --loglevel=info --pool=solo -n %worker_name% -Q Cherry_Task_Group1