@echo off

set worker_name=JobTracker
call .\set_env.cmd

celery -A cherry worker --loglevel=info --pool=prefork -n %worker_name% -Q Cherry_Task_Group1