@echo off

set worker_name=worker3
call .\set_env.cmd
cd ../../
celery -A cherry worker --loglevel=info --pool=solo -n %worker_name% -Q cherry_task_group1
cd .\deploy\windows