@echo off

set worker_name=jobtracker
call .\set_env.cmd
cd ../../
celery -A cherry worker --loglevel=info --pool=solo -n %worker_name% -Q cherry_job
cd .\deploy\windows