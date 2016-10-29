cherry
===============

# Install
## Install on Ubuntu 14.04
You should have install `virtualenv` on your host.
```shell
$ pip install virtualenv

$ apt-get install redis-server
```

clone code:
```shell
$ git clone https://github.com/arthurchiao/cherry.git

$ cd cherry
```

create one virtual environment for developing:

```shell
$ virtualenv .venv
$ source .venv/bin/activate
```

install python packages:

```shell
$ pip install -r requirements.txt
```

## Install on Ubuntu 16.04
```shell
# for ubuntu 16.04
$ apt install virtualenv

$ apt install ffmpeg

$ apt install rabbitmq-server
```

# Run
## Run Local Job
```shell
$ cd cherry
$ source .venv/bin/activate

(.venv) $ python test_local_job.py
```

## Run Remote Job
worker node:
```shell
$ cd cherry
$ source .venv/bin/activate

(.venv) $ celery -A cherry --loglevel=info
```

master node:
```shell
$ cd cherry
$ source .venv/bin/activate

(.venv) $ python test_remote_job.py
```
