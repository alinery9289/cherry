FROM ffmpeg_ubuntu16.04:2016.11.01

MAINTAINER Yanan Zhao <arthurchiao@hotmail.com>

ADD ../../cherry /
RUN apt update
RUN apt install -y python-pip
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN apt install redis-server

RUN mkdir /etc/cherry
RUN mkdir /etc/cherry/templates
RUN cp cherry/ubuntu/conf /etc/cherry/.
RUN cp cherry/ubuntu/templates/xx.sh /etc/cherry/templates/.
RUN mkdir /cherry-run
RUN mkdir /cherry-run/video_files
