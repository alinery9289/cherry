#!/bin/bash

sudo docker run -d -it --name test_cherry \
    --net host \
    -v /home/cloud/opensource/arthurchiao/cherry:/cherry \
    cherry_ubuntu16.04:0.0.1 bash
