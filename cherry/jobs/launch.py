# -*- coding: utf-8 -*-
"""
launch.py

Launch jobs, locally, or remotely.

author: huodahaha, zhangxusheng,
        Yanan Zhao <arthurchiao@hotmail.com>
date:2016/10/21
last modified: 2016-11-01 17:38:59
"""
from __future__ import absolute_import

import time
import json

from cherry.tasks.operators import loader,uploader,downloader
from cherry.tasks.tasks import task_dict
from cherry.celery import celery_app
from celery import chain, group, chord

@celery_app.task(name='cherry.task.sliced_job')
def launch_quick_job(context):
    """launch jobs through celery

    context: json-serialized parameters of the job
    """
    # generate filter chain
    context = json.loads(context)
    filters = context['filters'].keys()
    _chain = []
    for f in filters:
        _chain.append(task_dict[f].s()) # s(): celery subtask
    filter_chain = chain(_chain)

    sub_contexts = task_dict['task_slice'](json.dumps(context))

    subtasks = []
    for cxt in sub_contexts:
        cxt = task_dict['task_upload'](cxt)
        subtasks.append(filter_chain(cxt))

    cxts = []
    while subtasks:
        del_list = []
        for i in range(len(subtasks)):
            if subtasks[i].successful():
                # print "OK!"
                del_list.append(i)
                cxt = subtasks[i].get()
                cxts.append(task_dict['task_download'](cxt))

        for i in reversed(del_list):
            del subtasks[i]

        time.sleep(1)
        print len(cxts)

    context = task_dict['task_merge'](cxts)

@celery_app.task(name='cherry.task.intact_job')
def launch_normal_job(context):
    """
    context: json-serialized parameters of the job
    """
    filters = context['filters'].keys()
    print("filters: %s" % (filters))

    # load file to roam dir
    context = loader.load((json.dumps(context)))

    # upload to redis or ftp
    context = uploader.upload(context)

    # processing
    context = task_dict[filters[0]](context)

    # download processed file to local
    context = downloader.download(context)
