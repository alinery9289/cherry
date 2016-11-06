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


"""
这里使用了轮询的方式，而非celery内置的group或者chord，是因为group使用一直出现bug，如果能使用
group代替轮询的方式，会是一个比较优雅的做法。
使用group以及chord失败的残留代码如下

# task = group(filter_chain_s(segment) for segment in segments)
# try:
    # task = chord((chain(task_dict['task_upload'].s(i),
    # task_dict['task_downlaod'].s()) for i in
    # task_dict['task_slice'](params)), task_dict['task_merge'].s())
    # task = chord(filter_chain_s(segment) for segment in sgements)(task_dict['task_merge'].s())
    # task = group(chain(filter_chain_s(i)) for i in
    # task_dict['task_slice'](params))
    # task = chord([filter_chain_s(segment[0])],
    # task_dict['task_merge'].si(params))
    # async_r = []
    # for segment in segments:
    #     async_r.append(task_dict['task_upload'].s(segment))
    # g = group(*async_r)
    # pdb.set_trace()


# except Exception, e:
#     pdb.set_trace()

# task = group(chain(filter_chain_s(segment)) for segment in segments)
"""


@celery_app.task(name='cherry.task.job')
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

        time.sleep(0.2)
        print len(cxts)

    context = task_dict['task_merge'](cxts)


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
