# -*- coding: utf-8 -*-
'''
jobtracker
author: huodahaha, zhangxusheng
date:2016/10/21
'''
from __future__ import absolute_import

import time
import json

from cherry.tasks.task_tracker import task_dict
from cherry.celery import Cherry_App
from celery import chain, group, chord


'''
这里使用了轮询的方式，而非celery内置的group或者chord，是因为group使用一直出现bug，如果能使用
group代替轮询的方式，会是一个比较优雅的做法。
使用group以及chord失败的残留代码如下

# task = group(filter_chain_s(segment) for segment in segments)
# try:
    # task = chord((chain(task_dict['task_upload'].s(i), task_dict['task_downlaod'].s()) for i in task_dict['task_slice'](to_slicer_in_str)), task_dict['task_merge'].s())
    # task = chord(filter_chain_s(segment) for segment in sgements)(task_dict['task_merge'].s())
    # task = group(chain(filter_chain_s(i)) for i in task_dict['task_slice'](to_slicer_in_str))
    # task = chord([filter_chain_s(segment[0])], task_dict['task_merge'].si(to_slicer_in_str))
    # async_r = []
    # for segment in segments:
    #     async_r.append(task_dict['task_upload'].s(segment))
    # g = group(*async_r)
    # pdb.set_trace()
    

# except Exception, e:
#     pdb.set_trace()

# task = group(chain(filter_chain_s(segment)) for segment in segments)
'''

@Cherry_App.task(name='Cherry.Task.Job_tracker')
def execute_quick_job(to_slicer_in_str):
    
    # generate filter chain
    filters = to_slicer_in_str['filters'].keys()
    filter_chain = []
    for filter_name in filters:
        filter_chain.append(task_dict[filter_name].s())
    filter_chain_s = chain(filter_chain)

    segments = task_dict['task_slice'](to_slicer_in_str)

    subtasks = []
    for segment in segments:
        ret = task_dict['task_upload'](segment)
        subtasks.append(filter_chain_s(ret))

    
    rets= []
    while len(subtasks) != 0:
        del_list = []
        for i in range(len(subtasks)):
            if subtasks[i].successful():
                # print "OK!"
                del_list.append(i)
                res = subtasks[i].get()
                rets.append(task_dict['task_download'](res))

        for i in reversed(del_list):
            del subtasks[i]

        time.sleep(0.2)
        print len(rets)

    ret = task_dict['task_merge'](rets)
    
    
def execute_normal_job(message_in_str):
    # generate filter chain
    filters = message_in_str['filters'].keys()
#     filter_chain = []
#     for filter_name in filters:
#         filter_chain.append(task_dict[filter_name].s())
#     filter_chain_s = chain(filter_chain)
    load_ret = task_dict['task_load'](json.dumps(message_in_str))
    upload_ret = task_dict['task_upload'](load_ret)
    process_ret = task_dict[filters[0]](upload_ret)
#     filter_chain_s.apply_async(to_filter_para_in_str = upload_ret)
    
#     while task.successful()==False:
#         time.sleep(0.5)
#         print "now processing"
#     
#     res = task.get()
    task_dict['task_download'](process_ret)
