# -*- coding: utf-8 -*-
'''
make task
author: huodahaha
date:2015/11/12
email:huodahaha@gmail.com
'''
from __future__ import absolute_import


from cherry.tasks.filters import filters_dict
from cherry.tasks.filters import (sync_transcoder, blank_filter)

from cherry.tasks.basic_module import (slicer, downloader, uploader, merger)

from celery import Celery

from cherry.celery import Cherry_App

# upload,slice,download,merge直接由job_traker调用
# celery的异步任务只有filter类

def task_upload(to_uploader_para_in_str):
    uploader_instance = uploader()
    return uploader_instance.upload(to_uploader_para_in_str)

def task_slice(to_slicer_para_in_str):
    slicer_ins = slicer()
    return slicer_ins.slice(to_slicer_para_in_str)

def task_download(to_downloader_para_in_str):
    downloader_instance = downloader()
    return downloader_instance.download(to_downloader_para_in_str)

def task_merge(to_merger_para_in_str):
    merger_instance = merger()
    return merger_instance.merge(to_merger_para_in_str)

def generate_filter_task():
    task_dict = {}
    global filters_dict    

    for filter_name in filters_dict:

        @Cherry_App.task(name='Cherry.Task.' + filter_name)
        def filter_task(to_filter_para_in_str):
            anonymity_filter_instance = filters_dict[filter_name]()
            return anonymity_filter_instance.do_process_main(to_filter_para_in_str)
        task_dict[filter_name] = filter_task

    return task_dict
        
task_dict = generate_filter_task()


# 以下为迭代算法

# group_num = check_from_DB(filters_dict)


# @Cherry_App.task(name='Cherry.Task.iteration_'+str(group_num))
# def filter_task(to_filter_para_in_str, filters):
#     for filter_i in filters:
#         to_filter_para_in_str = task_dict(filter_i)(to_filter_para_in_str)
#     return to_filter_para_in_str


# def check_from_DB(filters_dict):
#     '''
#     根据filters_dict 查找数据库，是否存在group，如果存在返回group_num,如果不存在，insert，生成一个新的group_num并返回
#     '''
#     pass

#     return group_num


task_dict['task_slice'] = task_slice
task_dict['task_merge'] = task_merge
task_dict['task_upload'] = task_upload
task_dict['task_download'] = task_download