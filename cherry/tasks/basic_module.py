# -*- coding: utf-8 -*-
'''
basic module
author: huodahaha, zhangxusheng
date:2016/10/14
'''

from __future__ import absolute_import

import json
import os
# import logging.handlers
import redis
import hashlib
import time
import shutil


from cherry.util import MyRoam
from cherry.util.MyExceptions import (FFmpegExecuteError, GPACExecuteError, InternalError)
from cherry.util.MyConfig import conf_dict
from cherry.util.ftptool import MYFTP
from cherry.util.logtool import Pro_log_main


# log_path = os.getenv('DISTRIBUTED_TRANSCODER_LOG')
# LOG_FILE = log_path + '\\' + 'log.log'
#
# handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 5) # init a handler instance
#
# fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(funcName)s - %(message)s'
# formatter = logging.Formatter(fmt)           # init a formatter instance
# handler.setFormatter(formatter)              # register formatter
#
# logger = logging.getLogger('log')    # get logger named 'log'
# logger.setLevel(logging.DEBUG)
# logger.addHandler(handler)                   # register handler


# @singleton deocorator
class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance

class base_module(object):
    def __init__(self):
        # read enviroment variable

        self.roam_path = os.getenv('DISTRIBUTED_TRANSCODER_ROAM')
        if not os.path.exists(self.roam_path):
            raise IOError('Could not open roam directory')

        # read conf file
        self.redis_IP = conf_dict['redis']['ip']
        self.redis_port = conf_dict['redis']['port']
        self.redis_connection = redis.Redis(host=self.redis_IP, port = self.redis_port, db = 0)

        self.ftp_hostaddr = conf_dict['ftp']['hostaddr']
        self.ftp_port = conf_dict['ftp']['port']
        self.ftp_username =  conf_dict['ftp']['username']
        self.ftp_password =  conf_dict['ftp']['password']
        self.rootdir_remote =  conf_dict['ftp']['rootdir_remote']
        self.ftp_connection = MYFTP(self.ftp_hostaddr, self.ftp_username, self.ftp_password, self.rootdir_remote, self.ftp_port)
        self.transfer_method = conf_dict['all']['transfer_method']

        if (self.transfer_method == 'redis'):
            self.download_file = self.redis_download_file
            self.upload_file = self.redis_upload_file

        elif (self.transfer_method == 'ftp'):
            self.download_file = self.ftp_download_file
            self.upload_file = self.ftp_upload_file

        else:
            raise ValueError('could not recognise transfer_method: %s'%self.transfer_method)

        # get logger handler
#         self.logger = logging.getLogger('log')

    def redis_get(self, redis_key):
        return self.redis_connection.get(redis_key)

    def redis_set(self, key, data):
        return self.redis_connection.set(key, data)

    def redis_del(self, key):
        return self.redis_connection.delete(key)

    def redis_is_exist(self, key):
        return self.redis_connection.exists(key)

    def redis_download_file(self, key, local_name):
        data = self.redis_get(key)
        with open(local_name, 'wb') as f:
            f.write(data)

    def redis_upload_file(self, key, local_name):
        with open(local_name,'rb') as f:
            data = f.read()
            self.redis_set(key, data)

    def ftp_download_file(self, remote_name, local_name): #remote_name: /xxx/xxx.video local_name: before.video
        print 'local_name:'+ local_name
        self.ftp_connection.login()
        self.ftp_connection.download_file(local_name,remote_name)

    def ftp_upload_file(self, remote_name, local_name): #remote_name: /xxx/xxx.video local_name: after.video
        print 'local_name:'+ local_name
        self.ftp_connection.login()
        self.ftp_connection.upload_file(local_name,remote_name)

    def ask_for_a_new_process_task_ID(self):
        md5_generator = hashlib.md5()
        time_str = str(time.time())
        while True:
            # randomly change the str in case the hash collision
            key_str = time_str + 'any_salt'
            md5_generator.update(key_str)
            key = md5_generator.hexdigest()
            # pdb.set_trace()
#             if not self.is_exist(key):
                # generate a 8 bit hash code
            return key

class base_filter(base_module, Singleton):
    def __init__(self):
        super(base_filter, self).__init__()
        self.before_name = 'before.mp4'
        self.after_name = 'after.mp4'
        self.filter_name = self.get_filter_name()

    def __parameter_decode(self, to_filter_para_in_str):
        to_filter_parameter_in_json = json.loads(to_filter_para_in_str)
#         to_filter_parameter_in_json['filters'] = json.loads(to_filter_parameter_in_json['filters'])

        filter_parameter = to_filter_parameter_in_json['filters'][self.filter_name]
        data_key = to_filter_parameter_in_json['data_key']
        process_task_ID = to_filter_parameter_in_json['process_task_ID']
        return filter_parameter, data_key, process_task_ID


    def get_filter_name(self):
        return self.__class__.__name__


    def filter_foo(self, process_task_ID, filter_parameter):
        pass


    def do_process_main(self, to_filter_para_in_str):
        print  self.filter_name
        with MyRoam.temporary_env(self.roam_path):
            # check the parameter passed by the upper layer
            filter_parameter, src, process_task_ID = self.__parameter_decode(to_filter_para_in_str)
            # write log
            main_log = Pro_log_main()
            main_log.info("Begin to download task from job tracker, taskid is "+process_task_ID+".")

            # download the segment need to be transcoded
            self.download_file(src, self.before_name)

            main_log.info("Begin to process task, taskid is "+process_task_ID+".")
            # do the filter process the segment with FFmpeg
            self.filter_foo(process_task_ID, filter_parameter)

            # write status
            main_log.info("Begin to upload task to job tracker, taskid is "+process_task_ID+".")
            # upload the transcoded segment
            self.upload_file(src, self.after_name)

            main_log.info("Upload task to job tracker ok, taskid is "+process_task_ID+".")
            return to_filter_para_in_str

class uploader(base_module, Singleton):
    def __init__(self):
        super(uploader, self).__init__()
        # self.logger.log('start upload !!')

    def upload(self, to_uploader_para_in_str):
        to_uploader_para_in_json = json.loads(to_uploader_para_in_str)
        process_task_ID = to_uploader_para_in_json['process_task_ID']
        data_key = process_task_ID + '.' + to_uploader_para_in_json['segment_file_name'] + '.data'
        file_name = self.roam_path + '\\' + process_task_ID + '\\before\\' + to_uploader_para_in_json['segment_file_name']


        self.upload_file(data_key, file_name)
            # self.dump_to_storage(return_key, json.dumps({'filters':None}))

        to_uploader_para_in_json['data_key'] = data_key
        # to_uploader_para_in_json['return_key'] = return_key
        # self.logger.log('upload success task_ID:%s!! ' % process_task_ID)
        return json.dumps(to_uploader_para_in_json)

class downloader(base_module, Singleton):

    def __init__(self):
        super(downloader,self).__init__()
        # self.logger.log('start download!!')


    def download(self, to_downloader_para_in_str):
        to_downloader_parameter_in_json = json.loads(to_downloader_para_in_str)
        data_key = to_downloader_parameter_in_json['data_key']
        # return_key = to_downloader_parameter_in_json['return_key']
        process_task_ID = to_downloader_parameter_in_json['process_task_ID']
        data_file_name = self.roam_path + '\\' + process_task_ID + '\\after\\data\\' + to_downloader_parameter_in_json['segment_file_name']
        # return_file_name = self.roam_path + '\\' + process_task_ID + '\\after\\return\\' + to_downloader_parameter_in_json['segment_file_name']
        self.download_file(data_key, data_file_name)
        # self.download_file(return_key, return_file_name)

        self.redis_del(data_key)
        # self.redis_del(return_key)

        del to_downloader_parameter_in_json['data_key']
        # del to_downloader_parameter_in_json['return_key']
        to_downloader_parameter_in_json['data_file_name'] = self.roam_path + '\\' + process_task_ID + '\\after\\data'
        to_downloader_parameter_in_json['return_file_name'] = self.roam_path + '\\' + process_task_ID + '\\after\\return'
        # self.logger.log('download success task_ID:%s!! ' % process_task_ID)
        return json.dumps(to_downloader_parameter_in_json)

class slicer(base_module, Singleton):
    def __init__(self):
        # pdb.set_trace()
        super(slicer, self).__init__()


    def __execute_FFmpeg(self, file_name):
        # logger.debug('__execute_FFmpeg(%s)'%file_name)
        s = 'ffmpeg -i %s -f segment -segment_time 10 -c copy -map 0 before\\segment%%d.mp4' % file_name
        ret = os.system(s)
        if ret != 0:
            raise FFmpegExecuteError('FFmpeg execute error: %s'%s)
        else:
            return os.listdir(os.getcwd() + '//' + 'before')


    def slice(self, to_slicer_para_in_str):
        task_ID = self.ask_for_a_new_process_task_ID()
        self.redis_set(task_ID, 'in progress')
        with MyRoam.temporary_env(self.roam_path, director_given_name = task_ID, generate_hash_dir = True, del_roam_data = False):
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            to_slicer_para_in_json = json.loads(to_slicer_para_in_str)
            # segment the file to segments\
            segments = self.__execute_FFmpeg(to_slicer_para_in_json['input_file_path'])

            # generate to_uploader_para_in_json list
            to_uploader_para_in_json = map(lambda segment: {'filters':json.dumps(to_slicer_para_in_json['filters']),
                                                            'output_file_path':to_slicer_para_in_json['output_file_path'],
                                                            'process_task_ID':task_ID,
                                                            'segment_file_name':segment}, segments)

            to_uploader_para_in_str = map(json.dumps, to_uploader_para_in_json)
            return to_uploader_para_in_str

class merger(base_module):
    def __init__(self):
        super(merger, self).__init__()


    def __index_of_segment(self, segment_file_name):
        if segment_file_name[0:7] != 'segment' or segment_file_name[-4:] != '.mp4':
            raise IOError('segment file name unstandarded: %s'%segment_file_name)
        else:
            return int(segment_file_name[7:-4])

    def __execute_MP4Box(self, output_file_path):
        # pdb.set_trace()
        segments = os.listdir(os.getcwd())
        if len(segments) == 0:
            raise InternalError('segments_num is not right')
        segments = sorted(segments, key = self.__index_of_segment)
        s = 'MP4Box '
        for segment in segments:
            s += '-cat ' + segment + ' '
        s += '-new ' + output_file_path
        ret = os.system(s)
        if ret != 0:
            raise GPACExecuteError('GPAC execute error: %s'%s)

    def merge(self, *args, **kargs):
        # pdb.set_trace()
        to_merger_parameter_in_json = json.loads(args[0][0])
        roam_path = to_merger_parameter_in_json['data_file_name']
        output_file_path = to_merger_parameter_in_json['output_file_path']
        task_ID = to_merger_parameter_in_json['process_task_ID']
        with MyRoam.temporary_env(root_path = roam_path, director_given_name = task_ID, generate_hash_dir = False, del_roam_data = False):
            self.__execute_MP4Box(output_file_path)

        os.system('rd /q /s ' + self.roam_path + '\\' + task_ID)

class loader(base_module, Singleton):
    def __init__(self):
        super(loader, self).__init__()

    def load(self, to_loader_para_in_str):
        task_ID = self.ask_for_a_new_process_task_ID()
        self.redis_set(task_ID, 'in progress')
        with MyRoam.temporary_env(self.roam_path, director_given_name = task_ID, generate_hash_dir = True, del_roam_data = False) as roam_context:
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            to_loader_para_in_json = json.loads(to_loader_para_in_str)
            to_loader_para_in_json['process_task_ID'] = task_ID
            shutil.copyfile(to_loader_para_in_json['input_file_path'], self.roam_path + '\\' + roam_context.roam_path+"\\before\\segement.mp4")
            to_loader_para_in_json['segment_file_name'] = 'segement.mp4'
            to_uploader_para_in_str = json.dumps(to_loader_para_in_json)
            return to_uploader_para_in_str
