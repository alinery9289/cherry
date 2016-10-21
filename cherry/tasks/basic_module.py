"""
basic module

author: huodahaha, zhangxusheng, Yanan Zhao
date:2016/10/14
"""

from __future__ import absolute_import

import json
import os
import redis
import hashlib
import time
import shutil


from cherry.util.roam import TmpEnv
from cherry.util.exceptions import (
    FFmpegExecuteError,
    GPACExecuteError,
     InternalError)
from cherry.util.config import conf_dict
from cherry.util.ftptool import FtpClient


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


class Singleton(object):
    """Singleton decorator
    """
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance


class ModuleBase(object):

    def __init__(self):
        # read enviroment variable

        self.roam_path = os.getenv('DISTRIBUTED_TRANSCODER_ROAM')
        if not os.path.exists(self.roam_path):
            raise IOError('Could not open roam directory')

        # read conf file
        self.redis_host = conf_dict['redis']['ip']
        self.redis_port = conf_dict['redis']['port']
        self.redis_connection = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=0)

        self.ftp_hostaddr = conf_dict['ftp']['hostaddr']
        self.ftp_port = conf_dict['ftp']['port']
        self.ftp_username = conf_dict['ftp']['username']
        self.ftp_password = conf_dict['ftp']['password']
        self.rootdir_remote = conf_dict['ftp']['rootdir_remote']
        self.ftp_connection = FtpClient(
            self.ftp_hostaddr,
            self.ftp_username,
            self.ftp_password,
            self.rootdir_remote,
            self.ftp_port)
        self.transfer_method = conf_dict['all']['transfer_method']

        if self.transfer_method == 'redis':
            self.download_file = self.redis_download_file
            self.upload_file = self.redis_upload_file
        elif self.transfer_method == 'ftp':
            self.download_file = self.ftp_download_file
            self.upload_file = self.ftp_upload_file
        else:
            raise ValueError(
                'could not recognise transfer_method: %s' %
                self.transfer_method)

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
        with open(local_name, 'rb') as f:
            data = f.read()
            self.redis_set(key, data)

    # remote_name: /xxx/xxx.video local_name: before.video
    def ftp_download_file(self, remote_name, local_name):
        print 'local_name:' + local_name
        self.ftp_connection.login()
        self.ftp_connection.download_file(local_name, remote_name)

    # remote_name: /xxx/xxx.video local_name: after.video
    def ftp_upload_file(self, remote_name, local_name):
        print 'local_name:' + local_name
        self.ftp_connection.login()
        self.ftp_connection.upload_file(local_name, remote_name)

    def generate_task_id(self):
        md5_generator = hashlib.md5()
        time_str = str(time.time())
        while True:
            # randomly change the str in case the hash collision
            key_str = time_str + 'any_salt'
            md5_generator.update(key_str)
            key = md5_generator.hexdigest()
            return key


class Uploader(ModuleBase, Singleton):

    def __init__(self):
        super(Uploader, self).__init__()
        # self.logger.log('start upload !!')

    def upload(self, params_str):
        params_json = json.loads(params_str)
        process_task_id = params_json['process_task_ID']
        data_key = process_task_id + '.' + \
            params_json['segment_file_name'] + '.data'
        file_name = self.roam_path + '\\' + process_task_id + \
            '\\before\\' + params_json['segment_file_name']

        self.upload_file(data_key, file_name)
        # self.dump_to_storage(return_key, json.dumps({'filters':None}))

        params_json['data_key'] = data_key
        # params_json['return_key'] = return_key
        # self.logger.log('upload success task_id:%s!! ' % process_task_ID)
        return json.dumps(params_json)


class Downloader(ModuleBase, Singleton):

    def __init__(self):
        super(Downloader, self).__init__()
        # self.logger.log('start download!!')

    def download(self, params_str):
        params_json = json.loads(params_str)
        data_key = params_json['data_key']
        process_task_id = params_json['process_task_ID']
        data_file_name = self.roam_path + '\\' + process_task_id + \
            '\\after\\data\\' + params_json['segment_file_name']
        # return_file_name = self.roam_path + '\\' + process_task_ID +
        # '\\after\\return\\' + params_json['segment_file_name']
        self.download_file(data_key, data_file_name)

        self.redis_del(data_key)
        del params_json['data_key']

        params_json['data_file_name'] = self.roam_path + '\\' + \
            process_task_id + '\\after\\data'
        params_json['return_file_name'] = self.roam_path + '\\' + \
            process_task_id + '\\after\\return'
        # self.logger.log('download success task_id:%s!! ' % process_task_ID)
        return json.dumps(params_json)


class Slicer(ModuleBase, Singleton):

    def __init__(self):
        super(Slicer, self).__init__()

    def _execute_ffmpeg(self, file_name):
        # logger.debug('_execute_ffmpeg(%s)'%file_name)
        s = 'ffmpeg -i %s -f segment -segment_time 10 -c copy -map 0 before\\segment%%d.mp4' % file_name
        ret = os.system(s)
        if ret != 0:
            raise FFmpegExecuteError('FFmpeg execute error: %s' % s)
        else:
            return os.listdir(os.getcwd() + '//' + 'before')

    def slice(self, params_str):
        task_id = self.generate_task_id()
        self.redis_set(task_id, 'in progress')
        with TmpEnv(self.roam_path, director_given_name=task_id, generate_hash_dir=True, del_roam_data=False):
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            params_json = json.loads(params_str)
            # segment the file to segments\
            segments = self._execute_ffmpeg(params_json['input_file_path'])

            # generate to_uploader_para_in_json list
            uploader_params_json = map(
                lambda segment: {
                    'filters': json.dumps(
                        params_json['filters']),
                    'output_file_path': params_json['output_file_path'],
                    'process_task_ID': task_id,
                    'segment_file_name': segment},
                segments)

            uploader_params_str = map(json.dumps, uploader_params_json)
            return uploader_params_str


class Merger(ModuleBase):

    def __init__(self):
        super(Merger, self).__init__()

    def _index_of_segment(self, segment_file_name):
        if segment_file_name[0:7] != 'segment' \
                or segment_file_name[-4:] != '.mp4':
            raise IOError(
                'segment file name unstandarded: %s' %
                segment_file_name)
        else:
            return int(segment_file_name[7:-4])

    def _execute_mp4box(self, output_file_path):
        segments = os.listdir(os.getcwd())
        if len(segments) == 0:
            raise InternalError('segments_num is not right')

        segments = sorted(segments, key=self._index_of_segment)
        s = 'MP4Box '
        for segment in segments:
            s += '-cat ' + segment + ' '
        s += '-new ' + output_file_path
        ret = os.system(s)
        if ret != 0:
            raise GPACExecuteError('GPAC execute error: %s' % s)

    def merge(self, *args, **kargs):
        params_json = json.loads(args[0][0])
        roam_path = params_json['data_file_name']
        output_file_path = params_json['output_file_path']
        task_id = params_json['process_task_ID']
        with TmpEnv(root_path=roam_path,director_given_name=task_id,generate_hash_dir=False,del_roam_data=False):
            self._execute_mp4box(output_file_path)

        os.system('rd /q /s ' + self.roam_path + '\\' + task_id)


class Loader(ModuleBase, Singleton):

    def __init__(self):
        super(Loader, self).__init__()

    def load(self, params_str):
        task_id = self.generate_task_id()
        self.redis_set(task_id, 'in progress')
        with TmpEnv(self.roam_path, director_given_name=task_id,  generate_hash_dir=True, del_roam_data=False) as roam_context:
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            params_json = json.loads(params_str)
            params_json['process_task_ID'] = task_id
            shutil.copyfile(
                params_json['input_file_path'],
                self.roam_path +
                '\\' +
                roam_context.roam_path +
                "\\before\\segement.mp4")
            params_json['segment_file_name'] = 'segement.mp4'
            to_uploader_para_in_str = json.dumps(params_json)
            return to_uploader_para_in_str
