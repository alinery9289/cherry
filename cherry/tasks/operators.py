"""
operators.py

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


from cherry.util import roam
from cherry.util.exceptions import (
    FFmpegExecuteError,
    GPACExecuteError,
    InternalError)
from cherry.util.config import conf_dict as conf
from cherry.util.ftptool import FtpClient


# log_path = os.getenv('CHERRY_LOG_DIR')
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

    # def get_instance(cls):
    #     return cls._instance


class Operator(object):

    def __init__(self):
        self.roam_path = os.getenv('CHERRY_ROAM_DIR') or conf[
                                   'all']['roam_path']
        if not self.roam_path or not os.path.exists(self.roam_path):
            raise IOError('Could not open roam directory %s' % self.roam_path)

        # read conf file
        self.redis_host = conf['redis']['ip']
        self.redis_port = conf['redis']['port']
        self.redis_connection = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=0)

        self.ftp_hostaddr = conf['ftp']['hostaddr']
        self.ftp_port = conf['ftp']['port']
        self.ftp_username = conf['ftp']['username']
        self.ftp_password = conf['ftp']['password']
        self.rootdir_remote = conf['ftp']['rootdir_remote']
        self.ftp_connection = FtpClient(
            self.ftp_hostaddr,
            self.ftp_username,
            self.ftp_password,
            self.rootdir_remote,
            self.ftp_port)
        self.transfer_method = conf['all']['transfer_method']

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

    def redis_upload_file(self, key, local_file):
        print("upload file to redis: (k,v) = (%s, %s)" % (key, local_file))
        with open(local_file, 'rb') as f:
            data = f.read()
            self.redis_set(key, data)

    # remote_name: /xxx/xxx.video local_name: before.video
    def ftp_download_file(self, remote_name, local_name):
        print 'local_name:' + local_name
        self.ftp_connection.login()
        self.ftp_connection.download_file(local_name, remote_name)

    # remote_name: /xxx/xxx.video local_name: after.video
    def ftp_upload_file(self, remote_file, local_file):
        print("upload file to ftp: (k,v) = (%s, %s)" % (remote_file, local_file))
        self.ftp_connection.login()
        self.ftp_connection.upload_file(local_file, remote_file)

    def generate_task_id(self):
        md5_generator = hashlib.md5()
        time_str = str(time.time())
        while True:
            # randomly change the str in case the hash collision
            key_str = time_str + 'any_salt'
            md5_generator.update(key_str)
            key = md5_generator.hexdigest()[0:16]
            return key


class Loader(Operator, Singleton):

    def __init__(self):
        super(Loader, self).__init__()

    def load(self, context):
        """ load input file to specified local location

        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        task_id = self.generate_task_id()
        self.redis_set(task_id, 'in progress')
        with roam.RoamCxt(self.roam_path,
                          given_dir=task_id,
                          generate_hash_dir=True,
                          del_roam_data=False) as roam_cxt:
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            cxt = json.loads(context)
            cxt['task_id'] = task_id

            src_file = cxt['input_file_path']
            dst_file = os.path.join(self.roam_path, roam_cxt.roam_path,
                                    "before", "segment.mp4")
            print("copying file %s to %s" % (src_file, dst_file))
            try:
                shutil.copyfile(src_file, dst_file)
            except Exception as e:
                raise IOError("file loading error: %s" % str(e))

            cxt['segment_file_name'] = 'segment.mp4'
            return json.dumps(cxt)


class Uploader(Operator, Singleton):

    def __init__(self):
        super(Uploader, self).__init__()
        # self.logger.log('start upload !!')

    def upload(self, context):
        """ upload file to redis/ftp server

        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        cxt = json.loads(context)
        task_id = cxt['task_id']
        data_key = task_id + '.' + cxt['segment_file_name'] + '.data'
        file_name = os.path.join(self.roam_path, task_id, 'before',
                                 cxt['segment_file_name'])

        print("Uploader: uploading file %s, data_key %s" %
              (file_name, data_key))
        # upload to redis or ftp server
        self.upload_file(data_key, file_name)

        cxt['data_key'] = data_key
        return json.dumps(cxt)


class Downloader(Operator, Singleton):

    def __init__(self):
        super(Downloader, self).__init__()
        # self.logger.log('start download!!')

    def download(self, context):
        """ download file from redis/ftp server to local

        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        cxt = json.loads(context)
        data_key = cxt['data_key']
        task_id = cxt['task_id']

        data_file_name = os.path.join(self.roam_path, task_id,
                                      'after', 'data',
                                      cxt['segment_file_name'])

        self.download_file(data_key, data_file_name)
        from cherry.util.logtool import TaskLogger
        task_log =  TaskLogger("12321")
        task_log.info(data_key)
        self.redis_del(data_key)
        del cxt['data_key']

        cxt['data_file_name'] = os.path.join(self.roam_path,
                                             task_id, "after", "data")
        cxt['return_file_name'] = os.path.join(self.roam_path, task_id,
                                               "after", "return")
        # self.logger.log('download success task_id:%s!! ' % task_id)
        print('download task_id: %s success' % task_id)
        return json.dumps(cxt)


class Slicer(Operator, Singleton):

    def __init__(self):
        super(Slicer, self).__init__()

    def _do_segmentation(self, file_name):
        """segment input file in current directory (os.getcwd())
        """
        cmd = "ffmpeg -i %s -f segment -segment_time 15 -c copy -map 0 " \
            "segment%%d.mp4" % file_name

        ret = os.system(cmd)
        if ret != 0:
            raise FFmpegExecuteError('FFmpeg execute error: %s' % cmd)
        else:
            return os.listdir(os.getcwd())

    def slice(self, context):
        task_id = self.generate_task_id()
        self.redis_set(task_id, 'in progress')
        with roam.RoamCxt(self.roam_path, given_dir=task_id,
                          generate_hash_dir=True, del_roam_data=False):
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            os.chdir('before')
            cxt = json.loads(context)
            segments = self._do_segmentation(cxt['input_file_path'])
            os.chdir(os.path.pardir)

            # generate params for uploader
            uploader_cxt = map(
                lambda segment: {
                    'filters': cxt['filters'],
                    'output_file_path': cxt['output_file_path'],
                    'task_id': task_id,
                    'segment_file_name': segment},
                segments)

            uploader_cxts = map(json.dumps, uploader_cxt)
            return uploader_cxts


class Merger(Operator):

    def __init__(self):
        super(Merger, self).__init__()

    def _index_of_segment(self, segment_file_name):
        if segment_file_name[0:7] != 'segment' \
                or segment_file_name[-4:] != '.mp4':
            raise IOError(
                'segment file name unstandarded: %s' % segment_file_name)
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
        cxt = json.loads(args[0][0])
        roam_path = cxt['data_file_name']
        output_file_path = cxt['output_file_path']
        task_id = cxt['task_id']
        with roam.RoamCxt(root_path=roam_path,
                          given_dir=task_id,
                          generate_hash_dir=False,
                          del_roam_data=False):
            self._execute_mp4box(output_file_path)

        print("Merger: removing %s ..." %
              (os.path.join(self.roam_path, task_id)))
        shutil.rmtree(os.path.join(self.roam_path, task_id))


# create instances
loader = Loader()
uploader = Uploader()
downloader = Downloader()
slicer = Slicer()
merger = Merger()
