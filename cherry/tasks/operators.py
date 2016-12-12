"""
operators.py

author: huodahaha, zhangxusheng, Yanan Zhao
date:2016/10/14
"""

from __future__ import absolute_import

import os
import re
import time
import redis
import shutil
import hashlib


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

from cherry.util.logtool import TaskLogger
task_logger =  TaskLogger("123456")

class Singleton(object):
    """Singleton decorator
    """
    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance

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
    
    def generate_task_id(self):
        md5_generator = hashlib.md5()
        time_str = str(time.time())
        while True:
            # randomly change the str in case the hash collision
            key_str = time_str + 'any_salt'
            md5_generator.update(key_str)
            key = md5_generator.hexdigest()[0:16]
            return key
    
    def redis_upload_file(self, key, local_file):
#         print("upload file to redis: (k,v) = (%s, %s)" % (key, local_file))
        task_logger.info("redis upload %s " %key)
        with open(local_file, 'rb') as f:
            data = f.read()
            self.redis_connection.set(key, data)
    
    # remote_name: /xxx/xxx.video local_name: after.video
    def ftp_upload_file(self, remote_file, local_file):
#         print("upload file to ftp: (k,v) = (%s, %s)" % (remote_file, local_file))
        self.ftp_connection.login()
        self.ftp_connection.upload_file(local_file, remote_file)
           
    def redis_download_file(self, key, local_name):
        task_logger.info("redis download %s"%key)
        data = self.redis_connection.get(key)
        with open(local_name, 'wb') as f:
            f.write(data)    
            
    # remote_name: /xxx/xxx.video local_name: before.video
    def ftp_download_file(self, remote_name, local_name):
#         print 'local_name:' + local_name
        self.ftp_connection.login()
        self.ftp_connection.download_file(local_name, remote_name)

class Uploader(Operator, Singleton):

    def __init__(self):
        super(Uploader, self).__init__()

    def upload(self, cxt):
        """ upload file to redis/ftp server

        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        task_id = cxt['task_id']
        cache_type = cxt['cache_type']
        
          
        data_key = task_id + '.' + cxt['segment_file_name'] + '.data'
        file_name = os.path.join(self.roam_path, task_id, 'before',
                                 cxt['segment_file_name'])

        task_logger.info("Uploader: uploading file %s, data_key %s" %
              (file_name, data_key))
        # upload to redis or ftp server
        if (cache_type == 'redis'): 
            self.redis_upload_file(data_key, file_name)
        elif (cache_type == 'ftp'):
            data_key = 'cache/'+data_key
            self.ftp_upload_file(data_key, file_name)
        else:
            raise ValueError('could not recognise cache_type: %s' % cache_type)  
        
        cxt['data_key'] = data_key
        return cxt

class Downloader(Operator, Singleton):

    def __init__(self):
        super(Downloader, self).__init__()

    def download(self, cxt):
        """ download file from redis/ftp server to local
        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        data_key = cxt['data_key']
        task_id = cxt['task_id']
        cache_type = cxt['cache_type']
        data_key_in_list = data_key.split('.')

        data_file_name = os.path.join(self.roam_path, task_id,
                                      'after', 'data',
                                      '.'.join(data_key_in_list[1:-1]))
        if (cache_type == 'redis'): 
            self.redis_download_file(data_key, data_file_name)
#             self.redis_del(data_key)
        elif (cache_type == 'ftp'):
            self.ftp_download_file(data_key, data_file_name)
#             self.ftp_del(data_key)
        else:
            raise ValueError('could not recognise cache_type: %s' % cache_type)  
        
        
        task_logger.info("task[%s]: downloading file %s done" % (task_id, data_key))

        cxt['data_file_name'] = data_file_name
        cxt['index_list'] = '_'.join(data_key_in_list[1].split('_')[1:])
        cxt['return_file_path'] = os.path.join(self.roam_path, task_id,
                                               "after", "return")
        return cxt

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

    def slice(self, cxt):
        task_id = self.generate_task_id()
        task_logger.info("task[%s]: slicing file %s done" % (task_id, cxt['input_file_path']))
        with roam.RoamCxt(self.roam_path, given_dir=task_id,
                          generate_hash_dir=True, del_roam_data=False):
            os.mkdir('before')
            os.mkdir('after')
            os.chdir('after')
            os.mkdir('data')
            os.mkdir('return')
            os.chdir(os.path.pardir)
            os.chdir('before')
            
            segments = self._do_segmentation(cxt['input_file_path'])
            os.chdir(os.path.pardir)

            # generate params for uploader
            slicer_cxt = map(
                lambda segment: {
                    'filters': cxt['filters'],
                    'cache_type': cxt['cache_type'],
                    'is_local':cxt['is_local'],
                    'task_id': task_id,
                    'segment_file_name': segment},
                segments)

            return slicer_cxt

class Merger(Operator):

    def __init__(self):
        super(Merger, self).__init__()

    def _index_of_segment(self, segment_file_name):
        return int(segment_file_name.split('_')[0][7:])

    def _execute_mp4box(self, output_file_path, task_id, index_list, slicer_nums):
        print os.getcwd()
        all_return_segments = os.listdir(os.getcwd())
        segments = []
        reobj = re.compile('segment[0-9]+_'+index_list+'\.[a-zA-Z0-9]+')
        for segment in all_return_segments:   
            if reobj.match(segment):
                segments.append(segment)
        
        if len(segments) != slicer_nums:
            raise InternalError('segments_num is not right')

        segments = sorted(segments, key=self._index_of_segment)
        s = 'MP4Box '
        for segment in segments:
            s += '-cat ' + segment + ' '
        s += '-new ' + output_file_path
        ret = os.system(s)
        if ret != 0:
            raise GPACExecuteError('GPAC execute error: %s' % s)

    def merge(self, cxt):
        output_file_path = cxt['output_file_path']
        task_id = cxt['task_id']
        slicer_nums = cxt['slicer_nums']
        index_list = cxt['index_list']
        
        task_logger.info("task[%s_%s]: merging file." % (task_id,index_list))
        with roam.RoamCxt(root_path=os.path.join(self.roam_path, task_id,'after','data'),
                          given_dir=None,
                          generate_hash_dir=False,
                          del_roam_data=False):
            self._execute_mp4box(output_file_path, task_id, index_list, slicer_nums)

class Loader(Operator, Singleton):

    def __init__(self):
        super(Loader, self).__init__()

    def load(self, cxt):
        """ load input file to specified local location

        context: json-serialized job description
        return: json-serialized context, for subsequent processing
        """
        task_id = self.generate_task_id()
        task_logger.info("task[%s]: loading file %s" % (task_id, cxt['input_file_path']))
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
            return cxt
        
class Backer(Operator, Singleton):

    def __init__(self):
        super(Backer, self).__init__()

    def back(self, cxt):
        data_file_name = cxt['data_file_name']
        output_file_path = cxt['output_file_path']
        task_id = cxt['task_id']
        
        shutil.move(data_file_name, output_file_path)
        print("Backer: Move output file %s ..." %
              (os.path.join(self.roam_path, task_id)))
        return cxt
        
class Deleter(Operator, Singleton):
    def __init__(self):
        super(Deleter, self).__init__()
        
    def redis_del(self, *key):
        for one_key in key:   
            task_logger.info("redis delete %s."%one_key)
        return self.redis_connection.delete(*key)
    
    def redis_keys(self, patten):
        return self.redis_connection.keys(patten)
    
    def ftp_del(self, key):
        task_logger.info("redis delete %s"%key)
        self.ftp_connection.login()
        return self.ftp_connection.delete_file(key)

    def delete_cache(self, cxt):
        task_id = cxt['task_id']
        cache_type = cxt['cache_type']
        
        shutil.rmtree(os.path.join(self.roam_path, task_id))
        if (cache_type == 'redis'): 
            self.redis_del(*self.redis_keys(task_id+'*'))
        elif (cache_type == 'ftp'):
            self.ftp_connection.login()
            del_list = self.ftp_connection.list_file('cache')
            reobj = re.compile(task_id+'.*?')
            for del_file in del_list:   
                if reobj.match(del_file):
                    self.ftp_del('cache/'+del_file)
        else:
            raise ValueError('could not recognise cache_type: %s' % cache_type)  

# create instances
uploader = Uploader()
downloader = Downloader()
slicer = Slicer()
merger = Merger()
loader = Loader()
backer = Backer()
deleter = Deleter()