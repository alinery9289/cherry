"""
filters.py

author: huodahaha, zhangxusheng, Yanan Zhao
date:2015/11/12
email:huodahaha@gmail.com
"""
from __future__ import absolute_import


import os
import sys
import shutil
import subprocess

from cherry.util.config import conf_dict as conf
from cherry.util.exceptions import FFmpegExecuteError, ParamterError
from cherry.util.template import Template
from cherry.util import roam
from cherry.util.logtool import Logger,TaskLogger
from cherry.tasks.operators import Operator, Singleton


class FilterBase(Operator):

    def __init__(self):
        super(FilterBase, self).__init__()
        
        self.before_name = 'before.mp4'
        self.after_name = 'after.mp4'
        self.filter_name = self.get_filter_name()
        self.local_filters = conf['all']['filters'].split(',')
        
    def get_filter_name(self):
        return self.__class__.__name__
    
    def update_data_key(self,data_key,index_series):
        data_key_values = data_key.split('.')
        data_key_values[-3] = data_key_values[-3]+'_'+str(index_series)
        return '.'.join(data_key_values) 

    def filter_foo(self, task_id, filter_params):
        pass

    def do_process_main(self, context):
        with roam.RoamCxt(self.roam_path) as roam_cxt:
            # decode parameter
            try :
                filter_params = context['filters'][self.filter_name]
                cache_type = context['cache_type']
                task_id = context['task_id']
                data_key = context['data_key']
                index_series = context['index_series']
                is_local = context['is_local'] #never local= 0, previous data not in local= 1,previous data in local = 2;  
                return_data_key = self.update_data_key(data_key,index_series)
            except Exception,e:
                raise ParamterError('could not parse the parameter.%s :%s'%(Exception,e))
            
            #choose the cache_type
            if (cache_type == 'redis'):
                self.download_file = self.redis_download_file
                self.upload_file = self.redis_upload_file
            elif (cache_type == 'ftp'):
                self.download_file = self.ftp_download_file
                self.upload_file = self.ftp_upload_file
            else :
                raise ValueError('could not recognise cache_type: %s' % cache_type)
            
            logger = Logger()
            # download the segment need to be transcoded
            logger.info("downloading data [%s] from job tracker" % data_key)
            if is_local==2:
                shutil.copy(context['local_data_key'],self.before_name)
            else:
                self.download_file(data_key, self.before_name)

            # do the filter process the segment with FFmpeg
            logger.info("processing data [%s]" % data_key)
            self.filter_foo(task_id, filter_params)
            
            # judge if process next filter in local 
            next_filter_names = []
            if is_local == 0:
                next_is_local = 0
#                 logger.info("is_local: %s" % is_local)
            else :
                return_new_contexts = []
                if (filter_params['next']!={}):    
                    next_is_local = 2
                    for one_next_filter in filter_params['next']:
                        next_filter_name = one_next_filter.keys()[0]
                        next_filter_names.append(next_filter_name)
                        if next_filter_name not in self.local_filters:
                            next_is_local = 1
#                             logger.info("is_local: %s" % is_local)
                            break
                else :
                    next_is_local = 1

            # send control command to local next filter or job tracker
            if (next_is_local == 2 and filter_params['next']!={}):
                
                logger.info("is_local: %s" % is_local)
                next_contexts = [{'filters': one_next_filter,
                                  'cache_type': cache_type,
                                  'task_id': task_id,
                                  'index_series': num,
                                  'data_key': return_data_key,
                                  'is_local':next_is_local,
                                  'local_data_key':os.path.join(self.roam_path, roam_cxt.roam_path,self.after_name),}
                                 for num,one_next_filter in enumerate(filter_params['next'])]
                
                logger.info("processing data [%s] done" % data_key)
                logger.info("will processing next data [%s] locally" % return_data_key) 
                
                for next_content in next_contexts: #call next filters
                    next_filter_instance = filters_dict[next_content['filters'].keys()[0]]()
                    return_new_contexts.extend(next_filter_instance.do_process_main(next_content))
                return return_new_contexts
            elif (filter_params['next']=={}):
                next_contexts = [{'filters': {},
                                  'output_file_path':filter_params['output_file_path'],
                                  'cache_type': cache_type,
                                  'task_id': task_id,
                                  'index_series': 0,
                                  'data_key':return_data_key, 
                                  'is_local':next_is_local,}]
                
            else:
                next_contexts = [{'filters': one_next_filter,
                                  'cache_type': cache_type,
                                  'task_id': task_id,
                                  'index_series': num,
                                  'data_key': return_data_key, #next_process_over
                                  'is_local':next_is_local,}
                                 for num,one_next_filter in enumerate(filter_params['next'])]
                
            logger.info("processing data [%s] done" % return_data_key)
            self.upload_file(return_data_key, self.after_name)
            logger.info("uploading next data [%s] to job tracker ok" % return_data_key)
            return next_contexts

class SimpleTranscoder(FilterBase):
    """transcode with ffmpeg
    """

    def __init__(self):
        super(SimpleTranscoder, self).__init__()

        self.filter_name = self.get_filter_name()

    def filter_foo(self,task_id, codec_parameter):

        self.before_name
        self.after_name

        s = '%s -i %s -c:v %s -b:v %s -c:a copy -s %s %s' % (
            conf['tools']['mp4box'], self.before_name, codec_parameter['codec'],
            codec_parameter['bitrate'],
            codec_parameter['resolution'],
            self.after_name)
        ret = os.system(s)
        if ret != 0:
            raise FFmpegExecuteError('FFmpeg execute error: %s' % s)


class TemplateTranscoder(FilterBase):

    def __init__(self):
        super(TemplateTranscoder, self).__init__()

        self.filter_name = self.get_filter_name()

    def filter_foo(self, task_id, codec_parameter):
        print("filter_foo @ TemplateTranscoder")

        template_params = {'before_file_name': self.before_name,
                           'after_file_name': self.after_name}
        template_params.update(codec_parameter)

        if sys.platform.startswith('linux'):
            template_file = 'hevc_template.sh'
        else:  # windows
            template_file = 'hevc_template.bat'

        template = Template()
        script = template.generate_bat(template_file, template_params)

        # TODO: check whether this is ok on windows
        script = os.path.join(os.getcwd(), script)
        print("absolute path of script %s" % script)
        process2 = subprocess.Popen(script,
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    universal_newlines=True)
        task_logger = TaskLogger(task_id)
        while True:
            line = process2.stdout.readline()
#             task_logger.debug(line)
            if not line:
                break

        return template_params


class BlankFilter(FilterBase):

    def __init__(self):
        super(BlankFilter, self).__init__()

        self.filter_name = self.get_filter_name()

    def filter_foo(self, codec_parameter):
        print 'this is a blank filter!!!'
        pass


# filters_dict = {'SimpleTranscoder': SimpleTranscoder,
filters_dict = {'TemplateTranscoder': TemplateTranscoder, 'SimpleTranscoder':SimpleTranscoder}
