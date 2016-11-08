"""
filters.py

author: huodahaha, zhangxusheng, Yanan Zhao
date:2015/11/12
email:huodahaha@gmail.com
"""
from __future__ import absolute_import


import os
import sys
import subprocess
import json

from cherry.util.exceptions import FFmpegExecuteError
from cherry.util.template import Template
from cherry.util import roam
from cherry.util.logtool import Logger,TaskLogger
from cherry.tasks.operators import Operator, Singleton


class FilterBase(Operator, Singleton):

    def __init__(self):
        super(FilterBase, self).__init__()
        
        self.before_name = 'before.mp4'
        self.after_name = 'after.mp4'
        self.filter_name = self.get_filter_name()

    def _parameter_decode(self, params_str):
        params_json = json.loads(params_str)
        filter_params = params_json['filters'][self.filter_name]

        data_key = params_json['data_key']
        task_id = params_json['task_id']
        cache_type = params_json['cache_type']
        return filter_params, data_key, task_id,cache_type

    def get_filter_name(self):
        return self.__class__.__name__

    def filter_foo(self, task_id, filter_params):
        pass

    def do_process_main(self, filter_params_str):
        with roam.RoamCxt(self.roam_path):
            filter_params, data_key, task_id, cache_type = self._parameter_decode(
                filter_params_str)
            
            if (cache_type == 'redis'):
                self.download_file = self.redis_download_file
                self.upload_file = self.redis_upload_file
            elif (cache_type == 'ftp'):
                self.download_file = self.ftp_download_file
                self.upload_file = self.ftp_upload_file
            else :
                raise ValueError('could not recognise cache_type: %s' % cache_type)
            
            # download the segment need to be transcoded
            logger = Logger()
            logger.info("downloading task[%s] from job tracker" % task_id)
            self.download_file(data_key, self.before_name)

            # do the filter process the segment with FFmpeg
            logger.info("processing task[%s]" % task_id)
            self.filter_foo(task_id, filter_params)

            # upload the transcoded segment
            logger.info("uploading task[%s] to job tracker" % task_id)
            self.upload_file(data_key, self.after_name)

            logger.info("processing task[%s] done" % task_id)
            return filter_params_str


class SimpleTranscoder(FilterBase):
    """transcode with ffmpeg
    """

    def __init__(self):
        super(SimpleTranscoder, self).__init__()

        self.filter_name = self.get_filter_name()
        print "here is:" + self.filter_name

    def filter_foo(self,task_id, codec_parameter):

        self.before_name
        self.after_name

        s = 'ffmpeg -i %s -c:v %s -b:v %s -c:a copy -s %s %s' % (
            self.before_name, codec_parameter['codec'],
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
        print "here:" + self.filter_name

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
            task_logger.debug(line)
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
