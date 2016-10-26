# -*- coding: utf-8 -*-
'''
filters module
author: huodahaha, zhangxusheng
date:2015/11/12
email:huodahaha@gmail.com
'''
from __future__ import absolute_import


import os
import subprocess

from cherry.tasks.basic_module import base_filter
from cherry.util.MyExceptions import FFmpegExecuteError
from cherry.util.MyTemplate import template
from cherry.util.redistool import Pro_status
from cherry.util.logtool import Pro_log

# transcode with ffmpeg
class sync_transcoder(base_filter):
    
    def __init__(self):
        super(sync_transcoder, self).__init__()

        # update filtera name
        self.filter_name = self.get_filter_name()
        print "here is:"+ self.filter_name


    def filter_foo(self, codec_parameter):
        print 'this is a ffmpeg filter!!!'


        self.before_name
        self.after_name

        
        s = 'ffmpeg -i %s -c:v %s -b:v %s -c:a copy -s %s %s' % (self.before_name, \
            codec_parameter['codec'], codec_parameter['bitrate'], codec_parameter['resolution'], self.after_name)
        ret = os.system(s)
        if ret != 0:
            raise FFmpegExecuteError('FFmpeg execute error: %s'%s)


class tem_transcoder(base_filter):
    
    def __init__(self):
        super(tem_transcoder, self).__init__()

        # update filtera name
        self.filter_name = self.get_filter_name()
        print "here:"+ self.filter_name


    def filter_foo(self, process_task_ID, codec_parameter):
        print 'this is a tmplate filter!!!'
        self.before_name
        self.after_name

        #load the control file template,set the parameter
        bat_file_tmp_param = {'before_file_name':self.before_name ,'after_file_name':self.after_name}
        bat_file_tmp_param.update(codec_parameter)
        tem_instance = template()
        bat_file_name = tem_instance.generate_bat('hevc_bat_template.bat', bat_file_tmp_param)
        #download file and process and upload result
        process2 = subprocess.Popen(bat_file_name, shell=True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT,universal_newlines= True)
        
#         one_status = Pro_status()
        one_log = Pro_log(process_task_ID)
        while True:
            line = process2.stdout.readline()
#             parse_state.add_ffmpeg_state_to_redis(param_data['taskid'],line)
            one_log.debug(line)
            if not line:
                break
        
        

class blank_filter(base_filter):
    
    def __init__(self):
        super(blank_filter, self).__init__()

        # update filtera name
        self.filter_name = self.get_filter_name()


    def filter_foo(self, codec_parameter):
        print 'this is a blank filter!!!'
        pass

filters_dict = {'tem_transcoder': tem_transcoder, 'tem_transcoder': tem_transcoder}