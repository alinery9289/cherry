"""
filters module
author: huodahaha, zhangxusheng, Yanan Zhao
date:2015/11/12
email:huodahaha@gmail.com
"""
from __future__ import absolute_import


import os
import subprocess

from cherry.util.MyExceptions import FFmpegExecuteError
from cherry.util.MyTemplate import template
from cherry.util.redistool import Pro_status
from cherry.util.logtool import Pro_log


class FilterBase(ModuleBase, Singleton):

    def __init__(self):
        super(FilterBase, self).__init__()
        self.before_name = 'before.mp4'
        self.after_name = 'after.mp4'
        self.filter_name = self.get_filter_name()

    def _parameter_decode(self, params_str):
        params_json = json.loads(params_str)
        filter_parameter = params_json['filters'][self.filter_name]

        data_key = params_json['data_key']
        process_task_id = params_json['process_task_id']
        return filter_parameter, data_key, process_task_id

    def get_filter_name(self):
        return self.__class__.__name__

    def filter_foo(self, process_task_ID, filter_parameter):
        pass

    def do_process_main(self, to_filter_para_in_str):
        print self.filter_name
        with MyRoam.TmpEnv(self.roam_path):
            # check the parameter passed by the upper layer
            filter_parameter, src, process_task_ID = self._parameter_decode(
                to_filter_para_in_str)

            main_log = Pro_log_main()
            main_log.info(
                "Begin to download task from job tracker, taskid is " +
                process_task_ID + ".")

            # download the segment need to be transcoded
            self.download_file(src, self.before_name)

            main_log.info(
                "Begin to process task, taskid is " +
                process_task_ID + ".")
            # do the filter process the segment with FFmpeg
            self.filter_foo(process_task_ID, filter_parameter)

            # write status
            main_log.info(
                "Begin to upload task to job tracker, taskid is " +
                process_task_ID + ".")
            # upload the transcoded segment
            self.upload_file(src, self.after_name)

            main_log.info(
                "Upload task to job tracker ok, taskid is " +
                process_task_ID + ".")
            return to_filter_para_in_str


class SyncTranscoder(FilterBase):
    """transcode with ffmpeg
    """

    def __init__(self):
        super(SyncTranscoder, self).__init__()

        self.filter_name = self.get_filter_name()
        print "here is:" + self.filter_name

    def filter_foo(self, codec_parameter):
        print 'this is a ffmpeg filter!!!'

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
        print 'this is a template filter!!!'
        self.before_name
        self.after_name

        # load the control file template,set the parameter
        bat_file_tmp_param = {
            'before_file_name': self.before_name,
            'after_file_name': self.after_name}
        bat_file_tmp_param.update(codec_parameter)
        tem_instance = template()
        bat_file_name = tem_instance.generate_bat(
            'hevc_bat_template.bat', bat_file_tmp_param)
        # download file and process and upload result
        process2 = subprocess.Popen(
            bat_file_name,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True)

        one_log = Pro_log(task_id)
        while True:
            line = process2.stdout.readline()
            one_log.debug(line)
            if not line:
                break


class BlankFilter(FilterBase):

    def __init__(self):
        super(BlankFilter, self).__init__()

        self.filter_name = self.get_filter_name()

    def filter_foo(self, codec_parameter):
        print 'this is a blank filter!!!'
        pass

filters_dict = {'tem_transcoder': tem_transcoder,
                'tem_transcoder': tem_transcoder}
