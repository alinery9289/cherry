# -*- coding: utf-8 -*-
'''
filters module
author: zhangxusheng
date:2015/11/12
email:huodahaha@gmail.com
'''
from __future__ import absolute_import

import os
import jinja2

from cherry.tasks.basic_module import Singleton
from cherry.util.MyConfig import conf_dict

# generate a template bat


class template(Singleton):

    def __init__(self):
        super(template, self).__init__()

        # update filtera name
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(
                conf_dict['all']['template_path']))
        self.bat_name = "execute"

    def generate_bat(self, template_name, bat_file_tmp_param):
        bat_file_name = self.bat_name + '_' + template_name
        bat_file_tmp_param.update(conf_dict['4k_tool'])

        bat_file_tmp = self.template_env.get_template(
            template_name).render(bat_file_tmp_param)

        # write the new control file
        lines = "\r\n".join(bat_file_tmp.split('\n'))
        try:
            bat_file = open(bat_file_name, 'w+')
            bat_file.writelines(lines)
            bat_file.close()
        except Exception as e:
            print('can\'t write bat_file '+str(Exception)+':'+str(e))
        return bat_file_name
