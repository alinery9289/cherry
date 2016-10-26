#encoding:utf-8
'''
generate temporary roam path
author: huodahaha
date:2015/10/18
email:huodahaha@gmail.com
'''

from __future__ import absolute_import



import os
import time
import hashlib
import logging  
import logging.handlers
import pdb

logger = logging.getLogger('transcoder')

class temporary_env(object):
    def __init__(self, root_path, director_given_name = None, generate_hash_dir = True, del_roam_data = True):
        self.__root_path = root_path
        self.__original_path = os.getcwd()
        self.__generate_hash_dir = generate_hash_dir
        self.__del_roam_data = del_roam_data
        self.__roam_folder = director_given_name
        if generate_hash_dir:
            if director_given_name is None:
                self.__roam_folder = self.__generate_dir_name()

    @property
    def roam_path(self):
        return self.__roam_folder

    def back_to_roam_dir(self):
        os.chdir(self.__roam_folder)

    def __generate_dir_name(self):
        md5_generator = hashlib.md5()
        md5_generator.update(str(time.time()))
        return md5_generator.hexdigest()[0:16]

    def __enter__(self):
        os.chdir(self.__root_path)
        if self.__generate_hash_dir:
            os.system('mkdir ' + self.__roam_folder)
            logger.debug('mkdir ' + self.__roam_folder)
            os.chdir(self.__roam_folder)
        return self


    def __exit__(self,exc_type,exc_value,traceback):
        if self.__del_roam_data:
            os.chdir(os.path.pardir)
            # pdb.set_trace()
            if self.roam_path:
                os.system('rd /q /s ' + self.roam_path)
        if os.path.exists(self.__original_path):
            os.chdir(self.__original_path)