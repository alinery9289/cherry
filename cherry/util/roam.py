# encoding:utf-8
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

logger = logging.getLogger('transcoder')


class TmpEnv(object):

    def __init__(self, root_path, director_given_name=None,
                 generate_hash_dir=True, del_roam_data=True):
        self._root_path = root_path
        self._original_path = os.getcwd()
        self._generate_hash_dir = generate_hash_dir
        self._del_roam_data = del_roam_data
        self._roam_dir = director_given_name
        if generate_hash_dir:
            if director_given_name is None:
                self._roam_dir = self._generate_dir_name()

    @property
    def roam_path(self):
        return self._roam_dir

    def back_to_roam_dir(self):
        os.chdir(self._roam_dir)

    def _generate_dir_name(self):
        md5_generator = hashlib.md5()
        md5_generator.update(str(time.time()))
        return md5_generator.hexdigest()[0:16]

    def __enter__(self):
        os.chdir(self._root_path)
        if self._generate_hash_dir:
            os.system('mkdir ' + self._roam_dir)
            logger.debug('mkdir ' + self._roam_dir)
            os.chdir(self._roam_dir)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._del_roam_data:
            os.chdir(os.path.pardir)
            if self.roam_path:
                os.system('rd /q /s ' + self.roam_path)
        if os.path.exists(self._original_path):
            os.chdir(self._original_path)
