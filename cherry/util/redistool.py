from __future__ import absolute_import

import redis

from cherry.util.config import conf_dict



class Pro_status():
    
    def __init__(self):
        self.config = {
        'host': conf_dict['redis']['ip'], 
        'port': conf_dict['redis']['port'],
        'db': 0,
        }

        self.r = redis.Redis(**self.config)
    def redis_set(self, name, value):
        self.r.set(name, value)
    def redis_get(self, name):
        return self.r.get(name)
