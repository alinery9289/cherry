#encoding:utf-8
import os
import ConfigParser

conf_file = os.getenv('DISTRIBUTED_TRANSCODER_CONF') + '\\conf.conf'

# conf_file = 'F:\\project\\2015\\cherry\\proj' + '\\conf.conf'
conf = ConfigParser.ConfigParser()
conf.read(conf_file)

sections = conf.sections()
conf_dict = {}
for section in sections:
    one_conf_dict={}
    for oneitem in conf.items(section):
        one_conf_dict[oneitem[0]]=oneitem[1]
    conf_dict[section]=one_conf_dict
