import os
import sys
import ConfigParser

conf_dir = os.getenv('DISTRIBUTED_TRANSCODER_CONF')
if not conf_dir:
    print("DISTRIBUTED_TRANSCODER_CONF not defined")
    raise

conf_file = os.path.join(conf_dir, 'cherry.conf')
conf = ConfigParser.ConfigParser()
conf.read(conf_file)

sections = conf.sections()
conf_dict = {}
for section in sections:
    one_conf_dict = {}
    for oneitem in conf.items(section):
        one_conf_dict[oneitem[0]] = oneitem[1]
    conf_dict[section] = one_conf_dict
