#!/usr/bin/env python
# -*- encoding: utf8 -*-

import ConfigParser

from sailog  import *


g_sai_conf = None


def sai_load_conf():
    sai_conf_path = "my.conf"
    sai_conf_path = "%s/cnf/my.conf" % os.getenv('ESP_HOME')
    # log_debug("loading config [%s]", sai_conf_path)
    if os.path.isfile(sai_conf_path):
        cf = ConfigParser.ConfigParser()
        cf.read(sai_conf_path)
        return cf
    else :
        log_error("error: invalid sai_conf_path[%s]!", sai_conf_path)
        return -1


def sai_conf_get(_section, _key):
    global g_sai_conf

    if g_sai_conf is None or g_sai_conf == -1:
        g_sai_conf = sai_load_conf()
        if g_sai_conf == -1:
            log_error("error: sai_load_conf failure")
            return -1
        else:
            # log_debug("config load succeed")
            pass
    else:
        # log_debug("already loaded")
        pass

    val = ""
    if g_sai_conf.has_section(_section) :
        if g_sai_conf.has_option(_section, _key):
            val = g_sai_conf.get(_section, _key)
        else:
            log_error("error: key[%s] in [%s] not found", _key, _section)
    else:
        log_error("error: section[%s] not found", _section)

    return val


if __name__=="__main__":
    sailog_set("saiconf.log")




# saiconf.py
