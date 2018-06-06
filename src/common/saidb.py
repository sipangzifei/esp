#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymssql

from sailog  import *
from saiconf import *

def db_init(_name=None):

    db_name = "DB"
    if _name is not  None:
        db_name = _name

    # log_debug('dbname -- %s', db_name)

    server   = sai_conf_get(db_name, "HOST")
    user     = sai_conf_get(db_name, "USERNAME")
    password = sai_conf_get(db_name, "PASSWORD")
    dbname   = sai_conf_get(db_name, "DBNAME")

    conn = None
    try:
        conn = pymssql.connect(server, user, password, dbname)
    except Exception, err:
        log_error("error: %s", err)

    return conn




def db_end(_db):
    _db.close()

if __name__ == "__main__":
    sailog_set("saidb.log")
    db = db_init()
    if db is None:
        log_debug("error: db_init")
    else :
        log_debug("nice: db_init")
        # raw_input()

# saidb.py
