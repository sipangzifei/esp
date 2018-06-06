# -*- coding: UTF-8 -*-

from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *
from myfunc  import *


def get_source_data(_table):

    key_str     = sai_conf_get('CHECK', 'KEY')

    where_str   = sai_conf_get('CHECK', 'WHERE')

    # Query DB-source
    sql = "select * from %s %s order by %s" % (_table, where_str, key_str)
    log_debug("sql1: %s", sql)

    MyCtx.cursorX.execute(sql)
    c1_list = MyCtx.cursorX.fetchall()

    return c1_list


def compare_table():
    table = ''

    try:

        # get the key-column to query table
        key_str = sai_conf_get('CHECK', 'KEY').strip()

        # get the columns to compare
        cmp_str = sai_conf_get('CHECK', 'CMP').strip()
        check_list = cmp_str.split(',')
        trim_list(check_list)

        # language
        lang = sai_conf_get('MY', 'LANG')


        # table
        table   = sai_conf_get('CHECK', 'TABLE')
        c1_list = get_source_data(table)

        diff = 0
        for row1 in c1_list:
            key_val = row1[key_str].strip()
            # log_debug("name -- [%s]", key_val)

            # DB_Y
            where_str = "%s = '%s'" % (key_str, key_val)

            sql = "SELECT * FROM %s where %s" % (table, where_str)
            # log_debug("sql2: %s", sql)
            MyCtx.cursorY.execute(sql)
            c2_list = MyCtx.cursorY.fetchall()
            if len(c2_list) == 0:
                log_debug("sorry: not found: %s", key_val)
                print("sorry: not found: %s" % key_val)
                continue
            row2 = c2_list[0]


            for column in check_list:
                # log_debug('column = %s', column)
                v1 = row1[column]
                v2 = row2[column]
                if isinstance(v1, int):
                    # log_debug("---int")
                    pass
                elif isinstance(v1, str):
                    # log_debug("---string")
                    pass
                elif isinstance(v1, float):
                    # log_debug("---float")
                    pass
                elif isinstance(v1, bytes):
                    # log_debug("---bytes")
                    pass
                elif isinstance(v1, unicode):
                    # log_debug("---unicode")
                    v1 = v1.rstrip().encode(lang)
                    v2 = v2.rstrip().encode(lang)
                elif v1 is None:
                    # log_debug("---%s: None",  column)
                    pass
                else:
                    log_info("---other")
                    log_info('---type = %s', type(v1))

                # log_debug('---[%s] : [%s]',  v1, v2)

                if v1 != v2:
                    log_info('>>> [%s].[%s]: [%s] --> [%s]', key_val, column, v1, v2)
                    diff = diff + 1
                    pass
                else:
                    # log_debug('---the same[%s] : [%s]', v1, v2)
                    pass

            # break

        if diff == 0:
            log_info('rows are the same totally')
        else:
            log_info('there are %d difference', diff)

    except Exception, err:
        log_error("error: %s" % err)

    return 0



def check_init():

    src_dbname = ''
    tgt_dbname = ''

    try:
        src_dbname = sai_conf_get('RUNTIME', 'SOURCE').strip()
        tgt_dbname = sai_conf_get('RUNTIME', 'TARGET').strip()

        log_debug('src-db: %s', src_dbname)
        log_debug('tgt-db: %s', tgt_dbname)

        MyCtx.connX     = db_init(src_dbname)
        MyCtx.cursorX   = MyCtx.connX.cursor(as_dict=True)

        MyCtx.connY     = db_init(tgt_dbname)
        MyCtx.cursorY   = MyCtx.connY.cursor(as_dict=True)
    except Exception, err:
        log_error('error: %s', err)

    return 0


def check_doit():

    compare_table()

    return 0


def check_done():

    MyCtx.connX.close()
    MyCtx.connY.close()


def check_run():
    check_init()

    check_doit()

    check_done()

# main()
if __name__=="__main__":
    sailog_set("check.log")
    check_run()


# check.py
