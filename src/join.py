# -*- coding: UTF-8 -*-

from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *
from myfunc  import *




def get_left_join_sql(_table1, _table2, _keys_list):

    on_section = ""
    where_section = ""
    for column in _keys_list:
        column = column.strip()
        if len(on_section) > 0:
            on_section    += " and "

        if len(where_section) > 0:
            where_section += " and "

        on_section    += "a.%s = b.%s" % (column, column)
        where_section += "b.%s is null" % (column)


    sql = "select a.* from %s a left join %s b on %s where %s" % (_table1, _table2, on_section, where_section)


    return sql


def dump_cursor(_table, _cursor, _key_list, _dis_list):
    for row in _cursor:
        buffer  = ""
        buffer1 = ""
        buffer2 = ""
        for column in _key_list:
            column = column.strip()
            if len(buffer1) > 0:
                buffer1 += "| "
            buffer1 += "%s:%s " % (column, row[column].strip())

        for column in _dis_list:
            column = column.strip()
            if len(buffer2) > 0:
                buffer2 += ", "
            buffer2 += "%s" % (row[column].strip())

        # buffer = "%s>>> %s --- %s" % (_table, buffer1, buffer2)
        buffer = "%s>>> %s" % (_table, buffer2)
        log_debug("%s", buffer.encode('gbk'))
    

def join_two_table(_db, _t1, _t2, _table_config):
    try:

        cursor = _db.cursor(as_dict=True)

        # Get config list
        key_list, cmp_list, dis_list = get_config_list(_table_config)
        if len(key_list) == 0:
            log_error("sorry, please config cnf/my.cnf KEY=xxx")
        elif len(cmp_list) == 0:
            log_error("sorry, please config cnf/my.cnf CMP=xxx")
        else:
            # log_debug("%s", cmp_list)
            pass


        # Query 1: A left B
        sql = get_left_join_sql(_t1, _t2, key_list)
        cursor.execute(sql)
        dump_cursor(_t1, cursor, key_list, dis_list)

        log_info("----------------------------------------")


        # Query 2: B left A
        sql = get_left_join_sql(_t2, _t1, key_list)
        cursor.execute(sql)
        dump_cursor(_t2, cursor, key_list, dis_list)


        cursor.close()
    except Exception, err:
        log_error("error: %s",  err)


def test_join(_t1, _t2, _table_config):
    try:
        conn = db_init()

        join_two_table(conn, _t1, _t2, _table_config)

        conn.close()
    except Exception, err:
        log_error("error: %s" % err)


# main()
if __name__=="__main__":
    sailog_set("join.log")
    t1 = "est_t1"
    t2 = "est_t2"
    table_config = "est_element"
    test_join(t1, t2, table_config)

#
