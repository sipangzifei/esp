# -*- coding: UTF-8 -*-

from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *
from myfunc  import *




#
def get_order_str(_key_list):
    return ','.join(_key_list)


#

def get_where_str(_keys_list, _row):

    where_section = ""
    for column in _keys_list:
        column = column.strip()

        if len(where_section) > 0:
            where_section += " and "

        where_section += "%s = '%s'" % (column, _row[column])


    return where_section



def compare_two_table(_db, _t1, _t2, _table_config):
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

        # Get order by str
        order_str = get_order_str(key_list)

        # Query DB-A
        sql = "select * from %s order by %s" % (_t1, order_str)
        # log_debug("sql1: %s", sql)
        cursor.execute(sql)
        c1_list = cursor.fetchall()

        for row1 in c1_list:
            name = row1['elem_name'].rstrip()
            # log_debug("[%s]", name)

            # DB_Y
            where_str = get_where_str(key_list, row1)
            sql = "SELECT * FROM %s where %s" % (_t2, where_str)
            # log_debug("sql2: %s", sql)
            cursor.execute(sql)
            c2_list = cursor.fetchall()
            if len(c2_list) == 0:
                # log_debug("sorry: not found: %s", name)
                continue
            row2 = c2_list[0]

            for column in cmp_list:
                column = column.strip()
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
                    v1 = v1.rstrip().encode("gbk")
                    v2 = v2.rstrip().encode("gbk")
                elif v1 is None:
                    # log_debug("---%s: None",  column)
                    pass
                else:
                    log_info("---other")
                    log_info('---type = %s', type(v1))

                # log_debug('---[%s] : [%s]',  v1, v2)

                if v1 != v2:
                    log_info('>>> [%s].[%s]: [%s] -- [%s]', name, column, v1, v2)
                    pass
                else:
                    # log_debug('---the same[%s] : [%s]', v1, v2)
                    pass
            # break

        cursor.close()
    except Exception, err:
        log_error("error: %s" % err)



def test_compare(_t1, _t2, _table_config):
    try:
        conn = db_init()

        compare_two_table(conn, _t1, _t2, _table_config)

        conn.close()
    except Exception, err:
        log_error("error: %s" % err)


# main()
if __name__=="__main__":
    sailog_set("cmp.log")
    t1 = "est_t1"
    t2 = "est_t2"
    table_config = "est_element"
    test_compare(t1, t2, table_config)


#
