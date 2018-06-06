# -*- coding: UTF-8 -*-

from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *

from cmp     import *
from join    import *




def run():
    sailog_set("run.log")

    argv = sai_get_args()

    if len(argv) != 3:
        print("error: invalid usage!")
        print("-- python run.py src-table dst-table config-table")
        print("-- python run.py dev.dbo.est_element uat.dbo.est_element est_element")
        return -1

    t1 = argv[0]
    t2 = argv[1]
    table_config = argv[2]
    log_debug("config table: [%s]", table_config)
    log_debug("left  table:  [%s]", t1)
    log_debug("right table:  [%s]", t2)

    conn = db_init()

    log_debug("compare result:")
    compare_two_table(conn, t1, t2, table_config)

    log_debug("join    result:")
    join_two_table(conn, t1, t2, table_config)

    log_debug("------------------------------------------------------------------------")



# main
run()


#
