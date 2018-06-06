# -*- coding: UTF-8 -*-

from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *


try:
    conn_X = db_init()
    cursor = conn_X.cursor(as_dict=True)

    fo = open("a.txt", "w")

    cursor.execute('select * from est_t1 order by elem_name')
    row = cursor.fetchone()
    for row in cursor:
        buffer = "%s|Type=%s|MaxLen=%d|Precision=%d|Default=%s|" %  (row['elem_name'].rstrip(), row['elem_type'], row['max_len'], row['elem_prec'], row['def_val'])
        print("%s" % buffer)
        fo.write(buffer+"\n")

    conn_X.close()
    fo.close()
except Exception, err:
    print("error: %s" % err)


#
