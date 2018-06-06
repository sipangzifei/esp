# -*- coding: UTF-8 -*-


#from sailog  import *
#from saiconf import *
#from saiutil import *
from saidb   import *


conn = db_init()

cursor = conn.cursor(as_dict=True)

#cursor.execute('sp_columns est_element')
cursor.execute('select * from est_element_log')

for row in cursor:
    # print('row = %s -- %s' % (row['COLUMN_NAME'], row))
    print('row = %s' % row)

