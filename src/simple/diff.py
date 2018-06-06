from os import getenv
import pymssql

server = getenv("PYMSSQL_TEST_SERVER")
user = getenv("PYMSSQL_TEST_USERNAME")
password = getenv("PYMSSQL_TEST_PASSWORD")

conn = pymssql.connect(server, user, password, "tempdb")
cursor = conn.cursor(as_dict=True)



######
cursor.execute('SELECT * FROM p2 where id=2')

p2_list = cursor.fetchall()
print("rowcount: %s" % len(p2_list))
print("type: %s" % type(p2_list))

r2 = p2_list[0]
print("row : %s" % r2)
print("type: %s" % type(r2))
######



######
cursor.execute('SELECT * FROM p3 where id=2')
p3_list = cursor.fetchall()
r3 = p3_list[0]
######



cmp_list = r2.keys()
print("cmp : %s" % cmp_list)
print("type: %s" % type(cmp_list))



for column in cmp_list:
    print('column = %s' % (column))
    v1 = r2[column]
    v2 = r3[column]
    print('---type = %s' % type(v1))
    if isinstance(v1, int):
        print("---int")
    elif isinstance(v1, str):
        print("---string")
    elif isinstance(v1, float):
        print("---float")
    elif isinstance(v1, bytes):
        print("---bytes")
    elif isinstance(v1, unicode):
        print("---unicode")
    else:
        print("---other")

    print('---[%s] : [%s]' % (v1, v2))

    if v1 != v2:
        print('---changed [%s] : [%s]' % (v1, v2))
    else:
        print('---the same[%s] : [%s]' % (v1, v2))


conn.close()
