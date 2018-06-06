from os import getenv
import pymssql

server = getenv("PYMSSQL_TEST_SERVER")
user = getenv("PYMSSQL_TEST_USERNAME")
password = getenv("PYMSSQL_TEST_PASSWORD")

conn = pymssql.connect(server, user, password, "tempdb")

c1 = conn.cursor()
c1.execute('SELECT * FROM persons')
c1_list = c1.fetchall()

c2 = conn.cursor()
c2.execute('SELECT * FROM persons WHERE salesrep=%s', 'John Doe')
c2_list = c2.fetchall()

print( "all persons" )
print( c1_list )  # shows result from c2 query!

print( "John Doe" )
print( c2_list )  # shows no results at all!

print( "c1_list" )
for row in c1_list:
    print('row = %r' % (row,))


conn.close()
