from os import getenv
import pymssql

server = getenv("PYMSSQL_TEST_SERVER")
user = getenv("PYMSSQL_TEST_USERNAME")
password = getenv("PYMSSQL_TEST_PASSWORD")

conn = pymssql.connect(server, user, password, "tempdb")
cursor = conn.cursor(as_dict=True)

cursor.execute("""
CREATE PROCEDURE FindPerson
@name VARCHAR(100)
AS BEGIN
SELECT * FROM persons WHERE name = @name
END
""")

cursor.callproc('FindPerson', ('Jane Doe',))

for row in cursor:
    print("ID=%d, Name=%s" % (row['id'], row['name']))

conn.close()
