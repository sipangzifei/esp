
import os,pymssql

server="192.168.163.128"
user="sa"
password="cmbc1234"

conn=pymssql.connect(server,user,password,database="star32_demo")
cursor=conn.cursor()
cursor.execute("""select getdate()""")
row=cursor.fetchone()
while row:
    print("sqlserver version:%s"%(row[0]))
    row=cursor.fetchone()

conn.close()

