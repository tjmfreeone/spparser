#!/usr/bin/python3
 
import pymysql
 
# 打开数据库连接
db = pymysql.connect("localhost","admin","12qwaszx","test" )
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor(pymysql.cursors.SSDictCursor)
 
 
# 使用预处理语句创建表
sql = """CREATE TABLE IF NOT EXISTS MPLOYEE(
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"""
 
res = cursor.execute("show tables;")
print(cursor.fetchall())
# 关闭数据库连接
db.close()
