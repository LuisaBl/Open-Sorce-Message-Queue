import mysql.connector

mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root12345',
    port='3306',
    database='marbach_test'
)

mycursor = mydb.cursor()

mycursor.execute('SELECT * FROM marbach')

marbach = mycursor.fetchall()

for user in marbach:
    print(user[0])