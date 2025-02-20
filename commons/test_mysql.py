import pymysql

try:
    conn = pymysql.connect(
        host="34.60.103.140",
        user="usmanzafar@addo.ai",
        password="Addo@123",
        database="gosales",
        port=3306
    )
    cursor = conn.cursor()
    cursor.execute("SELECT NOW();")
    print(cursor.fetchone())
    conn.close()
    print("Connected successfully!")
except pymysql.MySQLError as e:
    print(f"Error: {e}")
