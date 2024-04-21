import mysql.connector
import pandas as pd
from mysql.connector import errorcode

config={
    'host': 'cloudprojectsql.mysql.database.azure.com',
    'user': 'maanasa999',
    'password': 'Saibaba@162',
    'database': 'team36'
    }
try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

cursor = conn.cursor()

# cursor.execute("DROP TABLE IF EXISTS households;")
cursor.execute("""CREATE TABLE households (HSHD_NUM int, L varchar(255), AGE_RANGE varchar(255),MARITAL varchar(255),
INCOME_RANGE varchar(255),HOMEOWNER varchar(255),HSHD_COMPOSITION varchar(255),HH_SIZE varchar(255),CHILDREN varchar(255),PRIMARY KEY (HSHD_NUM));
""")
#
# df = pd.read_csv("D:\\Mohan_Work\\Graduation\CloudComputing\\KrogerData\\400_products.csv")
# df.columns = df.columns.str.replace(' ', '')
# df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# dftotuple = list(df.to_records(index=False))
# for eachtuple in dftotuple:
#     try:
#         cursor.execute(
#             """INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s,%s,%s,%s,%s)""",
#             (int(eachtuple.PRODUCT_NUM), str(eachtuple.DEPARTMENT), str(eachtuple.COMMODITY), str(eachtuple.BRAND_TY),
#              str(eachtuple.NATURAL_ORGANIC_FLAG)));
#     except Exception as e:  # work on python 3.x
#         print('Failed to upload to ftp: ' + str(e))
# cursor.execute("Select * from households");
# rows = cursor.fetchall()
# print("Read", cursor.rowcount, "row(s) of data.")
# for row in rows:
#     print(row)
    # print("Data row = (%s, %s, %s, %s, %s, %s, %s, %s, %s)" % (int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]), str(row[6]), str(row[7]), str(row[8])))
conn.commit()
cursor.close()
conn.close()
