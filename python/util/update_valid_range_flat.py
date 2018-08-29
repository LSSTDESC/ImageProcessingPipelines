#!/usr/bin/env python3

import sqlite3

conn = sqlite3.connect('calibRegistry.sqlite3')

cursor = conn.cursor()
for table in 'flat'.split():
    sql = 'update {} set calibDate="2022-11-21", validStart="1998-01-01", validEnd="2033-01-01" where id>0'.format(table)
    cursor.execute(sql)
cursor.close()
conn.commit()
