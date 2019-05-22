#!/usr/bin/env python

import sqlite3
import glob
import os, sys

reg_type = sys.argv[1]
registries=glob.glob(sys.argv[2])
outfile=sys.argv[3]

tmp_registry="./tmp_registry.sqlite3"
if os.path.exists(outfile):
    os.system('cp %s %s'%(outfile, tmp_registry))
    os.system('cp %s %s'%(outfile, outfile+'_bak'))
else:
    os.system('cp %s %s'%(registries[0], tmp_registry))
    registries = registries[1:]
    if len(registries)==0:
        print('copying single file')
        os.system('cp %s %s'%(tmp_registry,outfile))
        sys.exit(0)

if reg_type=="RAW2VISIT":
    tables=['raw','raw_visit']
elif reg_type=="VISIT2TRACT":
    tables=['overlaps',]

db_a = sqlite3.connect(tmp_registry)
db_a_cursor = db_a.cursor()
for registry in registries:
    print(registry)
    db_b = sqlite3.connect(registry)
    db_b_cursor = db_b.cursor()
    for table in tables:
        db_a_cursor.execute("select count(*) as count from %s"%table)
        offset,=db_a_cursor.fetchall()[0]
        print(offset)
        db_b_cursor.execute('SELECT * FROM %s'%table)
        output = db_b_cursor.fetchall()
        #print(output)
        for row in output:
            if table=='raw':
                #raw.id needs to be incremented
                new_row = (row[0]+offset,)+row[1:]
                fmt='(?'+15*',?'+')'
            elif table=='raw_visit':
                new_row=row
                fmt='(?'+3*',?'+')'
            elif table=='overlaps':
                #raw.id needs to be incremented 
                new_row = (row[0]+offset,)+row[1:]
                fmt='(?'+6*',?'+')'
            db_a_cursor.execute('INSERT or REPLACE INTO %s VALUES %s'%(table,fmt), new_row)
            db_a.commit()
    db_b_cursor.close()

db_a_cursor.close()
os.system('mv %s %s'%(tmp_registry,outfile))
