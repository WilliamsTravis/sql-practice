# -*- coding: utf-8 -*-
"""
Practicing Postgresql with solar radiation

Created on Sat Aug  3 14:22:16 2019

@author: trwi0358
"""
import psycopg2 as pq
import os
import site
import subprocess as sp

# A few steps to initialize postgresql
sqlenv = site.getsitepackages()[0]
pgdata = os.path.join(sqlenv, 'pgdata')
env = os.environ.copy()
env['PGDATA'] = pgdata

# If we are starting over
if not os.path.exists(pgdata):
    print("Creating postgres data folder")
    os.mkdir(pgdata)

    # Set path and initialize 
    r = sp.Popen(['pg_ctl', 'initdb'], stderr=sp.STDOUT, stdout=sp.PIPE,
                 env=env)
    statement = str(r.stdout.read()) 
    print(statement)

# What's in these files?
pg_hba = open(os.path.join(pgdata, 'pg_hba.conf')).readlines()
pg_ident = open(os.path.join(pgdata, 'pg_ident.conf')).readlines()
postgresql = open(os.path.join(pgdata, 'postgresql.conf')).readlines()

# Start service
r = sp.Popen(['pg_ctl', 'start'], stderr=sp.STDOUT, stdout=sp.PIPE, env=env)  

# Connect to postgres and create gridmet database
try:
    con = pq.connect(dbname='gridmet', user='trwi0358')
except:
    con = pq.connect(dbname='postgres', user='trwi0358')
    con.autocommit = True
    crsr = con.cursor()
    crsr.execute('CREATE DATABASE %s ;' % 'gridmet')
    con = pq.connect(dbname='gridmet', user='trwi0358')
con.autocommit = True
crsr = con.cursor()
crsr.execute("""
             CREATE TABLE srad (?)
             """)
#except pq.DatabaseError as e:
#    print(e)

print(con.get_dsn_parameters(), "\n")
crsr.execute("SELECT version();")
record = crsr.fetchone()
print(record, "\n")

# Close service
r = sp.Popen(['pg_ctl', 'stop'], stderr=sp.STDOUT, stdout=sp.PIPE, env=env)
print(r.stdout.read())
