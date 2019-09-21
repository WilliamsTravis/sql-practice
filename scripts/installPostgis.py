#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Install PostGIS plugin

Created on Sun Sep  1 17:20:35 2019

@author: travis
"""
import psycopg2 as pq
from sqlfunctions import initdb, config, runcmd

# Initialize database
initdb()

# Grab configuration parameters
params = config()

# Connect to database
con = pq.connect(**params)
con.autocommit = True
curs = con.cursor()
run = runcmd(curs).run

# Install postgis if not installed already
try:
   r =  run("""
            create extension postgis;
            create extension postgis_topology;
            select postgis_full_version();
            """)
   print("PostGIS installed: ")
   print(str(r))
except:
    r = run("select postgis_full_version();")
    print("PostGIS already installed: ")
    print(str(r))