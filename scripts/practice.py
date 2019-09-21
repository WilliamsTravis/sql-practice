# -*- coding: utf-8 -*-
"""
Created on Sun Aug  4 21:07:04 2019

@author: trwi0358
"""
import pandas as pd
import psycopg2 as pq
from sqlfunctions import initdb, config
import xarray as xr

# Initialize database
initdb()

# Grab configuration parameters
params = config()

# Connect to database
con = pq.connect(**params)
con.autocommit = True
curs = con.cursor()

# Initialize a new table
try:
    curs.execute(
      """
      create table modis (id text,
                          tile text, 
                          date text,
                          x text,
                          y text,'
                          duration text,
                          detections text,
                          did text)
      """)
except:
    curs.execute("""drop table modis""")
    curs.execute(
      """
      create table modis (id text,
                          tile text, 
                          date text,
                          x text,
                          y text,
                          duration text,
                          detections text,
                          did text)
      """)

# Import the csv into modis
curs.execute(
   """
   copy modis
   from '/home/travis/github/firedpy/data/tables/modis_events.csv'
   delimiter ',' csv
   """)


# A few sample queries - Show columns
curs.execute("""
  SELECT column_name
  FROM information_schema.columns 
  WHERE table_schema = 'public' 
  AND table_name = 'modis' 
  ORDER BY column_name ASC;
  """)
curs.fetchall()

# Show everything
curs.execute("""select * from modis limit 25;""")
curs.fetchall()

# Show elevations greater than 6,900
curs.execute(
  """
  SELECT *
  FROM modis
  WHERE x < '-10185000';
  """)
curs.fetchall()

# Install PostGIS?
curs.execute(
    """
    create extension postgis;
    create extension postgis_topology;
    """)
curs.fetchall()

# Check for postgis
curs.execute(
    """
     select postgis_full_version();
    """)
curs.fetchall()

# End Session
curs.close()
con.close()
