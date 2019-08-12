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

# Read in a simple data frame (flow histories from McPhee Reservoir)
df = pd.read_csv(r'C:/Users/trwi0358/github/NASA_ESP_VOI/sub_repos/'  + 
                 'game_models/data/elevations/mcphee.csv')
df = df[['date', 'elev_ft', 'evap_af', 'inflow_cfs', 'release_cfs',
         'storage_cfs', 'lat', 'lon']]
cols = list(df.columns)
cols.pop(0)
df[cols] = df[cols].astype(float)
df['id'] = df.index

# Initialize a new table
try:
    curs.execute(
      """
      create table mcphee (date text, elev_ft DOUBLE PRECISION,
                           evap_af DOUBLE PRECISION, 
                           inflow_cfs DOUBLE PRECISION,
                           release_cfs DOUBLE PRECISION,
                           storage_cfs DOUBLE PRECISION,
                           lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                           id integer primary key)
      """)
except:
    curs.execute("""drop table mcphee""")
    curs.execute(
      """
      create table mcphee (date text, elev_ft DOUBLE PRECISION,
                           evap_af DOUBLE PRECISION, 
                           inflow_cfs DOUBLE PRECISION,
                           release_cfs DOUBLE PRECISION,
                           storage_cfs DOUBLE PRECISION,
                           lat DOUBLE PRECISION, lon DOUBLE PRECISION,
                           id integer primary key)
      """)

# Data type issues, I can't seem to convert away from numpy
rows = [list(row) for row in df.itertuples(index=False)] 
for row in rows:
    cmd = "insert into mcphee values "
    cols = ", ".join(['%s' for i in range(len(row))])
    call = cmd + "(" + cols + ")"
    curs.execute(call, row)

# A few sample queries - Show columns
curs.execute("""
  SELECT column_name
  FROM information_schema.columns 
  WHERE table_schema = 'public' 
  AND table_name = 'mcphee' 
  ORDER BY column_name ASC;
  """)
curs.fetchall()

# Show everything
curs.execute("""select * from mcphee""")
curs.fetchall()

# Show elevations greater than 6,900
curs.execute("""
  SELECT *
  FROM mcphee
  WHERE elev_ft > 6900
  order by elev_ft desc;
  """)
curs.fetchall()

# End Session
curs.close()
con.close()

# Solar Radiation from John Abatzoglou's team in Idaho
srad_path = ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/' +
             'srad/srad_2019.nc#fillmismatch')
data = xr.open_dataset(srad_path)
