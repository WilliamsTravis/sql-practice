# -*- coding: utf-8 -*-
"""
Here, I want to use PostGIS to create a shapefile of our modis-detected burn
events.

Things to do:
    - Transform this into a CLI command with arguments for source and
      destination files. Perhaps also include the ability to use whichever
      driver is available in the gdal universe (at least .shp and .gpkg)
    - Finish the dissolve command - grouped by either id or did (day id)
    - Make sure the setup can be automated in a new environment
    - include the landcover and ecoregion attributes (st_intersects?)

Spyder Editor

This is a temporary script file.
"""
import argparse
import numpy as np
import os
import psycopg2 as psql
from sqlfunctions import initdb, config, runcmd, connect, disconnect
import time
import xarray as xr
psql.extensions.register_adapter(np.int64, psql._psycopg.AsIs)

src_help = """Path to desired fire event data table generated from
               events command. Defaults to 'data/tables/modis_events.csv'"""
ddst_help = """Path to desired daily fire event polygon to be generated from 
               the event data table. Defaults to
              'data/shapefiles/modis_events_daily.shp'"""
edst_help = """Path to desired event-level fire polygons to be generated from 
               the event data table. Defaults to
               'data/shapefiles/modis_events.shp'"""
dscr = """This function uses the results of events.py to build daily and
          event-level polygons for firedpy's MODIS burnt area classified
          wildfire events."""

def polygons():
    start = time.time()
    parser = argparse.ArgumentParser(description=dscr)
    parser.add_argument('-src', dest='src', help= src_help,
                        default='data/tables/modis_events.csv')
    parser.add_argument('-ddst', dest='ddst', help= ddst_help,
                        default='data/shapefiles/modis_events_daily.shp')
    parser.add_argument('-edst', dest='edst', help= edst_help,
                        default='data/shapefiles/modis_events.shp')
    args = parser.parse_args()
    src = args.src
    ddst = args.ddst
    edst = args.edst
    src = os.path.join(os.getcwd(), src)  # <---------------------------------- Correct?
    ddst = os.path.join(os.getcwd(), ddst)
    edst = os.path.join(os.getcwd(), edst)

    # start up and connect to postgres  # <------------------------------------ This sometimes fails on the first attempt...?
    try:
        connect()
    except:
        initdb()
    params = config()
    con = psql.connect(**params)
    con.autocommit = True
    curs = con.cursor()

    # Create a command interface  # <------------------------------------------ I'm not sure this is necessary or preferable
    run = runcmd(curs).run

    # Create table     # <----------------------------------------------------- It might be more efficient to add new events to existing ones and have only one data base. (How to add new observations to existing event records?)
    table = os.path.basename(edst).split('.')[0]
    catalog = run("select * from pg_catalog.pg_tables;")
    tables = [c[1] for c in catalog]        
    if table in tables:  # <--------------------------------------------------- Research primary vs foreign keys (normalizing databases)
        run(f"""
             drop table {table};
             create table {table} (id integer,
                                   tile text, 
                                   date text,
                                   x double precision,
                                   y double precision,
                                   duration integer,
                                   detections integer,
                                   did text);""")
    else:
        run(f"""
             create table {table} (id integer,
                                   tile text, 
                                   date text,
                                   x double precision,
                                   y double precision,
                                   duration integer,
                                   detections integer,
                                   did text);""")

    # Read in the csv
    run(f"""copy {table} (id, tile, date, x, y, duration, detections, did)
            from '{src}' csv header;""")

    # Insert the MODIS CRS into the system and create a spatial table
    try:
        run(f"""
             insert into spatial_ref_sys (srid, auth_name, auth_srid,
             proj4text, srtext) values ( 96842, 'sr-org', 6842, '+proj=sinu 
             +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m 
             +no_defs ', 
             'PROJCS["Sinusoidal",
              GEOGCS["GCS_Undefined",
              DATUM["Undefined", 
              SPHEROID["User_Defined_Spheroid", 6371007.181,0.0]],
              PRIMEM["Greenwich",0.0],
              UNIT["Degree", 0.0174532925199433]],
              PROJECTION["Sinusoidal"],
              PARAMETER["False_Easting",0.0],
              PARAMETER["False_Northing",0.0],
              PARAMETER["Central_Meridian",0.0],
              UNIT["Meter",1.0]]');

             select AddGeometryColumn ('{table}', 'geom', 96842, 'POINT', 2);
             update {table} set geom = ST_SetSRID(ST_MakePoint(x, y), 96842);
            """)
    except:
        run(f"""
             select AddGeometryColumn ('{table}', 'geom', 96842, 'POINT', 2);
             update {table} set geom = ST_SetSRID(ST_MakePoint(x, y), 96842);
             """)

    # Get the resolution from a netcdf  # <------------------------------------ Use the first available netcdf file, this one might not exist
    ref = xr.open_dataset('data/h08v06.nc')
    res = ref.crs.geo_transform[1]
    buff_res = (res / 2) + 1

    # The fastest way I know of is through command line utilities  # <--------- We can also try ogr2ogr to see if there are performance improvements
    host = params['host']
    user = params['user']
    pw = params['password']
    db = params['database']

    # Daily level events
    call = f"pgsql2shp -f {ddst} -h {host} -u {user} -P {pw} {db} "
    query = f"""'SELECT MAX(id) AS id,
                        MAX(detections) AS detections,
                        MAX(duration) AS duration,
                        FIRST_VALUE(date)
                            OVER(PARTITION BY did) AS date,
                        ST_Union(ST_Expand(geom, {buff_res})),
                        did
                 FROM {table}
                 GROUP BY did, date;' """
    command = call + query
    os.system(command)

    # Event level events
    call = f"pgsql2shp -f {edst} -h {host} -u {user} -P {pw} {db} "
    query = f"""'SELECT MAX(detections) AS detections,
                        MAX(duration) AS duration,
                        MAX(date) AS date,
                        ST_Union(ST_Expand(geom, {buff_res})),
                        id
                 FROM {table}
                 GROUP BY id;' """
    command = call + query
    os.system(command)    

    # End Session  # <--------------------------------------------------------- Check that the connection is truly closed
    curs.close()
    con.close()
    disconnect()

    # Signal completion
    end = time.time()
    print(f"Daily polygons saved to {ddst}")
    print(f"Event-level polygons saved to {edst}")
    print("Completed in " + str(round((end - start) / 60, 2)) + " minutes")

if __name__ == "__main__":
    polygons()


