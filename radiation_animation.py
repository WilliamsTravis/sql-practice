# -*- coding: utf-8 -*-
"""
Practicing Postgresql with solar radiation

Created on Sat Aug  3 14:22:16 2019

@author: trwi0358
"""
import cartopy.crs as ccrs
import cartopy.feature as cfeat
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
import os
from pylab import *
import xarray as xr

os.chdir('C:\\Users\\trwi0358\\github\\sql-practice')

# Solar Radiation from John Abatzoglou's team in Idaho
srad_path = ('http://thredds.northwestknowledge.net:8080/thredds/dodsC/MET/' +
             'srad/srad_2019.nc#fillmismatch')
ds = xr.open_dataset(srad_path)
var = list(ds.keys())[0]
ds = ds[var]
dates = ds['day']

def makeFigure():
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    ax.add_feature(cfeat.LAND)
    ax.add_feature(cfeat.OCEAN)
    ax.add_feature(cfeat.COASTLINE)
    ax.add_feature(cfeat.BORDERS, linestyle='dotted')
    return fig, ax

# Create Figure
fig, ax = makeFigure()

# Establish data limits
frames = ds.day.size
minv = ds.min(skipna=True)
maxv = ds.max(skipna=True)

# Define single frame function
def draw(frame, colorbar):
    grid = ds[frame]
    contour = grid.plot(ax=ax, transform=ccrs.PlateCarree(),
                        add_colorbar=colorbar, vmin=minv, vmax=maxv)
    title = u"%s \n %s" % (ds.description, ds.day[frame].values)
    ax.set_title(title)
    return contour

# Initialize function with colorbar
def init():
    return draw(0, colorbar=True)

# Animate function with no colorbar
def animate(frame):
    return draw(0, colorbar=False)

# Create the animation object
anim = animation.FuncAnimation(fig, animate, interval=0.01, blit=False, 
                               init_func=init, repeat=True)
# We might not have made a home for this yet
if not os.path.exists('images'):
    os.mkdir('images')

## Save to an mp4! Will this work in Ubuntu?
anim.save('images/radiation_2019.mp4', writer=animation.FFMpegWriter(fps=8))
plt.close(fig)