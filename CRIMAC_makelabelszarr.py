# -*- coding: utf-8 -*-
"""
CRIMAC labels zarr script
 
Copyright (C) 2023  Institute of Marine Research, Norway.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program; if not, write to the Free Software Foundation,
Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

"""
# Set a the version here
__version__ = 0.2

'''
from echolab2.instruments import EK80, EK60

import sys
import subprocess
import re
import dask
import scipy.ndimage
import numpy as np
import xarray as xr
import zarr as zr
'''
import os.path
import shutil
import glob
import ntpath
import datetime
import gc
import netCDF4

from scipy import interpolate
from psutil import virtual_memory


from annotationtools.crimactools.correct_distping import correct_parquet
from annotationtools.crimactools.parseworkfiles import ParseWorkFiles
from annotationtools.crimactools.writelabelszarr import WriteLabelsZarr
from annotationtools import readers

from rechunker.api import rechunk
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from matplotlib import pyplot as plt, colors
from matplotlib.colors import LinearSegmentedColormap, Colormap
import math
from numcodecs import Blosc


# script version as a string. We need to decide what numbers we should
# use and how to change it depending on changes in the script
version = os.getenv('VERSION_NUMBER')


def writelabels(datain, workin, dataout, OUTPUT_NAME,
                shipID='shipID', parselayers='1'):
    
    sv_file = dataout + '/' + OUTPUT_NAME  + "_sv.zarr"
    pq_filepath = dataout + '/' + OUTPUT_NAME  + "_labels.parquet"
    parser = ParseWorkFiles(rawdir=datain,
                            workdir=workin,
                            pq_filepath=pq_filepath,
                            svzarr_file=sv_file)
    parser.run()
    
    label_file = dataout + '/' + OUTPUT_NAME + "_labels.zarr"
    labelsZarr = WriteLabelsZarr(shipID=shipID,
                                 svzarrfile=sv_file,
                                 parquetfile=pq_filepath,
                                 savefile=label_file,
                                 pingchunk=40000,
                                 parselayers=1)
    labelsZarr.run()


if __name__ == '__main__':
    runtype = os.getenv('OUTPUT_TYPE', 'zarr')
    if (runtype ==  "labels.zarr"):
        writelabels(datain = os.path.expanduser("/datain"),
                    workin = os.path.expanduser("/workin"),
                    dataout = os.path.expanduser("/dataout"),
                    OUTPUT_NAME = os.getenv('OUTPUT_NAME', 'out'),
                    shipID = os.getenv('shipID', 'shipID') ,
                    parselayers = os.getenv('parselayers', '1'))



#workin = os.path.join(os.getenv('CRIMAC'),'2022','S2022611','ACOUSTIC','LSSS','WORK')
#outdir = os.path.join(os.getenv('CRIMACSCRATCH'),'2022','S2022611','ACOUSTIC','GRIDDED')

datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/D2019006/ACOUSTIC/EK80/EK80_RAWDATA'
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/D2019006/ACOUSTIC/GRIDDED'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/D2019006/ACOUSTIC/LSSS/WORK'


os.path.exists(datain)
os.path.exists(workin)
os.path.exists(dataout)
#os.path.exists(sv_file)

shipID = '1172'
OUTPUT_NAME = 'D2019006'



