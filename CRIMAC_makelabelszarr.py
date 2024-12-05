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

import os.path
from annotationtools.crimactools.parseworkfiles import ParseWorkFiles
from annotationtools.crimactools.writelabelszarr import WriteLabelsZarr

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
