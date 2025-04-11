# -*- coding: utf-8 -*-
"""
CRIMAC labels zarr script
 
Copyright (C) 2025  Institute of Marine Research, Norway.

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

from pathlib import Path
import os.path
import subprocess
import xarray as xr
import os
import zarr

# Script version as a string.
version = os.getenv('VERSION_NUMBER')
commit_sha = os.getenv('COMMIT_SHA')


def writelabels(rawin, workin, griddedout, OUTPUT_NAME,
                shipID='shipID', parselayers='1',
                version = 'NaN',
                commit_sha = 'NaN'):

    # List the rawin directory
    rawin_path = Path(rawin)
    workin_path = Path(workin)
    griddedout_path = Path(griddedout)

    # List all files in the directory
    print('shipID')
    print(shipID)
    print('Version')
    print(version)
    print('commit_sha')
    print(commit_sha)
    print(rawin)
    print([file for file in rawin_path.iterdir() if file.is_file()])
    print(workin)
    print([file for file in workin_path.iterdir() if file.is_file()])
    print(griddedout)
    print([file for file in griddedout_path.iterdir() if file.is_file()])

    batch = '/lsss-3.1.0-alpha/korona/KoronaCli.sh'

    '''
    Bruk `KoronaCli.sh work-file-to-netcdf -h` for å se kommandolinjeparametere.
    Eksempel:
    `KoronaCli.sh work-file-to-netcdf --data-dir rawDir --work-dir workDir --output-dir outDir --delta-range 0.2 --max-range 500`
    '''
    subprocess.run(['pip', 'freeze'])
    
    cmdstr = [batch, 'work-file-to-netcdf', '--data-dir', rawin,
              '--work-dir', workin, '--output-dir', griddedout+'/labelsnc',
              '--delta-range', '0.2', '--max-range', '500']

    print(cmdstr)
    subprocess.run(cmdstr)

    # open nc files
    nc_files = [griddedout+'/labelsnc/'+f for f in os.listdir(
        griddedout+'/labelsnc') if f.endswith('.nc')]
    print(nc_files)
    combined = xr.open_mfdataset(nc_files, chunks={}, combine='by_coords')

    # Rechunk
    combined = combined.chunk({'ping_time': 1000, 'range': 2500})

    # Change time and range vector based on the sv data
    sv = xr.open_zarr(os.path.join(griddedout, f"{OUTPUT_NAME}_sv.zarr"))
    combined['ping_time'] = sv['ping_time']
    combined['range'] = sv['range']
    
    # Write to zarr
    combined.to_zarr(os.path.join(griddedout, f"{OUTPUT_NAME}_labels.zarr"), mode='w')
    zarr.consolidate_metadata(os.path.join(griddedout, f"{OUTPUT_NAME}_labels.zarr"))

    # Read back the Zarr file
    zarr_file_path = os.path.join(griddedout, f"{OUTPUT_NAME}_labels.zarr")
    combined_readback = xr.open_zarr(zarr_file_path)
    print(combined_readback)

    # Delete the nc files
    for file in nc_files:
        os.remove(file)  # Remove the file
        print(f'Deleted: {file}')
    os.rmdir(griddedout+'/labelsnc/')


if __name__ == '__main__':
    writelabels(rawin = os.path.expanduser("/rawin"),
                workin = os.path.expanduser("/workin"),
                griddedout = os.path.expanduser("/griddedout"),
                OUTPUT_NAME = os.getenv('OUTPUT_NAME', 'out'),
                shipID = os.getenv('shipID', 'shipID') ,
                parselayers = os.getenv('parselayers', '1'),
                version = os.getenv('VERSION_NUMBER'),
                commit_sha = os.getenv('COMMIT_SHA'))
