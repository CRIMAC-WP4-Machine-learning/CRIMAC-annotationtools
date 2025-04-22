# This runs sanity checks on the test data
from pathlib import Path
import subprocess
import os
import xarray as xr
import numpy as np
import json

crimac_scratch = os.getenv('CRIMACSCRATCH')

# List of test data
workdirs = [d for d in Path(crimac_scratch,
                            'test_data').iterdir() if d.is_dir()]
testvalues = {}

for test_set in workdirs:
    workin = Path(test_set, 'ACOUSTIC/LSSS/WORK')
    if workin.exists():
        survey = str(test_set).split('/')[-1]
        rawdir60 = Path(test_set, 'ACOUSTIC/EK60/EK60_RAWDATA')
        rawdir80 = Path(test_set, 'ACOUSTIC/EK80/EK80_RAWDATA')
        if rawdir60.exists():
            rawin = rawdir60
        elif rawdir80.exists():
            rawin = rawdir80
        else:
            rawin = None

        griddedout = Path(crimac_scratch, 'test_data_out', survey,
                          'ACOUSTIC/GRIDDED')

        command = [
            "docker", "run", "-it", "--rm",
            "-v", str(griddedout)+':/griddedout',
            "-v", str(rawin)+':/rawin',
            "-v", str(workin)+':/workin',
            "--security-opt", "label=disable",
            "--env", "OUTPUT_TYPE=labels.zarr",
            "--env", "shipID=Unknown",
            "--env", "parselayers=1",
            "--env", "OUTPUT_NAME="+survey,
            "crimac-annotationtools:latest"]

        # Run the command
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with error: {e}")
    else:
        print('No workfile for '+survey)

