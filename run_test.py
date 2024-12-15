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
            datain = rawdir60
        elif rawdir80.exists():
            datain = rawdir80
        else:
            datain = None

        dataout = Path(crimac_scratch, 'test_data_out', survey,
                       'ACOUSTIC/GRIDDED')

        command = [
            "docker", "run", "-it", "--rm",
            "-v", str(dataout)+':/dataout',
            "-v", str(datain)+':/datain',
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
            sv = xr.open_zarr(Path(dataout, survey+'_sv.zarr'))
            labels = xr.open_zarr(Path(dataout, survey+'_labels.zarr'))

            _testvalue = (sv.sv.sum(dim='frequency')*labels.annotation).sum(
                dim=['range', 'ping_time', 'category'])
            testvalues[survey] = {'Status': 'OK',
                                  'commit_sha': labels.commit_sha,
                                  'sv_commit_sha': sv.git_commit,
                                  'checksum': float(_testvalue.values)}
        except subprocess.CalledProcessError as e:
            print(f"Command failed with error: {e}")
            testvalues[survey] = 'Failed'
            testvalues[survey] = {'Status': 'Failes',
                                  'commit_sha': labels.commit_sha,
                                  'sv_commit_sha': sv.git_commit,
                                  'checksum': np.nan}

    else:
        print('No workfile for '+survey)

with open("run_test_updated.json", "w") as file:
    json.dump(testvalues, file)

with open("run_test.json", "r") as file:
    testvalues_0 = json.load(file)

# Print test results
for key in testvalues.keys():
    if testvalues[key]['checksum'] is np.nan:
        d = 'nan'
    if testvalues[key]['checksum'] == 0:
        d = str(testvalues_0[key]['checksum']-testvalues[key][
            'checksum'])
    else:
        d = str(np.round(100*(testvalues_0[key]['checksum']-testvalues[key][
            'checksum'])/testvalues[key]['checksum']))+'%'

    print(key+': original checksum: '+str(
        testvalues_0[key]['checksum']) + ', Updated checksum: ' + str(
            testvalues[key]['checksum']), ', diff: ' + d)
