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
    survey = str(test_set).split('/')[-1]
    griddedout = Path(crimac_scratch, 'test_data_out', survey,
                      'ACOUSTIC/GRIDDED')
    
    # Run the command
    try:
        sv = xr.open_zarr(Path(griddedout, survey+'_sv.zarr'))
        labels = xr.open_zarr(Path(griddedout, survey+'_labels.zarr'))
        _testvalue = (sv.sv.sum(dim='frequency')*labels.annotation).sum(
            dim=['range', 'ping_time', 'category'])
        testvalues[survey] = {'Status': 'OK',
                              'labels_git_commit': labels.git_commit,
                              'sv_git_commit': sv.git_commit,
                              'checksum': float(_testvalue.values)}
    except:
        print(f"Command failed")
        testvalues[survey] = 'Failed'
        testvalues[survey] = {'Status': 'Failes',
                              'labels_git_commit': labels.git_commit,
                              'sv_git_commit': sv.git_commit,
                              'checksum': np.nan}

with open("run_test_updated.json", "w") as file:
    json.dump(testvalues, file)

with open("run_test.json", "r") as file:
    testvalues_0 = json.load(file)

# Print test results
for key in testvalues.keys():
    if testvalues[key]['checksum'] is np.nan:
        d = 'nan'
    if testvalues[key]['checksum'] == 0:
        d = str(-testvalues_0[key]['checksum']-testvalues[key][
            'checksum'])
    else:
        d = str(-np.round(100*(testvalues_0[key]['checksum']-testvalues[key][
            'checksum'])/testvalues[key]['checksum']))+'%'

    print(key+': original checksum: '+str(
        testvalues_0[key]['checksum']) + ', Updated checksum: ' + str(
            testvalues[key]['checksum']), ', diff: ' + d)


