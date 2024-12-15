import os.path
from annotationtools.crimactools.parseworkfiles import ParseWorkFiles
from annotationtools.crimactools.writelabelszarr import WriteLabelsZarr
import annotationtools.readers.convert_to_annotations
import annotationtools.readers
import annotationtools
from annotationtools import readers
from CRIMAC_makelabelszarr import writelabels

'''
# Directly on work_reader
work_fname = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2024002006_PJOHANHJORT_1019/ACOUSTIC/LSSS/WORK/2024002006-D20240513-T204721.work'
sv_fname = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2024002006_PJOHANHJORT_1019/ACOUSTIC/GRIDDED/S2024002006_PJOHANHJORT_1019_sv.zarr'
raw_fname =  '/mnt/c/DATAscratch/crimac-scratch/test_data/S2024002006_PJOHANHJORT_1019/ACOUSTIC/EK80/EK80_RAWDATA/2024002006-D20240513-T204721.raw'
work = readers.work_reader(work_fname)
# Boundaries ok
ann_sv = readers.work_to_annotation(work, raw_fname, sv_fname, False)
nils = ann_sv.df_
avv_raw = readers.work_to_annotation(work, raw_fname)
'''

# Used this to fix the datetime error in excluded regions
'''
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/D2023001/ACOUSTIC/GRIDDED/'
datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/D2023001/ACOUSTIC/EK80/EK80_RAWDATA'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/D2023001/ACOUSTIC/LSSS/WORK'
shipID = 'Unknown'
OUTPUT_NAME = 'D2023001'
'''

# There were an error in the handling of bubble corrections (missing information)
# There were also a problem with indexing using ping numbers (substract 1!)
'''
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2005114/ACOUSTIC/GRIDDED/'
datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2005114/ACOUSTIC/EK60/EK60_RAWDATA'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2005114/ACOUSTIC/LSSS/WORK'
shipID = 'Unknown'
OUTPUT_NAME = 'S2005114'
'''

# This one had an error with the exclude region. Fixed by changing how the array is reshaped.
'''
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2014119/ACOUSTIC/GRIDDED'
datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2014119/ACOUSTIC/EK60/EK60_RAWDATA'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2014119/ACOUSTIC/LSSS/WORK/'
shipID = 'Unknown'
OUTPUT_NAME = 'S2014119'
'''
# This one had also an error with the exclude region. Fixed by changing how the array is reshaped.

'''
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2017838/ACOUSTIC/GRIDDED'
datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2017838/ACOUSTIC/EK60/EK60_RAWDATA'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2017838/ACOUSTIC/LSSS/WORK'
shipID = 'Unknown'
OUTPUT_NAME = 'S2017838'
'''

# ?
'''
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2014807/ACOUSTIC/GRIDDED'
datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2014807/ACOUSTIC/EK60/EK60_RAWDATA'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2014807/ACOUSTIC/LSSS/WORK'
shipID = 'Unknown'
OUTPUT_NAME = 'S2014807'
'''


datain = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2024002006_PJOHANHJORT_1019/ACOUSTIC/EK80/EK80_RAWDATA/'
workin = '/mnt/c/DATAscratch/crimac-scratch/test_data/S2024002006_PJOHANHJORT_1019/ACOUSTIC/LSSS/WORK/'
dataout = '/mnt/c/DATAscratch/crimac-scratch/test_data_out/S2024002006_PJOHANHJORT_1019/ACOUSTIC/GRIDDED/'
OUTPUT_NAME = 'S2024002006_PJOHANHJORT_1019'
shipID = '1019'

writelabels(datain = datain,
            workin = workin,
            dataout = dataout,
            OUTPUT_NAME = OUTPUT_NAME,
            shipID = shipID)

'''

cr = '/data/cruise_data/2024/S2024002006_PJOHANHJORT_1019/'
datain = cr+'ACOUSTIC/EK80/EK80_RAWDATA/'
workin = cr+'ACOUSTIC/LSSS/WORK/'
dataout = '/data/crimac-scratch/staging/2024/S2024002006/ACOUSTIC/GRIDDED/'
output_name = 'S2024002006'
shipID = '1019'

writelabels(datain = datain,
            workin = workin,
            dataout = dataout,
            OUTPUT_NAME = output_name,
            shipID = shipID)

cr = '/data/cruise_data/2023/S2023006009_PKRISTINEBONNEVIE_1172/'
datain = cr + 'ACOUSTIC/EK80/EK80_RAWDATA/'
workin = cr + '/ACOUSTIC/LSSS/WORK/'
dataout = '/data/crimac-scratch/staging/2023/S2023006009/ACOUSTIC/GRIDDED/'
output_name = 'S2023006009'
shipID = '1172'

cr = '/data/cruise_data/2022/S2022611_PKRISTINEBONNEVIE_1172/'
datain = cr+'ACOUSTIC/EK80/EK80_RAWDATA/'
workin = cr+'ACOUSTIC/LSSS/WORK/'
dataout = '/data/crimac-scratch/staging/2022/S2022611/ACOUSTIC/GRIDDED/'
output_name = 'S2022611'
shipID = '1019'


# open xarray
import xarray as xr
sv = dataout+output_name+'_sv.zarr'
sv_zarr = xr.open_zarr(sv)

# Generate labels
writelabels(datain = datain,
            workin = workin,
            dataout = dataout,
            OUTPUT_NAME = output_name,
            shipID = shipID)
'''
