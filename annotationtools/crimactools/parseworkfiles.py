
from echolab2.instruments import EK80, EK60

import sys
import subprocess
import re
import dask
import scipy.ndimage
import numpy as np
import xarray as xr
import zarr as zr
import os.path
import shutil
import glob
import ntpath
import datetime
import gc
import netCDF4
from scipy import interpolate
from psutil import virtual_memory
from annotationtools import readers
import pyarrow as pa
import pyarrow.parquet as pq
import math
from numcodecs import Blosc
import os
import sys
import pandas as pd

class ParseWorkFiles:
    def __init__(self, rawdir="", workdir="", pq_filepath="",svzarr_file=""):
        self.rawdir = rawdir
        self.workdir = workdir
        self.pq_filepath = pq_filepath
        self.pq_writer = None
        self.svzarr_file = svzarr_file
        self.counter = 0

    def append_to_parquet(self,df, pq_filepath, pq_obj=None):
        # Must set the schema to avoid mismatched schema errors
        fields = [
            pa.field('ping_index', pa.int64()),
            pa.field('ping_time', pa.timestamp('ns')),
            pa.field('mask_depth_upper', pa.float64()),
            pa.field('mask_depth_lower', pa.float64()),
            pa.field('priority', pa.int64()),
            pa.field('acoustic_category', pa.string()),
            pa.field('proportion', pa.float64()),
            pa.field('object_id', pa.string()),
            pa.field('channel_id', pa.string()),
            pa.field('upperThreshold', pa.float64()),
            pa.field('lowerThreshold', pa.float64()),
            pa.field('raw_file', pa.string())
            ]
        df_schema = pa.schema(fields)
        pa_tbl = pa.Table.from_pandas(df, schema=df_schema, preserve_index=False)
        if pq_obj == None:
            pq_obj = pq.ParquetWriter(pq_filepath, pa_tbl.schema)
        pq_obj.write_table(table=pa_tbl)
        return pq_obj

    # Refactor xarray reading:
    def make_pingtime_rawfile_parquet(self, zarrfile ):
        z2 = xr.open_zarr(zarrfile)
        raw_file_array =  z2.raw_file.values
        #ping_time_array = z2.ping_time.values.astype('datetime64[ns]') 
        #print(ping_time_array)
        ping_time_array =z2.ping_time.values.astype('datetime64[ns]') 
         
        print(ping_time_array)
        # Create a pandas DataFrame
        df = pd.DataFrame({'raw_file': raw_file_array, 'ping_time': ping_time_array})
        print(df)
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Define the Parquet file path
        parquet_file = zarrfile.replace("_sv.zarr", "_ping_time-raw_file.parquet")  
        pq.write_table(table, parquet_file, coerce_timestamps='us', allow_truncated_timestamps=True )




    def run(self):
        filenames = os.listdir(self.workdir)
        sorted_filenames = sorted(filenames)
        print("----------------")
        print(self.svzarr_file)
        if self.svzarr_file.endswith(".zarr"):
            self.make_pingtime_rawfile_parquet( self.svzarr_file )

        # Check if work file is found in xarray sv file
        if os.path.exists(self.svzarr_file):
            z2 = xr.open_zarr(self.svzarr_file)
            rawfilesinzarr = np.unique(z2.raw_file)
        else:
            rawfilesinzarr = []

        # Loop over work files
        for filename in sorted_filenames:
            work_fname = os.path.join(self.workdir, filename)
            
            # Check if the file exist and it is a work file
            if work_fname.endswith(".work") & os.path.isfile(work_fname):

                print("---"+filename+"---")

                # Check if raw & idx files exist for current work file
                raw_fname = os.path.join(self.rawdir,
                                         filename.replace("work", "raw"))
                exists_raw = os.path.isfile(raw_fname)
                idx_fname =   raw_fname.replace(".raw",".idx")
                exists_idx = os.path.isfile(idx_fname)

                # Check if raw file is part of the zarr sv file
                if filename.replace("work", "raw") in rawfilesinzarr:
                    exists_svzarr = True
                else:
                    exists_svzarr = False
                    
                ann_obj = None

                # Print status on file existence
                print('svzarr file exists (take presedence over raw files) : '+str(exists_svzarr))
                print('Raw file exists (will not be used if svzarr exists): '+str(exists_raw))
                print('Idx file exists (will not be used if svzarr exists) : '+str(exists_idx))

                try:
                    # Parse work file
                    work = readers.work_reader(work_fname)

                    # Check which pingtime indices to use
                    if exists_svzarr:
                        print("--- ping time from zarr ---")
                        print(self.svzarr_file)
                        ann_obj = readers.work_to_annotation(work, idx_fname,
                                                             self.svzarr_file,
                                                             False)
                        df = ann_obj.df_
                        self.pq_writer = self.append_to_parquet(
                            df, self.pq_filepath, self.pq_writer)
                        
                    elif exists_raw & exists_idx:
                        print("--- Ping time labels from raw & idx ---")
                        print(raw_fname)
                        print(idx_fname)
                        ann_obj = readers.work_to_annotation(work, idx_fname )
                        df = ann_obj.df_
                        self.pq_writer = self.append_to_parquet(
                            df, self.pq_filepath, self.pq_writer)
                    else:
                        print("--- No ping time information for work file ---")
                except Exception as e:
                    exception_type, exception_object, exception_traceback = sys.exc_info()
                    filename = exception_traceback.tb_frame.f_code.co_filename
                    line_number = exception_traceback.tb_lineno
                    print("Exception type: ", exception_type)
                    print("File name: ", filename)
                    print("Line number: ", line_number)
                    print("ERROR3 when reading the WORK file: " + str(
                        work_fname) + " (" + str( e) + ")")
            print('\n') # end work file loop
        self.pq_writer.close()

        # Read and print the Parquet file
        print('Annotation file')
        table = pq.read_table(self.pq_filepath)
        df = table.to_pandas()
        print (df)


if __name__ == '__main__':
    # Run the script from the command line with arguments
    if len(sys.argv) > 1:
        rawdir = ""
        workdir = ""
        pq_filepath = ""
        if "-rawdir" in sys.argv:
            rawdir = str(sys.argv[sys.argv.index("-rawdir") + 1])
        if "-workdir" in sys.argv:
            workdir = sys.argv[sys.argv.index("-workdir") + 1]
        if "-savefile" in sys.argv:
            pq_filepath = sys.argv[sys.argv.index("-savefile") + 1]
        if "-svzarr" in sys.argv:
            svzarr_file = sys.argv[sys.argv.index("-svzarr") + 1]
        parser = ParseWorkFiles(rawdir=rawdir, workdir=workdir, pq_filepath=pq_filepath,svzarr_file=svzarr_file)
        parser.run()
