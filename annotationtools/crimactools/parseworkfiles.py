import sys
import numpy as np
import xarray as xr
import os.path
from annotationtools import readers
import pyarrow as pa
import pyarrow.parquet as pq
import os
import pandas as pd
import warnings


class ParseWorkFiles:
    def __init__(self, svzarr_file="", workdir="", pq_filepath=""):
        self.workdir = workdir
        self.pq_filepath = pq_filepath
        self.pq_writer = None
        self.svzarr_file = svzarr_file
        self.counter = 0

    def append_to_parquet(self, df, pq_filepath, pq_obj=None):
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
        pa_tbl = pa.Table.from_pandas(df, schema=df_schema,
                                      preserve_index=False)
        if pq_obj == None:
            pq_obj = pq.ParquetWriter(pq_filepath, pa_tbl.schema)
        pq_obj.write_table(table=pa_tbl)
        return pq_obj

    def make_pingtime_rawfile_parquet(self, zarrfile):
        z2 = xr.open_zarr(zarrfile)
        raw_file_array = z2.raw_file.values
        ping_time_array = z2.ping_time.values.astype('datetime64[ns]') 

        # Create a pandas DataFrame
        df = pd.DataFrame({'raw_file': raw_file_array,
                           'ping_time': ping_time_array})
        
        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Define the Parquet file path
        parquet_file = zarrfile.replace("_sv.zarr",
                                        "_ping_time-raw_file.parquet")  
        pq.write_table(table, parquet_file, coerce_timestamps='us',
                       allow_truncated_timestamps=True )
        

    def run(self):
        workfiles = os.listdir(self.workdir)
        sorted_filenames = sorted(workfiles)
        print("----------------")
        print(self.svzarr_file)

        # Write time-ping parquet file from sv zarr
        self.make_pingtime_rawfile_parquet(self.svzarr_file)

        # List of raw files from sv zarr
        z2 = xr.open_zarr(self.svzarr_file)
        rawfilesinzarr = np.unique(z2.raw_file)

        # Loop over work files
        for rawfile in rawfilesinzarr:
            # Build the corresponding work file
            workfile = rawfile[:-4]+'.work'
            print(workfile)
            
            # If the sv files are trunkated due to the U29 bug, we need
            # to compare the first part of the filename only.
            if (str(z2.raw_file.dtype) == '<U29') & (len(rawfile) == 29):
                _workfiles = [workfile[0:29] for workfile in workfiles]
                warnings.warn("The sv file has the <U29 bug on rawfiles"
                              "names. Consider upgrading the sv zarr.")
            else:
                _workfiles = workfiles

            # Check if the work file exists:
            if workfile in _workfiles:
                # Build work file name
                work_fname = os.path.join(self.workdir, workfile)
                #try:
                # Parse the work file
                work = readers.work_reader(work_fname)
                work.work_fname = work_fname
                ann_obj = readers.work_to_annotation(work=work, 
                                                     svzarr=self.svzarr_file)
                df = ann_obj.df_
                if df is not None:
                    self.pq_writer = self.append_to_parquet(
                        df, self.pq_filepath, self.pq_writer)
                '''
                except Exception as e:
                    exception_type, exception_object, exception_traceback = sys.exc_info()
                    filename = exception_traceback.tb_frame.f_code.co_filename
                    line_number = exception_traceback.tb_lineno
                    print("Exception type: ", exception_type)
                    print("File name: ", filename)
                    print("Line number: ", line_number)
                    print("ERROR when reading the WORK file: " + str(
                        work_fname) + " (" + str(e) + ")")
                '''
            else: # end loop
                warnings.warn("There is no corresponding work file for "
                              + rawfile)
                
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
        parser = ParseWorkFiles(rawdir=rawdir,
                                workdir=workdir,
                                pq_filepath=pq_filepath,
                                svzarr_file=svzarr_file)
        parser.run()
