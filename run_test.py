# This runs sanity checks on the test data
from pathlib import Path
import os
from annotationtools.crimactools.parseworkfiles import ParseWorkFiles
from annotationtools.crimactools.writelabelszarr import WriteLabelsZarr


def writelabels(workdir=None, rawdir=None, zarrdir=None, outdir=None, pq_filepath=None,
                survey='None', shipID='shipID', parselayers='1'):
    print(survey)
    print(zarrdir)
    print(rawdir)
    print(workfile)
    print(outdir)
    print(pq_filepath)
    
    print('\n')
    parser = ParseWorkFiles(rawdir=str(rawdir), workdir=str(workdir),
                            pq_filepath=str(pq_filepath),
                            svzarr_file=str(zarrdir))
    labelsZarr = WriteLabelsZarr(shipID=shipID, svzarrfile=str(zarrdir),
                             parquetfile=pq_filepath, savefile=outdir,
                             pingchunk=40000,  parselayers=0)
    try:
        parser.run()
        labelsZarr.run()
    except:
        print('Parser failed')

crimac = os.getenv('CRIMAC')
crimac_scratch = os.getenv('CRIMACSCRATCH')

# List of test data

workdirs = [d for d in Path(crimac_scratch,'test_data').iterdir() if d.is_dir()]

for test_set in workdirs:
    workdir = Path(test_set, 'ACOUSTIC/LSSS/WORK')
    if workfile.exists():
        survey = str(test_set).split('/')[-1]
        rawdir60 = Path(test_set, 'ACOUSTIC/EK60/EK60_RAWDATA')
        rawdir80 = Path(test_set, 'ACOUSTIC/EK80/EK80_RAWDATA')
        if rawdir60.exists():
            rawdir = rawdir60
        elif rawdir80.exists():
            rawdir = rawdir80
        else:
            rawdir = None
        zarrdir = Path(crimac_scratch, 'test_data_out', survey,
                       'ACOUSTIC/GRIDDED', survey+'_sv.zarr')
        if zarrdir.exists():
            zarrdir = zarrdir
        else:
            zarrdir = None
        outdir = Path(crimac_scratch, 'test_data_out', survey,
                       'ACOUSTIC/GRIDDED')
        if not outdir.exists():
            outdir.mkdir(parents=True, exist_ok=True)
        pq_filepath = Path(crimac_scratch, 'test_data_out', survey,
                           'ACOUSTIC/GRIDDED', survey+"_labels")

        # Convert based on raw file
        if rawdir is not None:
            writelabels(workdir=workdir, rawdir=rawdir, zarrdir=None,
                        outdir=str(Path(outdir, survey+'_labels_sv.zarr')),
                        pq_filepath = str(pq_filepath)+'_raw.parquet', survey=survey)
        # Convert based in zarr file
        if zarrdir is not None:
            writelabels(workdir=workdir, rawdir=None, zarrdir=zarrdir,
                        outdir=str(Path(outdir, survey+'_labels_zarr.zarr')),
                        pq_filepath = str(pq_filepath)+'_zarr.parquet',
                        survey=survey)
    else:
        Print('No workfile for ',+survey)


'''
labelszarrfile = outdir + '/' + OUTPUT_NAME +"_labels.zarr"
'''
