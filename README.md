# CRIMAC-annotationtools

This repository contatin tools to read, convert and write annotatons from the LSSS "work" files. A python representation of the annotations are defined as an annotation structure.

The main obejctive is to have test code that defines the annotations data structure in the ICES format, and code to read and write the format. It is our test implementation of the proposed standard, and the discussions at the ICES publication page should guide our development:
https://github.com/ices-publications/SONAR-netCDF4


## Python representation of the data
A python variable structure for annotations (based on dict?) that is a one to one mapping with the ICES Netcdf4 format is needed.

Based on discussions 06.01.2021 we should probably plan for two different representations. One that is aligned with the data grid, and one that defines the edges of the box that is independent of the data grid. For the first the time vector of the data is needed.

### Annotation grid
1:1 correspondence to the gridded data.

### Ping-range based annotations
Similar to the ICES standard. Independent of the grid.

## Functions

### converters:
- annotation_to_grid - (The grid has to be sparse)
- grid_to_annotation(par=thr) - Turn the pixel based grid into the annotation table. First the data is thresholded, then the neighbouring pixels needs to be connected to a "school", then the connected pixels are written as one object to the annotation table.

### Readers:
- zarrgrid_to_grid - zarrgrid is undefined.
- work_to_annotations - LSSSmaskReader - reads annotations from LSSS work files and info from Simrad raw files (such as ping time)
- ev_to_annotations - reads annotations from Echoviev .ev files
- icesnc_to_annotations - reads annotations from the ICES annotation format

### Writers:
- annotations_to_nc - Writes the ICES acoustic annotation format in necdf 
- annotations_to_work - Writes the ICES acoustic annotation format in necdf 
- annotations_to_zarr - Writes the ICES acoustic annotation format in zarr (resolution specific?)

## Test data
The test data are stored at two different places. It is recommended to first clone the annotation data.

### Annotation data
The annotations are found at
https://github.com/nilsolav/LSSS-label-versioning
and can be clones using

`git clone https://github.com/nilsolav/LSSS-label-versioning`

The repository contatins three different branches.

#### The master branch
The master branch contains the original work files (they also contain nc files for logistical resons; I need to commit them here in order to update the nc2work branch).

`git checkout master`

#### The schoolonly branch
This branch contain the nc files with schools only, i.e. removing layers, erased regions and excluded pings.

`git checkout schoolsonly`

#### The nc2work branch
This branch contain the nc files generated from the original work files and work files generated from the nc files.

`git checkout nc2work`

### Raw data

The raw data is located under the OceanInsight azure storage. Contact nilsolav@hi.no to get access. The data is found under 

`/oceaninsightscience/File Shares/hidata/LSSS-label-versioning/`

And the data should be placed in the folder hiearchy from the cloned repository.

## Starting points:
The code and snippets below are the starting point for the development, in addition to the discussion at the ICES publication page.

### Suggested implementation of the NC format
This is Gavins suggested annotation format, see the CDL files inside for more details. Part of this process will be to refine this and interact with ICES to achieve that end:

https://github.com/nilsolav/EchosounderNetCDF

It is based on his review on how the different software implement the annotations:

https://docs.google.com/document/d/1F5ub9-ElnGWgoFzOhwrNiAB6fZRhKI8Nw6FskMhzI0g/edit#heading=h.ihw2gdxqw9td


### Specific to the U-net implementation
This code has been written to use the U-net algorithm to make predictions and write the predictions to the NC format.

The fiste step generates a (temporary) pickle file from the U-net algorithm. This file could be a starting point for the definition of the python representation of the annotation format. This needs to be refactored to provide the revised annotations variable structure in python:

https://github.com/CRIMAC-WP4-Machine-learning/CRIMAC-classifyers-unet/blob/master/segmentation2nd.py


This function converts the temporary pickle file to nc. This needs to be refactored and use the annotations2nc instead:

https://github.com/CRIMAC-WP4-Machine-learning/CRIMAC-classifyers-unet/blob/master/createncfile.py

### Matlab code that reads the LSSS annotations
This matlab code reads the work files to matlab. This should be the starting point for the work2annotations function:

https://github.com/nilsolav/LSSSreader


### Code for posting NC files into LSSS
We can post the NC file to LSSS via LSSS API (NB use the interpolate branch):

https://github.com/nilsolav/EchosounderNetCDF/tree/interpolate
this part should perhaps be part of the report generation package, but can also stay here. In a sense, it is an alternative to the annotations_to_work function where we post the schools directly and let LSSS generate the work files.


### Docker for creating labels zarr data

This script first convert Marec LSSS' work files into a parquet file containing the annotations using the CRIMAC-annotationtools. These data are overlayed on the grid from sv.zarr generated by CRIMAC-preprocessing-korona, and a pixel wise annotation that matches the grid in the sv.zarr from CRIMAC-preprocessing-korona is generated.

In addition you have to set --env shipID=842 --env parselayers=0 . See details in the Example below

The output of this step is the parquet file: `<OUTPUT_NAME>_labels.parquet` and the Zarr  file: `<OUTPUT_NAME>_labels.zarr`  .


#### Build docker container
```bash
git clone https://github.com/CRIMAC-WP4-Machine-learning/CRIMAC-annotationtools.git
cd CRIMAC-annotationtools
docker build --build-arg=commit_sha=$(git rev-parse HEAD) --build-arg=version_number=$(git describe --tags) --no-cache --tag crimac-annotationtools .

```

#### Run the container

This docker uses the exact same command and parameters as in step 3 of the old crimac preprocessor

```bash
docker run -it \
-v /data/cruise_data/2020/S2020842_PHELMERHANSSEN_1173/ACOUSTIC/EK60/EK60_RAWDATA:/datain \
-v /data/cruise_data/2020/S2020842_PHELMERHANSSEN_1173/ACOUSTIC/LSSS/WORK:/workin \
-v /localscratch/ibrahim-echo/out:/dataout \
--security-opt label=disable \
--env OUTPUT_TYPE=labels.zarr \
--env shipID=842
--env parselayers=0 
--env OUTPUT_NAME=S2020842 \
crimac-annotationtools
```
