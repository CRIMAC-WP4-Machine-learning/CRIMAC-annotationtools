# CRIMAC-annotationtools
This repository contatin tools to read, convert and write annotatons from fisheries acoustics. A python representation of the annotations are defined as an annotation structure.

The main obejctive is to define the annotations ddata structure in the ICES format:
https://github.com/ices-publications/SONAR-netCDF4

The repository contains of:

Readers:
- work_to_annotations - LSSSmaskReader - reads annotations from LSSS work files
- ev_to_annotations - reads annotations from Echoviev .ev files
- icesnc_to_annotations - reads annotations from the ICES annotation format

Other functions:

- pingtimereader - reads a .raw file to obtain a ping/time mapping (this is missing from LSSS masks)

The annotation variable structure:

A python variable structure for annotations (based on dict?)

Writers:
- annotations_to_nc - Writes the ICES acoustic annotation format in necdf 
- annotations_to_work - Writes the ICES acoustic annotation format in necdf 
- annotations_to_zarr - Writes the ICES acoustic annotation format in zarr (resolution specific?)

Starting points:
This is Gavins suggested ICESNC annotation format. Part of this process will be to refine this and interact with ICES to achieve that end:

https://github.com/nilsolav/EchosounderNetCDF

See the CDL files inside.


Specific for the U-net implementation:

This one generates a (temporary) pickle file from the U-net algorithm. This needs to be refactored and use the annotations variable structure in python:

https://github.com/CRIMAC-WP4-Machine-learning/CRIMAC-classifyers-unet/blob/master/segmentation2nd.py

This one convert the pickle file to nc. This needs to be refactored and use the annotations2nc instead:

https://github.com/CRIMAC-WP4-Machine-learning/CRIMAC-classifyers-unet/blob/master/createncfile.py

This matlab code reads the work files to matlab. This should be the starting point for the work2annotations function:

https://github.com/nilsolav/LSSSreader


Independent of U-net (to be added to report generation?):

We can also post the NC file to LSSS via LSSS API (NB use the interpolate branch):

https://github.com/nilsolav/EchosounderNetCDF/tree/interpolate

