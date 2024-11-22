# -*- coding: utf-8 -*-


#Load som packages
import numpy as np
import xmltodict,os, struct, sys
from datetime import datetime
from echolab2.instruments import EK60, EK80
import pandas as pd
from echolab2.instruments.util.date_conversion import nt_to_unix, unix_to_datetime
from echolab2.instruments.util.simrad_raw_file import RawSimradFile
import decimal
import numpy as np
import xarray as xr
import pyarrow as pa
import pyarrow.parquet as pq

class rename_LSSS_vocab_to_ICES_vocab (object):
    """
    function to rename vocabulary for LSSS acoustic category to ices vocab

    Usage:
        annotation :    pandas data tableas reported from work_to_annotation
        link:           dictionary for user specific definition, i.e. {12:'HER'}
        reference_file: download reference file from IMR web portal. NOT AVALIABLE!!!
        set_UKN:        set to True to convert all non specified acoustic category to ices unknown 'UKN'
    """
    def __init__(self,annotation = None, link=None,reference_file=False,set_UKN=False):

        if link==None and reference_file == False:
            print('Either link or refrence_file must be specified')
            return


        if link==None and reference_file != False:
            print('A reference file is not yet avaliable')
            return

        if link!=None:
            #Change acousticCat for noise to 'D10'
            annotation['acoustic_category'][(annotation['acoustic_category']=='0.0') | (annotation['acoustic_category']=='0')]='D10'


            #Change acousticCat for missing data to NA
            annotation['acoustic_category'][(annotation['acoustic_category']=='-1.0') | (annotation['acoustic_category']=='-1')]='NAN'

            #Convert all other fields as specified from link table
            for link_i in link:
                annotation['acoustic_category'][(annotation['acoustic_category']==link_i) | (annotation['acoustic_category']==link_i+'.0')] = link[link_i]

            if set_UKN == True:
                annotation['acoustic_category'][[item not in ['D10','NAN']+list(link.values())  for item in annotation['acoustic_category']]]='UKN'

        self.annotation = annotation



class work_reader (object):

    """Function for reading the LSSS .work files including the interpretation
    masks linked to ecousounder files

    Author:
        Sindre Vatnehol
        Institue of Marine Research
        mail: sindre.vatnehol@hi.no


    Input:
        work_filename:  file name or file path of a .work file

    Output:
        Tree/list structure for storing the interpretation masks



    Internal datastructure description:

        ######################################################################
        General information:
        ######################################################################
        .info                               - structure for storing general info
        .info.numberOfPings                 - integer storing the number of pings in the acoustic file
        .info.timeFirstPing                 - float storing the time of first ping in UNIX time




        ######################################################################
        Excluded region information:
        Excluded region indicates that full pings within an time intervall is
        excluded for further analysis
        ######################################################################
        .excluded                           - structure for storing excluden regions
        .excluded.start_time                - list storing the start time of each excluded region in UNIX time
        .excluded.numOfPings                - list storing the number of each ping for each excluded region




        ######################################################################
        Erased mask information:
        Erased mask indicate the pixels that are excluded from further analysis

        The mask is developed from channel ID (linking frequency), ping number
        (linking time), and paired depths (linging range).

        The paired depths are structured as followed:
            [100, 50]       - indicates that all depths from 100 to 150 are exclued
            [100,50,50,50]  - indicates that all depths from 100 to 150,
                              and 200 to 250 is excluded. pings 150 to 200 is
                              thus included
        ######################################################################
        .erased                           - a datastructure including information of erased mask
        .erased.referenceTime             - a float indicating the reference UNIX time, usually the time of the first erased pixel
        .erased.masks                     - a list containing datastructures
        .erased.masks[i]                  - a datastructe of the i´th mask
        .erased.masks[i].channelID        - a list containing the channel ID nuber(i.e. 1, 2, 3, ...)
        .erased.masks[i].pingOffset       - a list of containing the ping number (instead of time)
        .erased.masks[i].depth            - a list of paired depth intervalls




        ######################################################################
        School box information:
        School box includes the box masks and the interpretation, i.e. the
        proportion the acoustic pixels within the box should be integrated to
        different acoustic cathegories
        ######################################################################
        .school                                     - list containing datastructure
        .school[i].referenceTime                    - a float of the UNIX time of the first ping in the si'th chool box
        .school[i].objectNumber                     - a integer reference to the school ID number
        .school[i].relativePingNumber               - a list of ping numbers, this indicates the time domaine of the box
        .school[i].min_depth                        - a list of the upper shape of the box
        .school[i].max_depth                        - a list of the lower shape of the box
        .school[i].interpretation                   - a list of datastructure including interpretation on each channel
        .school[i].interpretations[ii].frequency    - the frequency that has been interperated on
        .school[i].interpretations[ii].species_id   - a list of the different acoustic cathegories
        .school[i].interpretations[ii].fraction     - a list of the proportions to be used in the integration





        ######################################################################
        Layer information:
        The layer information includes the masks and interpretation.
        This is similar to the school box, but the school has a higher higherarcy.
        I.e. a box and a layer can overlap, but the pixels within the school uses
        the interpretation information from the school and not from the layer.
        ######################################################################
        .layer                                  - a list containing datastructures
        .layer[i].restSpecies                   - a str that reference to what has been used as the rest in proportion
        .layer[i].interpretation                - a list of datastructure including interpretation on each channel
        .layer[i].interpretation[ii].frequency  - the frequency that has been interpretated on
        .layer[i].interpretation[ii].species_id - a list of the different acoustic cathegories
        .layer[i].interpretation[ii].fraction   - a list of the proportions to be used in the integration
        .layer[i].boundaries                    - a datastructure including the masks
        .layer[i].boundaries.ID                 - a int refereing to the layer id
        .layer[i].boundaries.ping               - a list of the ping number
        .layer[i].boundaries.depths_upper       - a list of the upper shape of the layer
        .layer[i].boundaries.depths_lower-      - a list of the lower shape of the layer




        TODO: add bubble and threshold information

    """






    def __init__(self,work_filename=''):



        #Define a structure type that is used to output the data
        class structtype():
            pass



        #define the first level of the output format
        self.info = structtype()





        #msg for user
        print('Reading:' + work_filename)




        #Parse the LSSS work file to a dictionary
        with open(work_filename) as fd:
            doc = xmltodict.parse(fd.read())




        ####################################################################
        #Grab information that is linked to the information section
        ####################################################################
        self.info.numberOfPings = int(doc['regionInterpretation']['timeRange']['@numberOfPings'])
        self.info.timeFirstPing = float(doc['regionInterpretation']['timeRange']['@start'])





        ####################################################################
        #Procesing the information for exclude region
        ####################################################################

        #checking if the attribute is not missing (null) and if it is not empty
        parseelement = 0
        try:
            doc['regionInterpretation']['exclusionRanges']

            parseelement = 1
        except:
            parseelement = 0
            print("ERROR: LSSS work file :: in  Procesing the information for exclude region (null)")

        if parseelement > 0 and not not(doc['regionInterpretation']['exclusionRanges']):

            self.exclude = structtype()

            #Define the structure of the excluded part
            self.exclude.start_time = list()
            self.exclude.numOfPings = list()


            #Check if there is more than one excluded region and fill inn data
            if type(doc['regionInterpretation']['exclusionRanges']['timeRange'])!=list:
                timeRange = doc['regionInterpretation']['exclusionRanges']['timeRange']
                numOfPings = int(timeRange['@numberOfPings'])
                start_time = float(timeRange['@start'])
                self.exclude.start_time = np.hstack((self.exclude.start_time,start_time))
                self.exclude.numOfPings = np.hstack((self.exclude.numOfPings,numOfPings))
            else:
                for timeRange in doc['regionInterpretation']['exclusionRanges']['timeRange']:
                    numOfPings = int(timeRange['@numberOfPings'])
                    start_time = float(timeRange['@start'])
                    self.exclude.start_time = np.hstack((self.exclude.start_time,start_time))
                    self.exclude.numOfPings = np.hstack((self.exclude.numOfPings,numOfPings))




        ####################################################################
        #Procesing the information for the erased masks
        ####################################################################

        #check if there is any inforamtion of erased masks

        #checking if the attribute is not missing (null) and if it is not empty
        parseelement=0
        try:
            doc['regionInterpretation']['masking']
            parseelement = 1
        except :
            parseelement = 0
            print("ERROR: LSSS work file :: in  Procesing the information for the erased masks (null)")
        if parseelement>0 and not not(doc['regionInterpretation']['masking']):

            self.erased = structtype()
            #If so proceed

            #Fill in info of the time of first erased pixel
#            self.erased.referenceTime = float(doc['regionInterpretation']['masking']['@referenceTime'])


            #Check if there is mask information
            if doc['regionInterpretation']['masking'].get('mask') != None:
                #Make a list structure of all the different erased masks
                self.erased.masks = [None] * len(doc['regionInterpretation']['masking']['mask'])

                #For bookkeeping
                i=0


                #Check if there is erased mask on one or several channels
                if type(doc['regionInterpretation']['masking']['mask'])!=list:

                    #Grab the mask info
                    mask = doc['regionInterpretation']['masking']['mask']

                    #define the datastructure
                    self.erased.masks[i]=structtype()
                    self.erased.masks[i].channelID = int(mask['@channelID'])
                    self.erased.masks[i].pingOffset = list()
                    self.erased.masks[i].depth =list()

                    #recognize if it is only for one ping, then fill inn info
                    if type(mask['ping']) !=list:
                        ping = mask['ping']
                        self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                        self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                    else:
                        for ping in mask['ping']:
                            self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                            self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                else:

                    #loop through each channel
                    for mask in doc['regionInterpretation']['masking']['mask']:

                        #define the datastructure
                        self.erased.masks[i]=structtype()
                        self.erased.masks[i].channelID = int(mask['@channelID'])
                        self.erased.masks[i].pingOffset = list()
                        self.erased.masks[i].depth =list()


                        #recognize if it is only for one ping, then fill inn info
                        if type(mask['ping']) !=list:
                            ping = mask['ping']
                            self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                            self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                        else:
                            for ping in mask['ping']:
                                self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                                self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                        i += 1






        ####################################################################
        # Processing the school mask information
        ####################################################################

        #Check if there is any school boxes
        if not not(doc['regionInterpretation']['schoolInterpretation']):

            self.school = list()

            #for bookkeeping
            i = 0

            schoolRep=[None]
            parseelement=0
            try:
                doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep']
                parseelement = 1
            except :
                try:
                    doc['regionInterpretation']['schoolInterpretation']['schoolRep']
                    parseelement = 2
                except :
                    parseelement = 0
                    print("ERROR: LSSS work file :: in  Procesing the information for the schools (null)")

            if parseelement==1 and not not(doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep']):
                schoolRep=doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep']
            if parseelement==2 and not not(doc['regionInterpretation']['schoolInterpretation']['schoolRep']):
                schoolRep=doc['regionInterpretation']['schoolInterpretation']['schoolRep']


            #Check if this is one ore several schools
            if type(schoolRep)!=list:
                self.school = [None]

                #grab school info
                schools = schoolRep

                #Define the school as a structure and fill in info
                self.school[i] = structtype()
                # self.school[i].referenceTime = float(schools['@referenceTime'])
                if schools.get('@objectNumber')!=None:
                    self.school[i].objectNumber = int(schools['@objectNumber'])
                else:
                    self.school[i].objectNumber = -1


                #Check if there has been any interpretation made
                if bool(schools['speciesInterpretationRoot']):

                    #Grab the interpretation
                    interpretation = schools['speciesInterpretationRoot']['speciesInterpretationRep']


                    #add interpretation info for each channel
                    ii=0
                    if type(interpretation)!=list:
                        self.school[i].interpretations = [None]
                        intr = interpretation
                        self.school[i].interpretations[ii]=structtype()
                        self.school[i].interpretations[ii].frequency = intr['@frequency']
                        if not len(set(list(intr.keys())).intersection(set(['species','@species'])))>0:
                            self.school[i].interpretations[ii].species_id='-1'
                            self.school[i].interpretations[ii].fraction='1'
                        else:
                            species = intr['species']
                            species_id = list()
                            fraction = list()
                            if type(species)==list:
                                for s in species:
                                    species_id = np.hstack((species_id,s['@ID']))
                                    fraction = np.hstack((fraction,s['@fraction']))
                            else:
                                species_id = np.hstack((species_id,species['@ID']))
                                fraction = np.hstack((fraction,species['@fraction']))

                            self.school[i].interpretations[ii].species_id=species_id
                            self.school[i].interpretations[ii].fraction=fraction

                    else:
                        #set the number of interpretated frequecies
                        self.school[i].interpretations = [None] * len(interpretation)
                        for intr in interpretation:
                            species_id = list()
                            fraction = list()
                            if not len(set(list(intr.keys())).intersection(set(['species','@species'])))>0:
                                self.school[i].interpretations[ii]=structtype()
                                self.school[i].interpretations[ii].frequency = intr['@frequency']
                                self.school[i].interpretations[ii].species_id='-1'
                                self.school[i].interpretations[ii].fraction='1'

                            else:
                                self.school[i].interpretations[ii]=structtype()
                                self.school[i].interpretations[ii].frequency = intr['@frequency']
                                species = intr['species']
                                if type(species)==list:
                                    for s in species:
                                        species_id = np.hstack((species_id,s['@ID']))
                                        fraction = np.hstack((fraction,s['@fraction']))
                                else:
                                    species_id = np.hstack((species_id,species['@ID']))
                                    fraction = np.hstack((fraction,species['@fraction']))
                                self.school[i].interpretations[ii].species_id=species_id
                                self.school[i].interpretations[ii].fraction=fraction

                            ii=ii+1
                else:
                    #If no information is avaliable, output that this has no data
                    self.school[i].interpretations = structtype()
                    self.school[i].interpretations.frequency = -1
                    self.school[i].interpretations.species_id = -1
                    self.school[i].interpretations.fraction = -1



                #Print the interpretation mask of each school
                self.school[i].relativePingNumber=list()
                self.school[i].min_depth = list()
                self.school[i].max_depth = list()
                if parseelement == 1:
                    if type(schools['pingMask'])==list:
                        for ping in schools['pingMask']:
                            self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                            depth = ping['#text'].split()
                            self.school[i].min_depth = np.hstack((self.school[i].min_depth,float(depth[0])))
                            self.school[i].max_depth = np.hstack((self.school[i].max_depth,float(depth[1])))
                    else:
                        ping = schools['pingMask']
                        self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                        depth = ping['#text'].split()
                        self.school[i].min_depth = np.hstack((self.school[i].min_depth,float(depth[0])))
                        self.school[i].max_depth = np.hstack((self.school[i].max_depth,float(depth[1])))

                elif parseelement == 2:
                    mindepth = {}
                    maxdepth = {}
                    schooltemp=schools['boundaryPoints'].split()
                    pingtemp=schooltemp[::2]
                    depthtemp=schooltemp[1::2]
                    for d in range(len(pingtemp)):
                        if pingtemp[d] in maxdepth.keys():
                            mindepth[pingtemp[d]] = depthtemp[d]

                        else:
                            maxdepth[pingtemp[d]] = depthtemp[d]

                    for d in range(len(pingtemp)):
                        vmin = float(mindepth[pingtemp[d]])
                        vmax = float(maxdepth[pingtemp[d]])
                        if vmax>vmin:
                            self.school[i].relativePingNumber = np.hstack(
                                (self.school[i].relativePingNumber, int(pingtemp[d])))
                            self.school[i].min_depth = np.hstack((self.school[i].min_depth, mindepth[pingtemp[d]]))
                            self.school[i].max_depth = np.hstack((self.school[i].max_depth, maxdepth[pingtemp[d]]))
                        else:
                            self.school[i].relativePingNumber = np.hstack(
                                (self.school[i].relativePingNumber, int(pingtemp[d])))
                            self.school[i].min_depth = np.hstack((self.school[i].min_depth, maxdepth[pingtemp[d]]))
                            self.school[i].max_depth = np.hstack((self.school[i].max_depth, mindepth[pingtemp[d]]))

                i=i+1



            else:
                #Define the size of the list
                self.school = [None] * len(schoolRep)
                for schools in schoolRep:

                    #Define the school as a structure and fill in infoo
                    self.school[i] = structtype()
#                   self.school[i].referenceTime = float(schools['@referenceTime'])

                    if schools.get('@objectNumber')!=None:
                        self.school[i].objectNumber = int(schools['@objectNumber'])
                    else:
                        self.school[i].objectNumber = -1

                    #Check if there has been any interpretation made
                    if bool(schools['speciesInterpretationRoot']):

                        #Grab the interpretation
                        interpretation = schools['speciesInterpretationRoot']['speciesInterpretationRep']

                        #set the number of interpretated frequecies
                        self.school[i].interpretations = [None] * len(interpretation)

                        #add interpretation info for each frequency
                        ii=0
                        if type(interpretation)!=list:
                            intr = interpretation
                            self.school[i].interpretations[ii]=structtype()
                            self.school[i].interpretations[ii].frequency = intr['@frequency']

                            if 'species' in list(intr.keys()):
                                species = intr['species']
                                species_id = list()
                                fraction = list()
                                if type(species)==list:
                                    for s in species:
                                        species_id.append(s['@ID'])
                                        fraction.append(s['@fraction'])
                                else:
                                    species_id.append(species['@ID'])
                                    fraction.append(species['@fraction'])
                            else:

                                species_id = -1
                                fraction = -1

                            self.school[i].interpretations[ii].species_id=species_id
                            self.school[i].interpretations[ii].fraction=fraction

                        else:
                            for intr in interpretation:
                                self.school[i].interpretations[ii]=structtype()
                                self.school[i].interpretations[ii].frequency = intr['@frequency']

                                if 'species' in list(intr.keys()):
                                    species = intr['species']
                                    species_id = list()
                                    fraction = list()
                                    if type(species)==list:
                                        for s in species:
                                            species_id.append(s['@ID'])
                                            fraction.append(s['@fraction'])
                                    else:
                                        species_id.append(species['@ID'])
                                        fraction.append(species['@fraction'])
                                else:
                                    species_id = -1
                                    fraction = -1
                                self.school[i].interpretations[ii].species_id=species_id
                                self.school[i].interpretations[ii].fraction=fraction

                                ii=ii+1
                    else:
                        self.school[i].interpretations = structtype()
                        self.school[i].interpretations.frequency = -1
                        self.school[i].interpretations.species_id = -1
                        self.school[i].interpretations.fraction = -1


                    #Print the interpretation mask of each school
                    self.school[i].relativePingNumber=list()
                    self.school[i].min_depth = list()
                    self.school[i].max_depth = list()
                    if parseelement == 1:
                        if type(schools['pingMask'])==list:
                            for ping in schools['pingMask']:
                                depth = ping['#text'].split()
                                min_depth=depth[0::2]
                                max_depth=depth[1::2]
                                for d in range(len(min_depth)):
                                    self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                                    self.school[i].min_depth = np.hstack((self.school[i].min_depth,min_depth[d]))
                                    self.school[i].max_depth = np.hstack((self.school[i].max_depth,max_depth[d]))
                        else:
                            ping = schools['pingMask']
                            depth = ping['#text'].split()
                            min_depth=depth[0::2]
                            max_depth=depth[1::2]
                            for d in range(len(min_depth)):
                                self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                                self.school[i].min_depth = np.hstack((self.school[i].min_depth,min_depth[d]))
                                self.school[i].max_depth = np.hstack((self.school[i].max_depth,max_depth[d]))
                    elif parseelement == 2:
                        mindepth = {}
                        maxdepth = {}
                        schooltemp=schools['boundaryPoints'].split()

                        pingtemp=schooltemp[::2]
                        depthtemp=schooltemp[1::2]

                        for d in range(len(pingtemp)):
                            if pingtemp[d] in maxdepth.keys():
                                mindepth[pingtemp[d]] = depthtemp[d]
                                #print(pingtemp[d]+" min")
                            else:
                                maxdepth[pingtemp[d]] = depthtemp[d]
                                #print(pingtemp[d]+" max")

                        for d in range(len(pingtemp)):
                            vmin = float(mindepth[pingtemp[d]])
                            vmax = float(maxdepth[pingtemp[d]])
                            if vmax>vmin:
                                self.school[i].relativePingNumber = np.hstack(
                                    (self.school[i].relativePingNumber, int(pingtemp[d])))
                                self.school[i].min_depth = np.hstack((self.school[i].min_depth, mindepth[pingtemp[d]]))
                                self.school[i].max_depth = np.hstack((self.school[i].max_depth, maxdepth[pingtemp[d]]))
                            else:
                                self.school[i].relativePingNumber = np.hstack(
                                    (self.school[i].relativePingNumber, int(pingtemp[d])))
                                self.school[i].min_depth = np.hstack((self.school[i].min_depth, maxdepth[pingtemp[d]]))
                                self.school[i].max_depth = np.hstack((self.school[i].max_depth, mindepth[pingtemp[d]]))

                    i=i+1








        ####################################################################
        # Processing the Buble correction information
        ####################################################################
        if type(doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange'])!=list:
            bubble = doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']
            self.info.bubble = structtype()
            self.info.bubble.start = float(bubble['@start'])
            self.info.bubble.numberOfPings = int(bubble['@numberOfPings'])
            self.info.bubble.CorrectionValue = float(bubble['@bubbleCorrectionValue'])
        else:
            self.info.bubble = [None] * len(doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange'])
            i_buble = 0
            for bubble in doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']:
                self.info.bubble[i_buble] = structtype()
                self.info.bubble[i_buble].start = float(bubble['@start'])
                self.info.bubble[i_buble].numberOfPings = int(bubble['@numberOfPings'])
                self.info.bubble[i_buble].CorrectionValue = float(bubble['@bubbleCorrectionValue'])
                i_buble+=1



        with open(work_filename) as fd:
            doc = xmltodict.parse(fd.read())



        ####################################################################
        # Processing the thresholding information
        ####################################################################

        if doc['regionInterpretation'].get('thresholding') != None:

            if type(doc['regionInterpretation']['thresholding']['upperThresholdActive']['timeRange'])!=list:
                upperThresholdActive = doc['regionInterpretation']['thresholding']['upperThresholdActive']['timeRange']
                self.info.upperThresholdActive=structtype()
                self.info.upperThresholdActive.start = float(upperThresholdActive['@start'])
                self.info.upperThresholdActive.numberOfPings = int(upperThresholdActive['@numberOfPings'])
                self.info.upperThresholdActive.value = (upperThresholdActive['@value'])
            else:
                self.info.upperThresholdActive = [None] * len(doc['regionInterpretation']['thresholding']['upperThresholdActive']['timeRange'])
                i = 0
                for upperThresholdActive in doc['regionInterpretation']['thresholding']['upperThresholdActive']['timeRange']:

                    self.info.upperThresholdActive[i]=structtype()
                    self.info.upperThresholdActive[i].start = float(upperThresholdActive['@start'])
                    self.info.upperThresholdActive[i].numberOfPings = int(upperThresholdActive['@numberOfPings'])
                    self.info.upperThresholdActive[i].value = (upperThresholdActive['@value'])
                    i+=1



            if type(doc['regionInterpretation']['thresholding']['upperThreshold']['timeRange'])!=list:
                upperThreshold = doc['regionInterpretation']['thresholding']['upperThreshold']['timeRange']
                self.info.upperThreshold=structtype()
                self.info.upperThreshold.start = float(upperThreshold['@start'])
                self.info.upperThreshold.numberOfPings = int(upperThreshold['@numberOfPings'])
                self.info.upperThreshold.value = float(upperThreshold['@value'])
            else:
                self.info.upperThreshold = [None] * len(doc['regionInterpretation']['thresholding']['upperThreshold']['timeRange'])
                i = 0
                for upperThreshold in doc['regionInterpretation']['thresholding']['upperThreshold']['timeRange']:

                    self.info.upperThreshold[i]=structtype()
                    self.info.upperThreshold[i].start = float(upperThreshold['@start'])
                    self.info.upperThreshold[i].numberOfPings = int(upperThreshold['@numberOfPings'])
                    self.info.upperThreshold[i].value = float(upperThreshold['@value'])
                    i+=1


            if type(doc['regionInterpretation']['thresholding']['lowerThreshold']['timeRange'])!=list:
                lowerThreshold = doc['regionInterpretation']['thresholding']['lowerThreshold']['timeRange']
                self.info.lowerThreshold=structtype()
                self.info.lowerThreshold.start = float(lowerThreshold['@start'])
                self.info.lowerThreshold.numberOfPings = int(lowerThreshold['@numberOfPings'])
                self.info.lowerThreshold.value = float(lowerThreshold['@value'])
            else:
                self.info.lowerThreshold = [None] * len(doc['regionInterpretation']['thresholding']['lowerThreshold']['timeRange'])
                i = 0
                for lowerThreshold in doc['regionInterpretation']['thresholding']['lowerThreshold']['timeRange']:

                    self.info.lowerThreshold[i]=structtype()
                    self.info.lowerThreshold[i].start = float(lowerThreshold['@start'])
                    self.info.lowerThreshold[i].numberOfPings = int(lowerThreshold['@numberOfPings'])
                    self.info.lowerThreshold[i].value = float(lowerThreshold['@value'])
                    i+=1











        ####################################################################
        # Processing the layer information
        ####################################################################
        #Check if there is layer info
        if not not(doc['regionInterpretation']['layerInterpretation']):

            self.layer = structtype()
            layer_info = doc['regionInterpretation']['layerInterpretation']

            #grab the layer definitions
            boundaries_definitions=layer_info['boundaries']['curveBoundary']

            connect_id =structtype()
            connect_id.id = []
            connect_id.pingOffset = []
            for ids in layer_info['connectors']['connector']:
                connect_id.id =np.hstack((connect_id.id,int(ids['@id'])))
                connect_id.pingOffset =np.hstack((connect_id.pingOffset,int(ids['connectorRep']['@pingOffset'])))

            bound_keep = [None] * len(boundaries_definitions)
            i=0
            for bound in boundaries_definitions:
                bound_keep[i] = structtype()
                bound_keep[i].id = int(bound['@id'])
                startConnector = int(bound['@startConnector'])
                endConnector = int(bound['@endConnector'])


#                bound_keep[i].referenceTime = np.float(bound['curveRep']['pingRange']['@referenceTime'])
                bound_keep[i].startPing = int(connect_id.pingOffset[connect_id.id==startConnector])#int(bound['curveRep']['pingRange']['@startOffset'])
                bound_keep[i].numberOfPings= int(connect_id.pingOffset[connect_id.id==endConnector])- int(connect_id.pingOffset[connect_id.id==startConnector]) #int(bound['curveRep']['pingRange']['@numberOfPings'])
                depths = bound['curveRep']['depths']
                depths= depths.replace('\n',' ').split()
                bound_keep[i].depths = [float(i) for i in depths]
                if not (len(bound_keep[i].depths)) == (bound_keep[i].numberOfPings):
                    print('check boundries')
                i+=1


            #grab the layer definitions
            layer_definitions=layer_info['layerDefinitions']


            #recognise if there is one or several layers
            i=0
            if type(layer_definitions['layer'])!=list:

                #define a list of all layers
                self.layer = [None] * len(layer_definitions['layer'])
                lay = layer_definitions['layer']


                #define the layer as a structure type
                self.layer[i] = structtype()

                #store the rest species info
#                self.layer[i].restSpecies = lay['restSpecies']['@ID']

                if lay['speciesInterpretationRoot']!= None:

                    interpretation = lay['speciesInterpretationRoot']['speciesInterpretationRep']


                    ii = 0
                    if type(interpretation)!=list:
                        #define the one layer as a structure
                        self.layer[i].interpretation = structtype()

                        #add frequency
                        self.layer[i].interpretation.frequency = interpretation['@frequency']
                        self.layer[i].interpretation.species_id = list()
                        self.layer[i].interpretation.fraction = list()

                        if 'species' in list(interpretation.keys()):
                            species = interpretation['species']
                            if type(species)==list:
                                for spec in species:
                                    self.layer[i].interpretation.species_id = np.hstack((self.layer[i].interpretation.species_id,spec['@ID']))
                                    self.layer[i].interpretation.fraction = np.hstack((self.layer[i].interpretation.fraction,spec['@fraction']))
                                else:
                                    self.layer[i].interpretation.species_id = np.hstack((self.layer[i].interpretation.species_id,species['@ID']))
                                    self.layer[i].interpretation.fraction = np.hstack((self.layer[i].interpretation.fraction,species['@fraction']))
                        else:
                            self.layer[i].interpretation.species_id = np.hstack((self.layer[i].interpretation.species_id,-1))
                            self.layer[i].interpretation.fraction = np.hstack((self.layer[i].interpretation.fraction,-1))


                    else:

                        self.layer[i].interpretation = [None]*len(interpretation)
                        for intr in interpretation:
                            #define the one layer as a structure
                            self.layer[i].interpretation[ii] = structtype()

                            #add frequency
                            self.layer[i].interpretation[ii].frequency = intr['@frequency']
                            self.layer[i].interpretation[ii].species_id = list()
                            self.layer[i].interpretation[ii].fraction = list()

                            if 'species' in intr:
                                species = intr['species']

                                if type(species)==list:
                                    for spec in species:
                                        self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,spec['@ID']))
                                        self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,spec['@fraction']))
                                else:
                                    self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,species['@ID']))
                                    self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,species['@fraction']))

                            ii+=1


                #get mask information
                boundaries = lay['boundaries']['curveBoundary']
                if len(boundaries)!=2:
                    print('Check number of curveBoundary')


                self.layer[i].boundaries = structtype()

                if lay.get('objectNumber')!=None:
                    self.layer[i].boundaries.ID =int(lay['@objectNumber'])
                else:
                    self.layer[i].boundaries.ID =None
                self.layer[i].boundaries.referenceTime = list()



                b_id = [b.id for b in bound_keep]
                for bound in boundaries:

                    idx = b_id.index(int(bound['@id']))
                    self.layer[i].boundaries.ping = np.arange(bound_keep[idx].startPing ,bound_keep[idx].startPing +bound_keep[idx].numberOfPings)
                    if bound['@isUpper']=='true':
                         self.layer[i].boundaries.depths_upper = bound_keep[idx].depths
                    else:
                         self.layer[i].boundaries.depths_lower = bound_keep[idx].depths


                i+=1


            else:
                #define a list of all layers
                self.layer = [None] * len(layer_definitions['layer'])
                for lay in layer_definitions['layer']:

                    #define the layer as a structure type
                    self.layer[i] = structtype()



                    if bool(lay['speciesInterpretationRoot']):

                        interpretation = lay['speciesInterpretationRoot']['speciesInterpretationRep']

                        self.layer[i].interpretation = [None]*len(interpretation)

                        ii = 0
                        if type(interpretation)!=list:

                            #define the one layer as a structure
                            self.layer[i].interpretation[ii] = structtype()

                            #add frequency
                            self.layer[i].interpretation[ii].frequency = interpretation['@frequency']
                            self.layer[i].interpretation[ii].species_id = list()
                            self.layer[i].interpretation[ii].fraction = list()
                            if interpretation.get('species') != None:
                                species = interpretation['species']

                                if type(species)==list:
                                    for spec in species:
                                        self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,spec['@ID']))
                                        self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,spec['@fraction']))
                                else:
                                    self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,species['@ID']))
                                    self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,species['@fraction']))
                            else:
                                self.layer[i].interpretation[ii].species_id = -1
                                self.layer[i].interpretation[ii].fraction = -1

                        else:
                            for intr in interpretation:
                                #define the one layer as a structure
                                self.layer[i].interpretation[ii] = structtype()

                                #add frequency
                                if intr.get('@frequency')!= None:
                                    self.layer[i].interpretation[ii].frequency = intr['@frequency']
                                    self.layer[i].interpretation[ii].species_id = list()
                                    self.layer[i].interpretation[ii].fraction = list()


                                    if intr.get('species')!= None :
                                        species = intr['species']
                                        if type(species)==list:
                                            for spec in species:
                                                self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,spec['@ID']))
                                                self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,spec['@fraction']))
                                        else:
                                            self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,species['@ID']))
                                            self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,species['@fraction']))
                                    else:
                                        self.layer[i].interpretation[ii].frequency = -1
                                        self.layer[i].interpretation[ii].species_id = -1
                                        self.layer[i].interpretation[ii].fraction = -1

                                else:
                                    self.layer[i].interpretation[ii].frequency = -1
                                    self.layer[i].interpretation[ii].species_id = -1
                                    self.layer[i].interpretation[ii].fraction = -1
                                ii+=1


                    #get mask information
                    boundaries = lay['boundaries']['curveBoundary']
                    if len(boundaries)!=2:
                        print('Check number of curveBoundary')


                    self.layer[i].boundaries = structtype()
                    if lay.get('@objectNumber')!=None:
                        self.layer[i].boundaries.ID =int(lay['@objectNumber'])
                    else:
                        self.layer[i].boundaries.ID =None
                    self.layer[i].boundaries.referenceTime = list()



                    b_id = [b.id for b in bound_keep]
                    for bound in boundaries:

                        idx = b_id.index(int(bound['@id']))
                        self.layer[i].boundaries.ping = np.arange(bound_keep[idx].startPing ,bound_keep[idx].startPing +bound_keep[idx].numberOfPings)
                        if bound['@isUpper']=='true':
                             self.layer[i].boundaries.depths_upper = bound_keep[idx].depths
                        else:
                             self.layer[i].boundaries.depths_lower = bound_keep[idx].depths


                    i+=1









class work_to_annotation (object):


    def __init__(self, work, index_file, svzarr=None , correct_time = True):

        def depthConverter(depth):
            #Helper function to convert the LSSS depth notation to more general
            depth = depth.split(' ')
            for idx in range(len(depth)):
                if idx>0:
                    depth[idx]=float(depth[idx-1])+float(depth[idx])
                else:
                    depth[idx]=float(depth[idx])

            return(depth)
    
        def ping_timeConverter(ping_time):
            if isinstance(ping_time, np.float64):
                ping_time = np.datetime64(unix_to_datetime(ping_time))
            return(ping_time)

        # Use round towards zero
        decimal.getcontext().rounding = decimal.ROUND_DOWN
        raw_file = index_file[0: -3]+"raw"
        ping_time=list()
        # Get some info from the index file
        if svzarr is None:
            print("using raw files for index")
            #Read the ping times
            fid = RawSimradFile(index_file, 'r')
            config = fid.read(1)
            fid2 = RawSimradFile(raw_file, 'r')
            config = fid2.read(1)

            channel_ids = list(config['configuration'].keys())#data.channel_ids
            timestamp = config['timestamp'].timestamp()
            ping_time_IDX = list()

            run = True
            while run:
                try:
                    idx_datagram = fid.read(1)
                    p_time = nt_to_unix((int(idx_datagram['low_date']), int(idx_datagram['high_date'])), return_datetime=False)
                    #the lines below may be an option for some older files
                    #raw_string=struct.unpack('=4sLLLdddLL', idx_datagram)
                    #p_time = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)
                    ping_time_IDX.append(float(round(decimal.Decimal(p_time), 3)))
                except Exception as e:
                    run = False
                    exception_type, exception_object, exception_traceback = sys.exc_info()
                    filename = exception_traceback.tb_frame.f_code.co_filename
                    line_number = exception_traceback.tb_lineno
                    #print("ERROR: - Something went wrong when reading the WORK file RAW index"+ str(raw_file))
                    #print("Exception type: ", exception_type)
                    #print("File name: ", filename)
                    #print("Line number: ", line_number)
                    #print("ERROR: - Something went wrong when reading the WORK file: " + str(raw_file) + " (" + str( e) + ")")

            ping_time = np.array(ping_time_IDX)
            time_diff = np.datetime64(unix_to_datetime(ping_time[0])) - np.datetime64(unix_to_datetime(float(round(decimal.Decimal(timestamp), 3))))
            self.raw_work_timediff = time_diff
        else:
            # Load the zarr file
            print(svzarr)
            dataset = xr.open_zarr(svzarr)
            filenameraw = os.path.basename(raw_file)
            #filenameraw = filenameraw[:-3] 
            parquet_file = svzarr.replace("_sv.zarr", "_ping_time-raw_file.parquet")  
            print(parquet_file)
            table2 = pq.read_table(parquet_file)
            df2= table2.to_pandas()
            filter_column = "raw_file"
            filter_value = filenameraw 
            #df2['raw_file'] = df2['raw_file'].apply(lambda x: x.decode())

            print(filter_column+" : "+  filter_value  )
            #print(df2) 
            filtered_df = df2.loc[df2[filter_column] == filter_value] 
             
            print(filtered_df)
            column_name = "ping_time"
            filtered_col = filtered_df[column_name]
            filtered_col_rounded = filtered_col.dt.round("ms")
            ping_time = filtered_col_rounded.to_numpy()
            channel_ids = dataset.channel_id.values
            print(ping_time)
            print(channel_ids)

        

        raw_file_name = os.path.basename(raw_file )
        
        #For bookkeeping
        mask_depth_upper = []
        mask_depth_lower = []
        pingTime = []
        filenamelist = []
        ping_index = []
        priority = []
        acousticCat = []
        proportion = []
        ID = []
        ChannelID = []
        upperThreshold = []
        lowerThreshold = []

        upperThresholdpings = []
        lowerThresholdpings = []


        # =============================================================================
        # Process thresholds
        # =============================================================================

        #print(raw_file_name +" upperThresholdActive  "+str(work.info.upperThresholdActive.start)+" "+str(np.datetime64(unix_to_datetime(work.info.upperThresholdActive.start))) +" "+str(work.info.upperThresholdActive.numberOfPings)+" "+str(work.info.upperThresholdActive.value))

        if "upperThreshold" in dir(work):
            if( type(work.info.upperThreshold) != list):
                #print(raw_file_name +" upperThreshold  "+str(work.info.upperThreshold.start)+" "+str(np.datetime64(unix_to_datetime(work.info.upperThreshold.start)) )+" "+str(work.info.upperThreshold.numberOfPings)+" "+str(work.info.upperThreshold.value))
                list1 = [work.info.upperThreshold.value] * work.info.upperThreshold.numberOfPings
                upperThresholdpings=upperThresholdpings+list1
            else:
                for thr in  work.info.upperThreshold:
                    #print(raw_file_name +" upperThreshold  "+str(thr.start)+" "+str(np.datetime64(unix_to_datetime(thr.start))) +" "+str(thr.numberOfPings)+" "+str(thr.value))
                    list1 = [thr.value] * thr.numberOfPings
                    upperThresholdpings=upperThresholdpings+list1
        else:
            list1 = [-1] * len(ping_time)
            upperThresholdpings=upperThresholdpings+list1


        if "upperThreshold" in dir(work):
            if( type(work.info.lowerThreshold) != list):
                #print(raw_file_name +" lowerThreshold  "+str(work.info.lowerThreshold.start)+" "+str(np.datetime64(unix_to_datetime(work.info.lowerThreshold.start))) +" "+str(work.info.lowerThreshold.numberOfPings)+" "+str(work.info.lowerThreshold.value))
                list1 = [work.info.lowerThreshold.value] * work.info.lowerThreshold.numberOfPings
                lowerThresholdpings=lowerThresholdpings+list1
            else:
                for thr in  work.info.lowerThreshold:
                    #print(raw_file_name +" lowerThreshold  "+str(thr.start)+" "+str(np.datetime64(unix_to_datetime(thr.start))) +" "+str(thr.numberOfPings)+" "+str(thr.value))
                    list1 = [thr.value] * thr.numberOfPings
                    lowerThresholdpings=lowerThresholdpings+list1
        else:
            list1 = [-1] * len(ping_time)
            lowerThresholdpings=lowerThresholdpings+list1



        # =============================================================================
        # Process the excluded region
        # =============================================================================
        if "exclude" in dir(work):
            for i in range(len(work.exclude.start_time)):
                for chn in channel_ids:
                    start_time= (work.exclude.start_time[i])
                    end_time= ping_time[int(np.where(start_time==ping_time)[0])+int(work.exclude.numOfPings[i])-1]
                    indxp=0;
                    for p in ping_time[(ping_time>=start_time) & (ping_time<=end_time)]:
                        pingTime.append( ping_timeConverter(p)) 
                        mask_depth_upper.append(0.0)
                        mask_depth_lower.append(9999.9)
                        priority.append(1)
                        acousticCat.append(0)
                        proportion.append(1.0)
                        ChannelID.append(chn)
                        ID.append('exclude-'+str(i))
                        filenamelist.append(raw_file_name)
                        ping_index.append(indxp)
                        upperThreshold.append(upperThresholdpings[indxp])
                        lowerThreshold.append(lowerThresholdpings[indxp])
                        indxp=indxp+1



        ####################################################################
        #Process the erased region
        ####################################################################
        if 'erased' in dir(work):
            if "masks" in dir(work.erased):
                for i in range(len(work.erased.masks)):
                    if 'depth' in dir(work.erased.masks[i]):
                        mask_depth = np.array([np.array(depthConverter(d)) for d in work.erased.masks[i].depth])
                        mask_times = [ping_time[int(p)] for p in work.erased.masks[i].pingOffset ]
                        for ii in range(len(mask_times)):
                            m_depth = mask_depth[ii]
                            m_depth=m_depth.reshape(-1,2)
                            for iii in range(m_depth.shape[0]):
                                if type(work.erased.masks[i].channelID)==int:
                                    pingTime.append( ping_timeConverter(mask_times[ii]))
                                    mask_depth_upper.append(min(m_depth[iii,:]))
                                    mask_depth_lower.append(max(m_depth[iii,:]))
                                    priority.append(1)
                                    acousticCat.append(0)
                                    proportion.append(1.0)
                                    ChannelID.append(channel_ids[work.erased.masks[i].channelID-1])
                                    ID.append('erased')
                                    ping_index.append( work.erased.masks[i].pingOffset[ii])
                                    filenamelist.append(raw_file_name)
                                    upperThreshold.append(upperThresholdpings[int(work.erased.masks[i].pingOffset[ii]) ])
                                    lowerThreshold.append(lowerThresholdpings[int(work.erased.masks[i].pingOffset[ii]) ])
                                else:
                                    for chn in channel_ids[work.erased.masks[i].channelID-1]:
                                        pingTime.append(ping_timeConverter(mask_times[ii]))
                                        mask_depth_upper.append(min(m_depth[iii,:]))
                                        mask_depth_lower.append(max(m_depth[iii,:]))
                                        priority.append(1)
                                        acousticCat.append(0)
                                        proportion.append(1.0)
                                        ChannelID.append(chn)
                                        ID.append('erased')
                                        ping_index.append( work.erased.masks[i].pingOffset[ii])
                                        filenamelist.append(raw_file_name)
                                        upperThreshold.append(upperThresholdpings[int(work.erased.masks[i].pingOffset[ii]) ])
                                        lowerThreshold.append(lowerThresholdpings[int(work.erased.masks[i].pingOffset[ii]) ])


        ####################################################################
        #Process the school mask
        ####################################################################

        
        
        if 'school' in dir(work):
            for i in range(len(work.school)):
                mask_depth=[list(a) for a in zip(work.school[i].min_depth ,work.school[i].max_depth)]
                mask_times = [ping_time[int(p)] for p in work.school[i].relativePingNumber]
                
                if type(work.school[i].interpretations)==list:
                    region_channels=[]
                    region_category_names=[]
                    region_category_proportions=[]
                    
                    for intr in work.school[i].interpretations:
                        if 'frequency' in dir(intr):
                            if not intr.species_id == -1:
                                region_category_names = np.hstack((region_category_names,intr.species_id))#[(c.species_id) for c in intr]
                                region_category_proportions = np.hstack((region_category_proportions,intr.fraction))# [(c.fraction) for c in intr]
                                region_channels=np.hstack((region_channels,[i for i in channel_ids if intr.frequency in i]*len(intr.fraction)))
                            else:
                                region_channels=np.hstack((region_channels,-1))
                                region_category_names = np.hstack((region_category_names, -1))
                                region_category_proportions = np.hstack((region_category_proportions,-1))

                        else:
                            region_channels=np.hstack((region_channels,-1))
                            region_category_names = np.hstack((region_category_names, -1))
                            region_category_proportions = np.hstack((region_category_proportions,-1))

                else:
                    region_channels = work.school[i].interpretations.frequency
                    region_category_names=work.school[i].interpretations.species_id
                    region_category_proportions = work.school[i].interpretations.fraction

                
                for ii in range(len(mask_times)):
                    m_depth = np.array(mask_depth[ii])
                    m_depth=m_depth.reshape(-1,2)
                    for iii in range(m_depth.shape[0]):

                        if type(region_channels)!=np.ndarray:
                            chn = region_channels
                            if type(chn) != int:
                                chn = chn[0]
                            if chn == -1 or chn == -1:
                                pingTime.append ( ping_timeConverter(mask_times[ii]))
                                mask_depth_upper.append(float(min(m_depth[iii,:])))
                                mask_depth_lower.append(float(max(m_depth[iii,:])))
                                priority.append(2)
                                acousticCat.append(-1)
                                proportion.append(-1)
                                ChannelID.append(-1)
                                ID.append('School-'+str(work.school[i].objectNumber))
                                ping_index.append( work.school[i].relativePingNumber[ii])
                                filenamelist.append(raw_file_name)
                                upperThreshold.append(upperThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                                lowerThreshold.append(lowerThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                            else:
                                if region_category_names == -1:
                                        pingTime.append(ping_timeConverter(mask_times[ii]))
                                        mask_depth_upper.append(float(min(m_depth[iii,:])))
                                        mask_depth_lower.append(float(max(m_depth[iii,:])))
                                        priority.append(2)
                                        acousticCat.append(region_category_names)
                                        proportion.append(region_category_proportions)
                                        ChannelID.append(chn)
                                        ID.append('School-'+str(work.school[i].objectNumber))
                                        ping_index.append( work.school[i].relativePingNumber[ii])
                                        filenamelist.append(raw_file_name)
                                        upperThreshold.append(upperThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                                        lowerThreshold.append(lowerThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                                else:
                                    for i_chn in np.arange(len(region_category_names)):
                                        pingTime.append(ping_timeConverter(mask_times[ii]))
                                        mask_depth_upper.append(float(min(m_depth[iii,:])))
                                        mask_depth_lower.append(float(max(m_depth[iii,:])))
                                        priority.append(2)
                                        acousticCat.append(region_category_names[i_chn])
                                        proportion.append(region_category_proportions[i_chn])
                                        ChannelID.append(chn)
                                        ID.append('School-'+str(work.school[i].objectNumber))
                                        ping_index.append( work.school[i].relativePingNumber[ii])
                                        filenamelist.append(raw_file_name)
                                        upperThreshold.append(upperThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                                        lowerThreshold.append(lowerThresholdpings[  int(work.school[i].relativePingNumber[ii] )])
                        else:
                            chan_length=len(region_channels)
                            if(chan_length>len(region_category_names)):
                                chan_length =len(region_category_names)
                            for ikk in range(chan_length):
                                pingTime.append( ping_timeConverter(mask_times[ii]))
                                mask_depth_upper.append(float(min(m_depth[iii,:])))
                                mask_depth_lower.append(float(max(m_depth[iii,:])))
                                priority.append(2)
                                acousticCat.append(region_category_names[ikk])
                                proportion.append(region_category_proportions[ikk])
                                ChannelID.append(region_channels[ikk])
                                ID.append('School-'+str(work.school[i].objectNumber))
                                ping_index.append( work.school[i].relativePingNumber[ii])
                                filenamelist.append(raw_file_name)
                                upperThreshold.append(upperThresholdpings[ int(work.school[i].relativePingNumber[ii] ) ])
                                lowerThreshold.append(lowerThresholdpings[ int(work.school[i].relativePingNumber[ii] ) ])






        ####################################################################
        #Process the layer mask
        ####################################################################
        if 'layer' in dir(work):
            for i in range(len(work.layer)):

                try:

                    if "boundaries" in dir(work.layer[i]):
                        mask_depth=[list(a) for a in zip(work.layer[i].boundaries.depths_upper ,work.layer[i].boundaries.depths_lower)]
                        mask_times = [ping_time[int(p)] for p in work.layer[i].boundaries.ping]


                        region_channels = []
                        region_channels_id = []
                        region_category_names = []
                        region_category_proportions = []
                        reg_chn_idx=0
                        #print (dir(work.layer[i]))
                        if "interpretation" in dir(work.layer[i]):

                            if type(work.layer[i].interpretation) == list:
                                for intr in work.layer[i].interpretation:

                                    run1=0
                                    if intr!=None:
                                        if "frequency" in dir(intr):
                                            run1=1

                                    if run1==1:
                                        if intr.frequency!= -1:

                                            #region_channels=np.hstack((region_channels,[i for i in channel_ids if intr.frequency in i][0]))
                                            region_channels.append(([i for i in channel_ids if intr.frequency in i][0]))
                                            region_category_names.append(intr.species_id)

                                            region_category_proportions.append(intr.fraction)

                                            if type(intr.fraction)== float:
                                                #region_channels_id.append((np.repeat(reg_chn_idx,1)))
                                                region_channels_id.append(reg_chn_idx)
                                            elif type(intr.fraction)== int:
                                                #region_channels_id.append((np.repeat(reg_chn_idx,1)))
                                                region_channels_id.append(reg_chn_idx)
                                            else:
                                                #region_channels_id.append(( np.repeat(reg_chn_idx,len(intr.fraction))))
                                                region_channels_id.append([reg_chn_idx]*20)

                                            reg_chn_idx+=1

                                        else:
                                            region_channels.append(-1)
                                            region_category_names.append(-1)
                                            region_category_proportions.append(-1)
                                            region_channels_id.append(reg_chn_idx)
                                            reg_chn_idx+=1


                                    else:
                                        region_channels.append("-1")
                                        region_category_names.append("-1")
                                        region_category_proportions.append("-1")
                                        region_channels_id.append(reg_chn_idx)
                                        reg_chn_idx+=1
                            else:

                                intr=work.layer[i].interpretation

                                if "frequency" in dir(intr):
                                    region_channels=np.hstack((region_channels,[i for i in channel_ids if intr.frequency in i] ))
                                    region_category_names=np.hstack((region_category_names,intr.species_id))
                                    region_category_proportions = np.hstack((region_category_proportions,intr.fraction))
                                    region_channels_id = np.hstack((region_channels_id,np.repeat(reg_chn_idx,len(intr.fraction))))
                                    reg_chn_idx+=1

                                else:
                                    region_channels = np.hstack((region_channels,-1))
                                    region_category_names=np.hstack((region_category_names,-1))
                                    region_category_proportions = np.hstack((region_category_proportions,-1))
                                    region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
                                    reg_chn_idx+=1

                        else:

                            region_channels = np.hstack((region_channels,-1))
                            region_category_names=np.hstack((region_category_names,-1))
                            region_category_proportions = np.hstack((region_category_proportions,-1))
                            region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
                            reg_chn_idx+=1


                        #print(len(mask_depth))
                        #print(mask_depth)
                        for ii in range(len(mask_depth)):
                            m_depth = np.array(mask_depth[ii])
                            m_depth=m_depth.reshape(-1,2)

                            for iii in range(m_depth.shape[0]):
                                for ik in range(len(region_channels)):

                                    sp_prop_c=0
                                    for sublist in region_category_proportions:

                                        if type(sublist) in [int,float,str,np.float64]:
                                            pingTime.append(ping_timeConverter(mask_times[ii]))
                                            mask_depth_upper.append(min(m_depth[iii,:]))
                                            mask_depth_lower.append(max(m_depth[iii,:]))
                                            priority.append(3)
                                            # ID.append('Layer-' + str(i))
                                            ID.append('Layer-' + str(work.layer[i].boundaries.ID))
                                            ChannelID.append(region_channels[ik])
                                            acousticCat.append(int(region_category_names[sp_prop_c]))
                                            proportion.append(float(region_category_proportions[sp_prop_c]))
                                            ping_index.append( work.layer[i].boundaries.ping[ii])
                                            filenamelist.append(raw_file_name)
                                            upperThreshold.append(upperThresholdpings[ work.layer[i].boundaries.ping[ii] ])
                                            lowerThreshold.append(lowerThresholdpings[ work.layer[i].boundaries.ping[ii] ])
                                        else:
                                            for sp_prop_cc in range(len(sublist)):
                                                pingTime.append( ping_timeConverter(mask_times[ii]))
                                                mask_depth_upper.append(min(m_depth[iii,:]))
                                                mask_depth_lower.append(max(m_depth[iii,:]))
                                                priority.append(3)
                                                # ID.append('Layer-' + str(i))
                                                ID.append('Layer-' + str(work.layer[i].boundaries.ID))
                                                ChannelID.append(region_channels[ik])
                                                acousticCat.append(int(region_category_names[sp_prop_c][sp_prop_cc]))
                                                proportion.append(float(region_category_proportions[sp_prop_c][sp_prop_cc]))
                                                ping_index.append( work.layer[i].boundaries.ping[ii])
                                                filenamelist.append(raw_file_name)
                                                upperThreshold.append(upperThresholdpings[ work.layer[i].boundaries.ping[ii] ])
                                                lowerThreshold.append(lowerThresholdpings[ work.layer[i].boundaries.ping[ii] ])
                                        sp_prop_c+=1
                                    #sp_prop_c=0
                                    #for sp_prop in region_category_names[0]:
                                    #    pingTime.append(np.datetime64(unix_to_datetime(mask_times[ii])))
                                    #    mask_depth_upper.append(min(m_depth[iii,:]))
                                    #    mask_depth_lower.append(max(m_depth[iii,:]))
                                    #    priority.append(3)
                                    #    ID.append('Layer-' + str(i))
                                    #    acousticCat.append(int(region_category_names[0][sp_prop_c]))
                                    #    proportion.append(float(region_category_proportions[0][sp_prop_c]))
                                    #    ChannelID.append(region_channels[ik])
                                    #    #print('Layer-' + str(i))
                                    #    #print(region_category_names[0][sp_prop_c])
                                    #    #print(region_category_proportions[0][sp_prop_c])
                                    #    sp_prop_c=sp_prop_c+1
                                    #    #print(region_channels[ik])
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    #print("ERROR: LSSS work file :: work_to_annotation - Process the layer mask " + TypeError + " " + NameError + ValueError)
                    print(exc_type, fname, exc_tb.tb_lineno)

#        if 'layer' in dir(work):
#            for i in range(len(work.layer)):
#                if "boundaries" in dir(work.layer[i]):
#                    mask_depth=[list(a) for a in zip(work.layer[i].boundaries.depths_upper ,work.layer[i].boundaries.depths_lower)]
#                    mask_times = [ping_time[int(p)] for p in work.layer[i].boundaries.ping]
#
#
#                    region_channels = []
#                    region_channels_id = []
#                    region_category_names = []
#                    region_category_proportions = []
#                    reg_chn_idx=0
#                    if "interpretation" in dir(work.layer[i]):
#                        if type(work.layer[i].interpretation) == list:
#                            for intr in work.layer[i].interpretation:
#                                if "frequency" in list(intr.keys()):
#                                    if intr.frequency!= -1:
#                                        region_channels=np.hstack((region_channels,[i for i in channel_ids if intr.frequency in i][0]))
#                                        region_category_names=np.hstack((region_category_names,intr.species_id))
#                                        region_category_proportions = np.hstack((region_category_proportions,intr.fraction))
#                                        if type(intr.fraction)== float:
#                                            region_channels_id = np.hstack((region_channels_id,np.repeat(reg_chn_idx,1)))
#                                        elif type(intr.fraction)== int:
#                                            region_channels_id = np.hstack((region_channels_id,np.repeat(reg_chn_idx,1)))
#                                        else:
#                                            region_channels_id = np.hstack((region_channels_id,np.repeat(reg_chn_idx,len(intr.fraction))))
#                                        reg_chn_idx+=1
#                                    else:
#                                        region_channels = np.hstack((region_channels,-1))
#                                        region_category_names=np.hstack((region_category_names,-1))
#                                        region_category_proportions = np.hstack((region_category_proportions,-1))
#                                        region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
#                                        reg_chn_idx+=1
#                                else:
#                                    region_channels = np.hstack((region_channels,-1))
#                                    region_category_names=np.hstack((region_category_names,-1))
#                                    region_category_proportions = np.hstack((region_category_proportions,-1))
#                                    region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
#                                    reg_chn_idx+=1
#                        else:
#                            intr=work.layer[i].interpretation
#                            if "frequency" in list(intr.keys()):
#                                region_channels=np.hstack((region_channels,[i for i in channel_ids if intr.frequency in i] ))
#                                region_category_names=np.hstack((region_category_names,intr.species_id))
#                                region_category_proportions = np.hstack((region_category_proportions,intr.fraction))
#                                region_channels_id = np.hstack((region_channels_id,np.repeat(reg_chn_idx,len(intr.fraction))))
#                                reg_chn_idx+=1
#                            else:
#                                region_channels = np.hstack((region_channels,-1))
#                                region_category_names=np.hstack((region_category_names,-1))
#                                region_category_proportions = np.hstack((region_category_proportions,-1))
#                                region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
#                                reg_chn_idx+=1
#                    else:
#                        region_channels = np.hstack((region_channels,-1))
#                        region_category_names=np.hstack((region_category_names,-1))
#                        region_category_proportions = np.hstack((region_category_proportions,-1))
#                        region_channels_id = np.hstack((region_channels_id,reg_chn_idx))
#                        reg_chn_idx+=1
#
#
#                    for ii in range(len(mask_depth)):
#                        m_depth = np.array(mask_depth[ii])
#                        m_depth=m_depth.reshape(-1,2)
#                        for iii in range(m_depth.shape[0]):
#                            for ik in range(len(region_channels)):
#                                for ikk in np.where(region_channels_id==ik)[0]:
#                                    pingTime.append(np.datetime64(unix_to_datetime(mask_times[ii])))
#                                    mask_depth_upper.append(min(m_depth[iii,:]))
#                                    mask_depth_lower.append(max(m_depth[iii,:]))
#                                    priority.append(3)
#                                    ID.append('Layer-' + str(i))
#                                    acousticCat.append(int(region_category_names[ikk]))
#                                    proportion.append(float(region_category_proportions[ikk]))
#                                    ChannelID.append(region_channels[ik])


        # =============================================================================
        # Add output as a dataframe
        # =============================================================================

        if len(pingTime) > 0:
            if correct_time:
                correct_time = np.hstack(pingTime) - time_diff
                print("Correcting time by " + str(time_diff))
            else:
                correct_time = np.hstack(pingTime)

            df = pd.DataFrame(data={'ping_index': ping_index,
                                    'ping_time': correct_time,
                                    'mask_depth_upper':mask_depth_upper,
                                    'mask_depth_lower':mask_depth_lower,
                                    'priority':priority,
                                    'acoustic_category':acousticCat,
                                    'proportion':proportion,
                                    'object_id':ID,
                                    'channel_id':ChannelID,
                                    'upperThreshold':upperThreshold,
                                    'lowerThreshold':lowerThreshold,
                                    'raw_file':filenamelist})

            # Convert if necessary
            self.df_ = df.astype({'ping_index': 'int64',
                                    'ping_time': 'datetime64[ns]',
                                    'mask_depth_upper': 'float64',
                                    'mask_depth_lower': 'float64',
                                    'priority': 'int64',
                                    'acoustic_category': str,
                                    'proportion': 'float64',
                                    'object_id': str,
                                    'channel_id': str,
                                    'upperThreshold': 'float64',
                                    'lowerThreshold': 'float64',
                                    'raw_file': str})
        else:
            self.df_ = None
