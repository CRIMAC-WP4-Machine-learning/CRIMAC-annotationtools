# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 11:18:25 2020

@author: sindrev
"""



#Load som packages
import numpy as np
import xmltodict
from datetime import datetime
from echolab2.instruments import EK60





class work_reader (object):

    """Class for read LSSS .work files to ices annotation class
    
    
    datastructure: 
        
    #school info
        school                               - list containing innformation of each school
        school[i].referenceTime
        school[i].objectNumber 
        school[i].relativePingNumber         -
        school[i].min_depth
        school[i].max_depth
        school[i].interpretation
        school[i].interpretations[ii].frequency 
        school[i].interpretations[ii].species_id 
        school[i].interpretations[ii].fraction
        
        
    #erased mask info
        erased                               -a datastructure including information of the errased mask
        erase.masks                          -a list containing the mask per frequency
        erased.masks[i].channelID            -channelID
        erased.masks[i].pingOffset           -list of ping number
        erased.masks[i].depth                -list of depth intervalls
        
    #excluded info
        excluded                             - a datastructure including innformation 
    
    
    """






    def __init__(self,work_filename=''):
        
        #Define a structure type that is used to output the data
        class structtype(): 
            pass
        
        
        #define outputs
        self.school = list()
        self.layer = structtype()
        self.exclude = structtype()
        self.erased = structtype()
        self.info = structtype()  
        
        
        
        
        
        #msg for user
        print('Reading:' + work_filename)
        
        
        
        
        #Parse the LSSS work file to a dictionary
        with open(work_filename) as fd:
            doc = xmltodict.parse(fd.read())
            
        
                    
        #number of pings in file to info section
        self.info.numberOfPings = np.int(doc['regionInterpretation']['timeRange']['@numberOfPings'])
        self.info.timeFirstPing = float(doc['regionInterpretation']['timeRange']['@start'])
        
        
        
        ####################################################################
        #Procesing the information for exclude region
        ####################################################################
        if not not(doc['regionInterpretation']['exclusionRanges']):
            
            self.exclude.start_time = list()
            self.exclude.numOfPings = list()
            
            #Check if there is more than one and fill inn data
            if len(doc['regionInterpretation']['exclusionRanges'])==1:
                timeRange = doc['regionInterpretation']['exclusionRanges']['timeRange']
                numOfPings = int(timeRange['@numberOfPings'])
                start_time = float(timeRange['@start'])
                
                self.exclude.start_time = np.hstack((self.exclude.start_time,start_time))
                self.exclude.numOfPings = np.hstack((self.exclude.numOfPings,numOfPings))
#                self.exclude.start_time_UNIX = np.hstack((self.exclude.start_time_UNIX,start_time_UNIX))
                
                
            else:
                #exclusionRanges
                for timeRange in doc['regionInterpretation']['exclusionRanges']['timeRange']: 
                    numOfPings = int(timeRange['@numberOfPings'])
                    start_time = float(timeRange['@start'])
                    
                    self.exclude.start_time = np.hstack((self.exclude.start_time,start_time))
                    self.exclude.numOfPings = np.hstack((self.exclude.numOfPings,numOfPings))
        
        
        
        
        
        
        
        
        
        ####################################################################
        #Procesing the information for the mask of erased pixels
        ####################################################################
        
        #check if there is any inforamtion of erased masks
        if not not(doc['regionInterpretation']['masking']):
            
            #Fillin info of the time of first erased pixel
            self.erased.referenceTime = float(doc['regionInterpretation']['masking']['@referenceTime'])
            
            #Set to a list of number of frequencies
            self.erased.masks = [None] * len(doc['regionInterpretation']['masking']['mask'])
            
            
            i=0
            #handle if it is only for one or several frequencies
            if len(doc['regionInterpretation']['masking']['mask'])==1:
                #Grab the mask info
                mask = doc['regionInterpretation']['masking']['mask']
                
                #define the structure and fill inn info
                self.erased.masks[i]=structtype()
                self.erased.masks[i].channelID = int(mask['@channelID'])
                self.erased.masks[i].pingOffset = list()
                self.erased.masks[i].depth =list()
                
                #recognize if it is only for one ping, then fill inn info
                if len(mask['ping']) ==1: 
                    ping = mask['ping']
                    self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                    self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                else: 
                    for ping in mask['ping']: 
                        self.erased.masks[i].pingOffset = np.hstack((self.erased.masks[i].pingOffset,int(ping['@pingOffset'])))
                        self.erased.masks[i].depth = np.hstack((self.erased.masks[i].depth,[ping['#text']]))
                             

            else:
                #loop through each frequency
                for mask in doc['regionInterpretation']['masking']['mask']: 
                    
                    #Set to a list of number of frequencies
                    self.erased.masks[i]=structtype()
                    self.erased.masks[i].channelID = int(mask['@channelID'])
                    self.erased.masks[i].pingOffset = list()
                    self.erased.masks[i].depth =list()
                    
                    
                    #recognize if it is only for one ping, then fill inn info
                    if len(mask['ping']) ==1: 
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
        if not not(doc['regionInterpretation']['schoolInterpretation']):
            
            #Define the size of the list
            self.school = [None] * len(doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep'])
            
            i = 0

            #Check if this is one ore several schools
            if len(doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep'])==1: 
                schools = doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep']
                
                #Define the school as a structure and fill in infoo
                self.school[i] = structtype()
                self.school[i].referenceTime = float(schools['@referenceTime'])
                self.school[i].objectNumber = int(schools['@objectNumber'])
                
                
                #Check if there has been any interpretation made
                if bool(schools['speciesInterpretationRoot']): 
                    
                    #Grab the interpretation
                    interpretation = schools['speciesInterpretationRoot']['speciesInterpretationRep']
                    
                    #set the number of interpretated frequecies
                    self.school[i].interpretations = [None] * len(interpretation)
                    
                    #add interpretation info for each frequency
                    ii=0
                    if len(interpretation)==1:
                        intr = interpretation
                        self.school[i].interpretations[ii]=structtype()
                        self.school[i].interpretations[ii].frequency = intr['@frequency']
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
                        for intr in interpretation: 
                            self.school[i].interpretations[ii]=structtype()
                            self.school[i].interpretations[ii].frequency = intr['@frequency']
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
                            
                            ii=ii+1
                else: 
                    self.school[i].interpretations = structtype()
                    self.school[i].interpretations.frequency = 'No data'
                    self.school[i].interpretations.species_id = 'No data'
                    self.school[i].interpretations.fraction = 'No data'
                         
                        
                #Print the interpretation mask of each school
                self.school[i].relativePingNumber=list()
                self.school[i].min_depth = list()
                self.school[i].max_depth = list()
                for ping in schools['pingMask']: 
                    self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                    depth = ping['#text'].split()
                    self.school[i].min_depth = np.hstack((self.school[i].min_depth,float(depth[0])))
                    self.school[i].max_depth = np.hstack((self.school[i].max_depth,float(depth[1])))     
                i=i+1
                
                
                
            else: 
                for schools in doc['regionInterpretation']['schoolInterpretation']['schoolMaskRep']: 
                    #Define the school as a structure and fill in infoo
                    self.school[i] = structtype()
                    self.school[i].referenceTime = float(schools['@referenceTime'])
                    self.school[i].objectNumber = int(schools['@objectNumber'])
                    
                    
                    #Check if there has been any interpretation made
                    if bool(schools['speciesInterpretationRoot']): 
                        
                        #Grab the interpretation
                        interpretation = schools['speciesInterpretationRoot']['speciesInterpretationRep']
                        
                        #set the number of interpretated frequecies
                        self.school[i].interpretations = [None] * len(interpretation)
                        
                        #add interpretation info for each frequency
                        ii=0
                        if len(interpretation)==1:
                            intr = interpretation
                            self.school[i].interpretations[ii]=structtype()
                            self.school[i].interpretations[ii].frequency = intr['@frequency']
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
                            for intr in interpretation: 
                                self.school[i].interpretations[ii]=structtype()
                                self.school[i].interpretations[ii].frequency = intr['@frequency']
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
                                
                                ii=ii+1
                    else: 
                        self.school[i].interpretations = structtype()
                        self.school[i].interpretations.frequency = 'No data'
                        self.school[i].interpretations.species_id = 'No data'
                        self.school[i].interpretations.fraction = 'No data'
                             
                            
                    #Print the interpretation mask of each school
                    self.school[i].relativePingNumber=list()
                    self.school[i].min_depth = list()
                    self.school[i].max_depth = list()
                    for ping in schools['pingMask']: 
                        self.school[i].relativePingNumber = np.hstack((self.school[i].relativePingNumber,int(ping['@relativePingNumber'])))
                        depth = ping['#text'].split()
                        self.school[i].min_depth = np.hstack((self.school[i].min_depth,float(depth[0])))
                        self.school[i].max_depth = np.hstack((self.school[i].max_depth,float(depth[1])))     
                    i=i+1
                
                
                
                
                
                
                
                
        ####################################################################
        # Processing the Buble correction information
        ####################################################################
#        bubbleCorrection = np.ones(self.info.numberOfPings,np.float)
#        if not not(doc['regionInterpretation']['bubbleCorrectionRanges']):
#            if len(doc['regionInterpretation']['bubbleCorrectionRanges'])==1: 
#                timeRange = doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']
#                start_time = UNIXtime_to_epoce(float(timeRange['@start']))
#                bubbleCorrection[range(self.info.ping_time.index(start_time),self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings']))]=float(timeRange['@bubbleCorrectionValue'])
#            else:
#                for timeRange in doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']: 
#                    timeRange = doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']
#                    start_time = UNIXtime_to_epoce(float(timeRange['@start']))
#                    bubbleCorrection[range(self.info.ping_time.index(start_time),self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings']))]=float(timeRange['@bubbleCorrectionValue'])
            
            
        
        
        
        
        
                
        ####################################################################
        # Processing the thresholding information
        ####################################################################
            
            
        
        
        
        
        
        
        
        
        
                
        ####################################################################
        # Processing the layer information
        ####################################################################
        if not not(doc['regionInterpretation']['layerInterpretation']):    
            layer_info = doc['regionInterpretation']['layerInterpretation']
            
            
            
            #grab the layer definitions
            boundaries_definitions=layer_info['boundaries']['curveBoundary']
            bound_keep = [None] * len(boundaries_definitions)
            i=0
            for bound in boundaries_definitions: 
                bound_keep[i] = structtype()
                bound_keep[i].id = int(bound['@id'])
                bound_keep[i].referenceTime = np.float(bound['curveRep']['pingRange']['@referenceTime'])
                bound_keep[i].startPing = int(bound['curveRep']['pingRange']['@startOffset'])
                bound_keep[i].numberOfPings= int(bound['curveRep']['pingRange']['@numberOfPings'])
                depths = bound['curveRep']['depths']
                depths= depths.replace('\n',' ').split()
                bound_keep[i].depths = [float(i) for i in depths]
                i+=1
            
            
            #grab the layer definitions
            layer_definitions=layer_info['layerDefinitions']
            
            #define a list of all layers
            self.layer = [None] * len(layer_definitions['layer'])
            
            #recognise if there is one or several layers
            i=0
            if len(layer_definitions['layer'])==1: 
                lay = layer_definitions['layer']
                
                
                
            else: 
                for lay in layer_definitions['layer']: 
                    
                    #define the layer as a structure type
                    self.layer[i] = structtype()
                    
                    #store the rest species info
                    self.layer[i].restSpecies = lay['restSpecies']['@ID']
                    
                    
                    interpretation = lay['speciesInterpretationRoot']['speciesInterpretationRep']
                    
                    self.layer[i].interpretation = [None]*len(interpretation)
                    
                    ii = 0
                    if len(interpretation)==1: 
                        #define the one layer as a structure
                        self.layer[i].interpretation[ii] = structtype()
                        
                        #add frequency
                        self.layer[i].interpretation[ii].frequency = intr['@frequency']
                        self.layer[i].interpretation[ii].species_id = list()
                        self.layer[i].interpretation[ii].fraction = list()
                        
                        species = intr['species']
                        
                        if type(species)==list: 
                            for spec in species: 
                                self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,spec['@ID']))
                                self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,spec['@fraction']))
                        else: 
                            self.layer[i].interpretation[ii].species_id = np.hstack((self.layer[i].interpretation[ii].species_id,species['@ID']))
                            self.layer[i].interpretation[ii].fraction = np.hstack((self.layer[i].interpretation[ii].fraction,species['@fraction']))
                                
                    else: 
                        for intr in interpretation: 
                            #define the one layer as a structure
                            self.layer[i].interpretation[ii] = structtype()
                            
                            #add frequency
                            self.layer[i].interpretation[ii].frequency = intr['@frequency']
                            self.layer[i].interpretation[ii].species_id = list()
                            self.layer[i].interpretation[ii].fraction = list()
                            
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
                    self.layer[i].boundaries.ID =int(lay['@objectNumber'])
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
            
            
            
            
            
            
            
            
            
            
            
            
            

class work_to_annotation(object): 
    
    '''
    class to convert work object to annotation
    '''
    
    def __init__(self,work,raw_filename = ''): 
        
        
        
        #Define a structure type that is used to output the data
        class structtype(): 
            pass
        
        
        def depthConverter(depth): 
            depth = depth.split(' ')
            for idx in range(len(depth)):
                if idx>0: 
                    depth[idx]=float(depth[idx-1])+float(depth[idx])
                else: 
                    depth[idx]=float(depth[idx])
                
            return(depth)
    
            
            
            
            
            
        def UNIXtime_to_epoce(timestamp):
            '''Helper function to convert time stamps in work file to epoc since 1601'''
            return((datetime.fromtimestamp(timestamp) - datetime(1601, 1, 1)).total_seconds()*1e9)
            
            
            
            
        data = EK60.EK60()
        data.read_raw(raw_filename)
        ping_time = data.get_channel_data(frequencies=38000)[38000][0].ping_time
        
        
        
        
        
        
        ####################################################################
        #Info secion
        ####################################################################
        self.info = structtype()
        self.info.numberOfPings=work.info.numberOfPings
        self.info.timeFirstPing=UNIXtime_to_epoce(work.info.timeFirstPing)
        self.info.ping_time = np.array([(datetime.fromtimestamp(time.astype('uint64')/1000)- datetime(1601, 1, 1)).total_seconds()*1e9 for time in ping_time])
        self.info.channel_names = list(data.get_channel_data().keys())
        
        
        
        
        ####################################################################
        #Define the maskoutput
        ####################################################################
        self.mask = list()
        
        
        
        ####################################################################
        #Add excluded region
        ####################################################################
        for i in range(len(work.exclude.start_time)): 
            self.mask.append(1)
            mask_i = len(self.mask)-1
            self.mask[mask_i] = structtype()
            self.mask[mask_i].region_id = list()
            self.mask[mask_i].region_name = list()
            self.mask[mask_i].region_provenance= 'LSSS'
            self.mask[mask_i].region_type= 'nodata'
            self.mask[mask_i].region_channels= list(np.arange(len(self.info.channel_names))+1)
            self.mask[mask_i].regions= list()
            self.mask[mask_i].start_time= UNIXtime_to_epoce(work.exclude.start_time[i])
            self.mask[mask_i].end_time = self.info.ping_time[np.int(np.where(self.mask[mask_i].start_time==self.info.ping_time)[0])+int(work.exclude.numOfPings[i])-1]
            self.mask[mask_i].mask_times = self.info.ping_time[((self.info.ping_time>=self.mask[mask_i].start_time) & (self.info.ping_time<=self.mask[mask_i].end_time))]
            self.mask[mask_i].max_depth = 9999
            self.mask[mask_i].min_depth = 0
            self.mask[mask_i].mask_depth = [[self.mask[mask_i].min_depth,self.mask[mask_i].max_depth] for x in range(len(self.mask[mask_i].mask_times))]
            self.mask[mask_i].priority = 1




        ####################################################################
        #Add erased region
        ####################################################################
        for i in range(len(work.erased.masks)):
            self.mask.append(1)
            mask_i = len(self.mask)-1
            self.mask[mask_i] = structtype()
            self.mask[mask_i].region_id = list()
            self.mask[mask_i].region_name = list()
            self.mask[mask_i].region_provenance= 'LSSS'
            self.mask[mask_i].region_type= 'nodata'
            self.mask[mask_i].regions= list()
            self.mask[mask_i].region_channels=work.erased.masks[i].channelID 
            self.mask[mask_i].mask_times = [ping_time[int(p)] for p in work.erased.masks[i].pingOffset ]
            self.mask[mask_i].start_time = self.mask[mask_i].mask_times[0]
            self.mask[mask_i].end_time = self.mask[mask_i].mask_times[-1]
            self.mask[mask_i].mask_depth = [depthConverter(d) for d in work.erased.masks[i].depth]
            self.mask[mask_i].min_depth = min([min(d) for d in self.mask[mask_i].mask_depth])
            self.mask[mask_i].max_depth = min([min(d) for d in self.mask[mask_i].mask_depth])
            self.mask[mask_i].priority = 1
                    
            
            
            
        
        ####################################################################
        #Add school mask
        ####################################################################
        for i in range(len(work.school)): 
            self.mask.append(1)
            mask_i = len(self.mask)-1
            self.mask[mask_i] = structtype()
            self.mask[mask_i].region_id = list()
            self.mask[mask_i].region_name = list()
            self.mask[mask_i].region_provenance= 'LSSS'
            self.mask[mask_i].region_type= 'analysis'
            self.mask[mask_i].regions= list()
            self.mask[mask_i].region_id = work.school[i].objectNumber
            self.mask[mask_i].mask_times = [ping_time[int(p)] for p in work.school[i].relativePingNumber]
            self.mask[mask_i].start_time = self.mask[mask_i].mask_times[0]
            self.mask[mask_i].end_time = self.mask[mask_i].mask_times[-1]
            self.mask[mask_i].min_depth = min(work.school[i].min_depth )
            self.mask[mask_i].max_depth = max(work.school[i].max_depth)
            self.mask[mask_i].mask_depth=[list(a) for a in zip(work.school[i].min_depth ,work.school[i].max_depth)]
#            
            if type(work.school[i].interpretations) == list:
                self.mask[mask_i].region_channels=[list(data.get_channel_data().keys()).index(ip)+1 for f in work.school[i].interpretations for ip in list(data.get_channel_data().keys()) if f.frequency in ip]
                self.mask[mask_i].region_category_ids = [c.species_id for c in work.school[i].interpretations]
                self.mask[mask_i].region_category_names = [c.species_id for c in work.school[i].interpretations]
                self.mask[mask_i].region_category_proportions = [c.fraction for c in work.school[i].interpretations]
            else: 
                self.mask[mask_i].region_category_ids=work.school[i].interpretations.species_id
                self.mask[mask_i].region_category_names = work.school[i].interpretations.species_id
                self.mask[mask_i].region_category_proportions = work.school[i].interpretations.fraction
                self.mask[mask_i].region_channels=[list(data.get_channel_data().keys()).index(ip)+1 for ip in list(data.get_channel_data().keys()) if work.school[i].interpretations.frequency in ip]
            self.mask[mask_i].priority = 2
            
        ####################################################################
        #Add layer mask
        ####################################################################
        for i in range(len(work.layer)):
            self.mask.append(1)
            mask_i = len(self.mask)-1
            self.mask[mask_i] = structtype()
            self.mask[mask_i].region_id = list()
            self.mask[mask_i].region_name = list()
            self.mask[mask_i].region_provenance= 'LSSS'
            self.mask[mask_i].region_type= 'analysis'
            self.mask[mask_i].regions= list()
#            self.mask[mask_i].region_id = work.school[i].objectNumber
            self.mask[mask_i].mask_times = [ping_time[int(p)] for p in work.layer[i].boundaries.ping]
            self.mask[mask_i].start_time = self.mask[mask_i].mask_times[0]
            self.mask[mask_i].end_time = self.mask[mask_i].mask_times[-1]
            self.mask[mask_i].min_depth = min(work.layer[i].boundaries.depths_upper)
            self.mask[mask_i].max_depth = max(work.layer[i].boundaries.depths_lower)
            self.mask[mask_i].mask_depth=[list(a) for a in zip(work.layer[i].boundaries.depths_upper ,work.layer[i].boundaries.depths_lower)]
            self.mask[mask_i].region_channels=[list(data.get_channel_data().keys()).index(i)+1 for f in work.layer[i].interpretation for i in list(data.get_channel_data().keys()) if f.frequency in i]
            self.mask[mask_i].region_category_ids = [c.species_id for c in work.layer[i].interpretation]
            self.mask[mask_i].region_category_names = [c.species_id for c in work.layer[i].interpretation]
            self.mask[mask_i].region_category_proportions = [c.fraction for c in work.layer[i].interpretation]
#            self.mask[mask_i].region_channels=[list(data.get_channel_data().keys()).index(ip)  for i.p in list(data.get_channel_data().keys()) if work.layer[i].interpretation.frequency in ip]
                
            self.mask[mask_i].priority = 3
