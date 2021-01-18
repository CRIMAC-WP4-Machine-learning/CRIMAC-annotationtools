# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 08:46:06 2021

@author: sindrev
"""

from decimal import Decimal
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import numpy as np

class annotation_to_work (object):

    """Class for writing LSSS .work files from annotation class"""



    def __init__(self,filename='',annotation=[]):
        
        
        
        def reversed_depthConverter(depth): 
            dp = list()
            for idx in range(len(depth)):
                if idx>0: 
                    dp.append(str(round(depth[idx] -depth[idx-1],7)))
                else: 
                    dp.append(str(depth[idx]))
                 
            return(' '.join(dp))
    
        
        def epoce_to_UNIXtime(timestamp):
            '''Helper function to convert epoc time to Work
            
            For some reason we need to add 1 houre!!!
            '''
            return((float(timestamp/1e9+(datetime(1601, 1, 1)-datetime(1970, 1, 1,1,0)).total_seconds())))
            
            
            
        
        # create the file structure
        data = ET.Element('regionInterpretation')
        data.set('version','1.0')
        
        
        #Set the first line where we describe the time of first ping and 
        #number of pings in file
        timeRange = ET.SubElement(data, 'timeRange')
        timeRange.set('start',str('%.12E' % Decimal(epoce_to_UNIXtime(annotation.info.timeFirstPing))))
        timeRange.set('numberOfPings',str(annotation.info.numberOfPings))
        
        
        
        #Write the information of excluded ranges
        exclusionRanges = ET.SubElement(data, 'exclusionRanges')
        for m in annotation.mask:
            if(m.min_depth == 0.0 and m.max_depth == 9999.9 and m.priority == 1):
                timeRange=ET.SubElement(exclusionRanges, 'timeRange')
                timeRange.set('start',str('%.12E' % Decimal(epoce_to_UNIXtime(m.mask_times[0]))))
                timeRange.set('numberOfPings',str(len(m.mask_times)))
        
        
        #Write the output of the bubbleCorrectionRanges
        #TODO: this has to be done in the reader
        bubbleCorrectionRanges = ET.SubElement(data, 'bubbleCorrectionRanges')
        
        
        
        #write the information of mask per channel
        masking = ET.SubElement(data, 'masking')
        min_time = []
        for m in annotation.mask:
            min_time = np.hstack((min_time,min(m.mask_times)))
        masking.set('referenceTime',str('%.12E' % Decimal(epoce_to_UNIXtime(min(min_time)))))
        
        for chn_i in annotation.info.channel_names:
             for m in annotation.mask: 
                 if m.priority==1 and (m.min_depth != 0.0 or m.max_depth != 9999.9) and m.region_type == 'no_data' :
                     mask = ET.SubElement(masking, 'mask')
                     mask.set('channelID',str(1+annotation.info.channel_names.index(chn_i)))
                     for p in range(len(m.mask_depth)):
                         ping = ET.SubElement(mask,'ping')
                         ping.set('pingOffset',str(np.where(annotation.info.ping_time==m.mask_times[p])[0][0]))
                         ping.text=reversed_depthConverter(m.mask_depth[p])
                    
        
        thresholding = ET.SubElement(data, 'thresholding')
        
        
        layerInterpretation = ET.SubElement(data, 'layerInterpretation')
        boundaries = ET.SubElement(layerInterpretation, 'boundaries')
        
        
        
        
        
        ####################################################################
        #Process the school information
        ####################################################################
        schoolInterpretation = ET.SubElement(data, 'schoolInterpretation')
        for m in annotation.mask: 
            if m.priority==2: 
                school = ET.SubElement(schoolInterpretation,'schoolMaskRep')
                school.set('referenceTime',str('%.12E' % Decimal(epoce_to_UNIXtime(annotation.info.timeFirstPing))))
                school.set('hasBeenVisisted','true')
                school.set('objectNumber',str(m.region_id))
                #TODO: do we need Rest species???
                spintroot = ET.SubElement(school,'speciesInterpretationRoot')
                for chn in m.region_channels: 
                    chn_idx = annotation.info.channel_names.index(chn)
                    interpRep = ET.SubElement(spintroot,'speciesInterpretationRep')
                    interpRep.set('frequency','?')
                    for spec in range(len(m.region_category_ids[chn_idx])):
                        species = ET.SubElement(interpRep,'species')
                        species.set('ID',str(m.region_category_ids[chn_idx][spec]))
                        species.set('fraction',str(m.region_category_proportions[chn_idx][spec]))
                for i in range(len(m.mask_times)): 
                    ping_idx = int(np.where(m.mask_times[i]==annotation.info.ping_time)[0])
                    pingMask = ET.SubElement(school,'pingMask')
                    pingMask.set('relativePingNumber',str(ping_idx+1))
                    pingMask.text = ' '.join([str(t) for t in m.mask_depth[i]])
                
                    
                    
        #item2.set('name','item2')
        #item1.text = 'item1abc'
        #item2.text = 'item2abc'
        
        # create a new XML file with the results
        mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
        myfile = open(filename, "w")
        myfile.write(str(mydata))

        
            