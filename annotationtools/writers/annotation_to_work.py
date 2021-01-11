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
        
        
        def epoce_to_UNIXtime(timestamp):
            '''Helper function to convert epoc time to Work
            
            For some reason we need to add 1 houre!!!
            '''
            return(round(float(timestamp/1e9+(datetime(1601, 1, 1)-datetime(1970, 1, 1,1,0)).total_seconds()),2))
            
            
            
#        #Grab mask priority
#        priority = np.array([i.priority for i in annotation.mask])
#        
#        region_channels = [i.region_channels for i in annotation.mask]
#        
#        unique_region_channels = list()
#        for r in region_channels:
#            unique_region_channels = np.unique(np.hstack((unique_region_channels,r)))
#            
#            
#        for urc in unique_region_channels: 
#            for ii in range(len(priority)): 
#                if priority[ii] ==1: 
#                    if type(region_channels[ii]) == int: 
#                        if int(urc) == region_channels[ii]: 
#                            print(region_channels[ii])
#                    else: 
#                        if int(urc) in region_channels[ii]: 
#                            print(region_channels[ii])
        
        # create the file structure
        data = ET.Element('regionInterpretation')
        data.set('version','1.0')
        
        
        timeRange = ET.SubElement(data, 'timeRange')
        timeRange.set('start',str('%.11E' % Decimal(epoce_to_UNIXtime(annotation.info.timeFirstPing))))
        timeRange.set('numberOfPings',str(annotation.info.numberOfPings))
        
        
        
        
        exclusionRanges = ET.SubElement(data, 'exclusionRanges')
        bubbleCorrectionRanges = ET.SubElement(data, 'bubbleCorrectionRanges')
        masking = ET.SubElement(data, 'masking')
        
        
        
        thresholding = ET.SubElement(data, 'thresholding')
        layerInterpretation = ET.SubElement(data, 'layerInterpretation')
        schoolInterpretation = ET.SubElement(data, 'schoolInterpretation')
        #item2.set('name','item2')
        #item1.text = 'item1abc'
        #item2.text = 'item2abc'
        
        # create a new XML file with the results
        mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
        myfile = open("E:/Arbeid/Koding/CRIMAC/code_repo/data/ACOUSTIC/LSSS/WORK/work_2.work", "w")
        myfile.write(str(mydata))

        
            