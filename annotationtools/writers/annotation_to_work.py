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
            if(m.min_depth == 0 and m.max_depth == 9999 and m.priority == 1):
                timeRange=ET.SubElement(exclusionRanges, 'timeRange')
                timeRange.set('start',str('%.12E' % Decimal(epoce_to_UNIXtime(m.mask_times[0]))))
                timeRange.set('numberOfPings',str(len(m.mask_times)))
        
        
        #Write the output of the bubbleCorrectionRanges
        #TODO: this has to be done in the reader
        bubbleCorrectionRanges = ET.SubElement(data, 'bubbleCorrectionRanges')
        
        
        
        #write the information of mask per channel
        masking = ET.SubElement(data, 'masking')
        
        for chn_i in range(len(annotation.info.channel_names)):
            for m in annotation.mask: 
                if m.priority==1 and (m.min_depth != 0 or m.max_depth != 9999) and m.region_type == 'no data':
                    print(m.region_type)
        
        
        
        thresholding = ET.SubElement(data, 'thresholding')
        layerInterpretation = ET.SubElement(data, 'layerInterpretation')
        schoolInterpretation = ET.SubElement(data, 'schoolInterpretation')
        #item2.set('name','item2')
        #item1.text = 'item1abc'
        #item2.text = 'item2abc'
        
        # create a new XML file with the results
        mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
        myfile = open(filename, "w")
        myfile.write(str(mydata))

        
            