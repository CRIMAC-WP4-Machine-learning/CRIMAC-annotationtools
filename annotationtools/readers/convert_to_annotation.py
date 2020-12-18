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
import os


class work_to_annotations (object):

    """Class for read LSSS .work files to ices annotation class"""






    def __init__(self,filename=''):
        
        
            
        def UNIXtime_to_epoce(timestamp):
            '''Helper function to convert time stamps in work file to epoc since 1601'''
            return((datetime.fromtimestamp(timestamp) - datetime(1601, 1, 1)).total_seconds()*1e9)
            
        #Fixing names of files
        filename = os.path.splitext(filename)[0]
        raw_filename=filename+'.raw'
        work_filename = filename+'.work'


#        #make error if both the work and the raw file does not exist
#        if not os.path.isfile(raw_filename) and not os.path.isfile(work_filename): 
#            print('asdf')
#            raise FileError('The file named '+ filename + 'does not have both a raw and a work file')
            
        
        #Define a structure type
        class structtype(): 
            pass
        
        
        #define outputs
        self.school = structtype()
        self.layer = structtype()
        self.exclude = structtype()
        self.ferased = structtype()
        self.info = structtype()  
        
        
        #msg for user
        print('Reading:' + work_filename)
        
        
        #Parse the LSSS work file to a dictionary
        with open(work_filename) as fd:
            doc = xmltodict.parse(fd.read())
            
        
                    
        #number of pings in file
        self.info.numberOfPings = np.int(doc['regionInterpretation']['timeRange']['@numberOfPings'])
        self.info.timeFirstPing = UNIXtime_to_epoce(float(doc['regionInterpretation']['timeRange']['@start']))
            
        
        #read ping times from ek60 file
        data = EK60.EK60()
        data.read_raw(raw_filename)
        ping_time = data.get_channel_data(frequencies=38000)[38000][0].ping_time
        self.info.ping_time = [(datetime.fromtimestamp(time.astype('uint64')/1000)- datetime(1601, 1, 1)).total_seconds()*1e9 for time in ping_time]
        self.info.channel_names = list(data.get_channel_data().keys())
        
        data.get_channel_data()['GPT  38 kHz 0090720749b8 1-1 ES38B'].soundspeed

        #test file
        if self.info.timeFirstPing!=self.info.ping_time[0]: 
            print('wronge time between first ping in work and snap')
        
        
        
        #If there isexcludedreagion
        if not not(doc['regionInterpretation']['exclusionRanges']):
            
            if len(doc['regionInterpretation']['exclusionRanges'])==1:
                timeRange = doc['regionInterpretation']['exclusionRanges']['timeRange']
                start_time = UNIXtime_to_epoce(float(timeRange['@start']))
                end_time = self.info.ping_time[self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings'])-1]
                start_depth = 0
                end_depth = 9999
            else:
                #exclusionRanges
                for timeRange in doc['regionInterpretation']['exclusionRanges']['timeRange']: 
                    start_time = UNIXtime_to_epoce(float(timeRange['@start']))
                    end_time = self.info.ping_time[self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings'])-1]
                    start_depth = 0
                    end_depth = 9999
        
        
        #Buble correction
        bubbleCorrection = np.ones(self.info.numberOfPings,np.float)
        if not not(doc['regionInterpretation']['bubbleCorrectionRanges']):
            if len(doc['regionInterpretation']['bubbleCorrectionRanges'])==1: 
                timeRange = doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']
                start_time = UNIXtime_to_epoce(float(timeRange['@start']))
                bubbleCorrection[range(self.info.ping_time.index(start_time),self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings']))]=float(timeRange['@bubbleCorrectionValue'])
            else:
                for timeRange in doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']: 
                    timeRange = doc['regionInterpretation']['bubbleCorrectionRanges']['timeRange']
                    start_time = UNIXtime_to_epoce(float(timeRange['@start']))
                    bubbleCorrection[range(self.info.ping_time.index(start_time),self.info.ping_time.index(start_time)+int(timeRange['@numberOfPings']))]=float(timeRange['@bubbleCorrectionValue'])
            
        
class ev_to_annotations (object):
    """Class for read EcoView .ev files to ices annotation class"""

    def __init__(self):
        print('Function is not implemented')
        
        
        
class icesnc_to_annotations (object):
    """Class for read ices .nc files to ices annotation class"""

    def __init__(self):
        print('Function is not implemented')
        
        