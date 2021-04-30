# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 08:46:06 2021

@author: sindrev
"""

from decimal import Decimal
import pandas as pd
from pytz import utc as pytz_utc
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import numpy as np
import decimal
import os, struct
from echolab2.instruments.util.date_conversion import nt_to_unix, unix_to_datetime
from echolab2.instruments.util.simrad_raw_file import RawSimradFile



class annotation_to_work (object):

    """Class for writing LSSS .work files from annotation class"""






    def __init__(self,out_folder='',raw_folder='',annotation=[]):
            
        
        
        def reversed_depthConverter(depth): 
            dp = list()
            for idx in range(len(depth)):
                if idx>0: 
                    dp.append(str(round(depth[idx] -depth[idx-1],7)))
                else: 
                    dp.append(str(depth[idx]))
                 
            return(' '.join(dp))
    
        
        
        # Use round towards zero
        decimal.getcontext().rounding = decimal.ROUND_DOWN
        
        
        
        #Scan IDX files
        print('Scanning idx files: ')
        
        #For bookkeeping
        ping_time_IDX = list()
        ping_time_IDX_corrected = list()
        ping_time_datetime = list()
        idx_file_list = list()
        
        #loop through each file
        for subdir, dirs, files in os.walk(raw_folder):
            for file in files:
                ext = os.path.splitext(file)[-1].lower()
                if ext in '.idx':
                    print('   - ' + file)
                    #Read start of file
                    in_files= os.path.join(subdir, file)
                    fid = RawSimradFile(in_files, 'r')
                    config = fid.read(1)
                    channel_ids = list(config['configuration'].keys())#data.channel_ids
                    timestamp = config['timestamp'].timestamp()
                    time_diff = None
                    run = True
                    #Read ping times
                    while run:
                        try:
                            idx_datagram = fid.read(1)
                            raw_string=struct.unpack('=4sLLLdddLL', idx_datagram)
                            p_time = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)
                            if time_diff == None: 
                                time_diff = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)-float(round(decimal.Decimal(timestamp), 3))
                                
                            p_time = float(round(decimal.Decimal(p_time), 3))
                            p_time_corrected = p_time-float(round(decimal.Decimal(time_diff),3))
                            ping_time_IDX.append(p_time)
                            ping_time_IDX_corrected.append(p_time_corrected)
                            ping_time_datetime.append(np.datetime64(unix_to_datetime(p_time_corrected)))
                            idx_file_list.append(file)
                        except:
                            run = False
                            
                            
                        
        scanned_df = pd.DataFrame(data={'ping_time_IDX': ping_time_IDX,
                            'ping_time_IDX_corrected':ping_time_IDX_corrected,
                            'ping_time_datetime':ping_time_datetime,
                            'idx_file_list':idx_file_list})
#        
#        
#        # =============================================================================
#        # Handle the file info and store the data  
#        # =============================================================================
#        in_files=raw_filename
#        pre, ext = os.path.splitext(in_files)
#        #Switch to idx in case raw was selected
#        in_files=pre+'.idx'
#        
#        fid = RawSimradFile(in_files, 'r')
#        config = fid.read(1)
#        run = True
#        ping_time_IDX = list()
#        ping_time = list()
#        while run == True:
#            try: 
#                idx_datagram = fid.read(1)
#                raw_string=struct.unpack('4sLLLdddLL', idx_datagram)
#                p_time = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)
#                ping_time_IDX.append(round(p_time,3))
#                ping_time.append((unix_to_datetime(round(p_time,3)).replace(tzinfo=None)))
#                
#            except:
#                run = False
#                
#                
#                
#        #Read the ping times
#        fid = RawSimradFile(in_files, 'r')
#        config = fid.read(1)
#        channel_ids = list(config['configuration'].keys())#data.channel_ids
#        timestamp = config['timestamp'].timestamp()
#        ping_time_IDX = list()
#
#        run = True
#        while run:
#            try:
#                idx_datagram = fid.read(1)
#                raw_string=struct.unpack('=4sLLLdddLL', idx_datagram)
#                p_time = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)
#                ping_time_IDX.append(float(round(decimal.Decimal(p_time), 3)))
#            except:
#                run = False
#                
#                
#                
#        ping_time = np.array(ping_time_IDX)
##        time_diff = np.datetime64(unix_to_datetime(ping_time[0])) - np.datetime64(unix_to_datetime(float(round(decimal.Decimal(timestamp), 3))))
#        time_diff = ping_time[0]-float(round(decimal.Decimal(timestamp), 3))
#        self.raw_work_timediff = time_diff
#        
#        
        
#        ping_time = ping_time-time_diff
        s_count = 1
        objectNumber = 1
        for idx_file in np.unique(scanned_df['idx_file_list']):
            anno = (annotation[annotation['ping_time'].isin(scanned_df[scanned_df['idx_file_list']==idx_file]['ping_time_datetime'])])
            s_df = scanned_df[scanned_df['idx_file_list']==idx_file]
    
            if not anno.empty:
                
                IDX_info = pd.DataFrame(data={'ping_time':s_df['ping_time_IDX'],
                                         'ping_time_IDX':s_df['ping_time_IDX']})
     
                
                
                # =============================================================================
                # Get channel info
                # =============================================================================
                channel_ids=list(config['configuration'].keys())#data.channel_ids
                frequency=[(int(config['configuration'][p]['frequency']/1000)) for p in config['configuration'].keys()]
                
                
                
                
                # =============================================================================
                # Start making the file structure
                # =============================================================================
                data = ET.Element('regionInterpretation')
                data.set('version','2')
                
                
                
                
                # =============================================================================
                # add first line of the work file
                # =============================================================================
        #        times_=(min(annotation.ping_time)-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
                
                
                timeRange = ET.SubElement(data, 'timeRange')
                timeRange.set('start',str('%.12E' % Decimal(min(IDX_info.ping_time))))
                timeRange.set('numberOfPings',str(len(np.unique(IDX_info.ping_time))))
                
                
                
                # =============================================================================
                # Process the exclude mask
                # =============================================================================
                ET.SubElement(data, 'exclusionRanges')
                masking = ET.SubElement(data, 'masking')
                #Grab all data that shall be excluded (0.0)
                excludedData = anno[anno.acoustic_category.astype(float)==0.0]
                #Grab and sort channel idx
                channel_ids = list(set(excludedData.channel_id))
                channel_ids.sort()
                #loop through each channel
                for chn in channel_ids: 
                    #Subsett excluded pixels per channel
                    sub_excludedData = excludedData[excludedData.channel_id == chn]
                    mask = ET.SubElement(masking, 'mask')
                    channel_ids.index(chn)
                    mask.set('channelID',str(1+channel_ids.index(chn)))
                    #loop thorugh each time
                    for p_time in sorted(set(sub_excludedData.ping_time)): 
                        ping = ET.SubElement(mask, 'ping')
        #                p_idx = np.where(IDX_info.ping_time==p_time)[0][0]
                        #Grab ping offset
                        p_idx = np.where(abs(IDX_info.ping_time-(p_time-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds())==        min(abs(IDX_info.ping_time-(p_time-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds())))[0][0]
                        ping.set('pingOffset',str(p_idx))
                        sub_data = sub_excludedData[sub_excludedData.ping_time == p_time]
                        if sub_data.shape[0]==1: 
                            inn_mask = list([str(float(sub_data.mask_depth_upper)),str(float(sub_data.mask_depth_lower-sub_data.mask_depth_upper))])
                            ping.text=' '.join(inn_mask)
                        else:
                            inn_mask = list()
                            for index, row in sub_data.iterrows():
                                inn_mask.append(row.mask_depth_upper)
                                inn_mask.append(row.mask_depth_lower)
                            ping.text=reversed_depthConverter(inn_mask)
                                         
                            
                            
                            
                            
                            
                ###################################################################   
                # Process the layer stuff           
                ###################################################################
                s_layer_data = anno[anno.priority==3]
                layerInterpretation = ET.SubElement(data, 'layerInterpretation')   
                boundaries = ET.SubElement(layerInterpretation, 'boundaries')   
                connectors = ET.SubElement(layerInterpretation, 'connectors')  
                layerDefinitions = ET.SubElement(layerInterpretation, 'layerDefinitions') 
                bound_id = 0
                connector_id = 0
                channel_ids=list(config['configuration'].keys())#data.channel_ids
#                objectNumber = 1
                if s_layer_data.empty: 
                    print('No layer is defined, Make a dummy layer')
                    curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
                    curveBoundary.set('id',str(bound_id))
                    bound_id+=1
                    curveBoundary.set('startConnector',str(connector_id))
                    connector_id+=1
                    curveBoundary.set('endConnector',str(connector_id))     
                    connector_id+=1 
                    curveRep = ET.SubElement(curveBoundary, 'curveRep')   
                    depths = ET.SubElement(curveRep, 'depths')   
                    depths.text = ' '.join([str(x) for x in np.repeat(0,len(IDX_info.ping_time))])
                    
                    curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
                    curveBoundary.set('id',str(bound_id))
                    bound_id+=1
                    curveBoundary.set('startConnector',str(connector_id))
                    connector_id+=1
                    curveBoundary.set('endConnector',str(connector_id))   
                    connector_id+=1   
                    curveRep = ET.SubElement(curveBoundary, 'curveRep')   
                    depths = ET.SubElement(curveRep, 'depths')   
                    depths.text = ' '.join([str(x) for x in np.repeat(350,len(IDX_info.ping_time))])
        #                depths.text = ' '.join([str(x) for x in layer_data.drop(['channel_id','priority','acoustic_category','proportion'], axis=1).drop_duplicates().mask_depth_lower])  
                    
                    
                    verticalBoundary = ET.SubElement(boundaries, 'verticalBoundary')     
                    verticalBoundary.set('id',str(bound_id))
                    bound_id+=1   
                    verticalBoundary.set('startConnector',str(connector_id-4))    
                    verticalBoundary.set('endConnector',str(connector_id-2))    
                    
                    verticalBoundary = ET.SubElement(boundaries, 'verticalBoundary')     
                    verticalBoundary.set('id',str(bound_id))
                    bound_id+=1  
                    verticalBoundary.set('startConnector',str(connector_id-3))    
                    verticalBoundary.set('endConnector',str(connector_id-1))    
                                 
                    
                    
                    connector = ET.SubElement(connectors, 'connector')     
                    connector.set('id',str(connector_id-4))    
                    connectorRep = ET.SubElement(connector, 'connectorRep') 
                    connectorRep.set('pingOffset',str(0))   
                    connectorRep.set('depth',str(0))    
                       
                    
                    connector = ET.SubElement(connectors, 'connector')     
                    connector.set('id',str(connector_id-3))    
                    connectorRep = ET.SubElement(connector, 'connectorRep') 
                    connectorRep.set('pingOffset',str(len(IDX_info.ping_time)))    
                    connectorRep.set('depth',str(0))   
                    
                    
                    connector = ET.SubElement(connectors, 'connector')     
                    connector.set('id',str(connector_id-2))    
                    connectorRep = ET.SubElement(connector, 'connectorRep') 
                    connectorRep.set('pingOffset',str(0))    
                    connectorRep.set('depth',str(350))   
                    
                    connector = ET.SubElement(connectors, 'connector')     
                    connector.set('id',str(connector_id-1))    
                    connectorRep = ET.SubElement(connector, 'connectorRep') 
                    connectorRep.set('pingOffset',str(len(IDX_info.ping_time)))    
                    connectorRep.set('depth',str(350))   
                    
                    
                       
                    layer = ET.SubElement(layerDefinitions, 'layer')  
                    layer.set('hasBeenVisisted','true')
                    layer.set('objectNumber',str(objectNumber))
                    
                    speciesInterpretationRoot = ET.SubElement(layer, 'speciesInterpretationRoot') 
                    
                        
                    boundaries_ = ET.SubElement(layer, 'boundaries') 
                    curveBoundary = ET.SubElement(boundaries_, 'curveBoundary') 
                    curveBoundary.set('id',str(bound_id-4))
                    curveBoundary.set('isUpper','true')
                    curveBoundary = ET.SubElement(boundaries_, 'curveBoundary') 
                    curveBoundary.set('id',str(bound_id-3))
                    curveBoundary.set('isUpper','false')
                    
                    verticalBoundary = ET.SubElement(boundaries_, 'verticalBoundary') 
                    verticalBoundary.set('id',str(bound_id-2))
                    verticalBoundary = ET.SubElement(boundaries_, 'verticalBoundary') 
                    verticalBoundary.set('id',str(bound_id-1))
                
                    
                else: 
                  
                    for id_s in set(s_layer_data.object_id):
                            
                        layer_data=s_layer_data[s_layer_data.object_id==id_s]
                         
                        curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
                        curveBoundary.set('id',str(bound_id))
                        bound_id+=1
                        curveBoundary.set('startConnector',str(connector_id))
                        connector_id+=1
                        curveBoundary.set('endConnector',str(connector_id))     
                        connector_id+=1 
                        curveRep = ET.SubElement(curveBoundary, 'curveRep')   
                        depths = ET.SubElement(curveRep, 'depths')   
                        depths.text = ' '.join([str(x) for x in layer_data.drop(['channel_id','priority','acoustic_category','proportion'], axis=1).drop_duplicates().mask_depth_upper])  
                        
                        curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
                        curveBoundary.set('id',str(bound_id))
                        bound_id+=1
                        curveBoundary.set('startConnector',str(connector_id))
                        connector_id+=1
                        curveBoundary.set('endConnector',str(connector_id))   
                        connector_id+=1   
                        curveRep = ET.SubElement(curveBoundary, 'curveRep')   
                        depths = ET.SubElement(curveRep, 'depths')   
                        depths.text = ' '.join([str(x) for x in layer_data.drop(['channel_id','priority','acoustic_category','proportion'], axis=1).drop_duplicates().mask_depth_lower])  
                        
                        
                        verticalBoundary = ET.SubElement(boundaries, 'verticalBoundary')     
                        verticalBoundary.set('id',str(bound_id))
                        bound_id+=1   
                        verticalBoundary.set('startConnector',str(connector_id-4))    
                        verticalBoundary.set('endConnector',str(connector_id-2))    
                        
                        verticalBoundary = ET.SubElement(boundaries, 'verticalBoundary')     
                        verticalBoundary.set('id',str(bound_id))
                        bound_id+=1  
                        verticalBoundary.set('startConnector',str(connector_id-3))    
                        verticalBoundary.set('endConnector',str(connector_id-1))    
                                     
                        
                        
                        connector = ET.SubElement(connectors, 'connector')     
                        connector.set('id',str(connector_id-4))    
                        connectorRep = ET.SubElement(connector, 'connectorRep') 
                        
                        
                        
                        time_min=(min(layer_data.ping_time)-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
                        connectorRep.set('pingOffset',str(np.where(round(IDX_info.ping_time,3) == round(time_min,3))[0][0]))   
                        connectorRep.set('depth',str(max(layer_data[layer_data.ping_time == min(layer_data.ping_time)].mask_depth_upper)))    
                           
                        
                        time_max=(max(layer_data.ping_time)-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
                        connector = ET.SubElement(connectors, 'connector')     
                        connector.set('id',str(connector_id-3))    
                        connectorRep = ET.SubElement(connector, 'connectorRep') 
                        connectorRep.set('pingOffset',str(np.where(round(IDX_info.ping_time,3) ==round(time_max,3))[0][0]+1))    
                        connectorRep.set('depth',str(max(layer_data[layer_data.ping_time == max(layer_data.ping_time)].mask_depth_upper)))   
                        
                        
                        connector = ET.SubElement(connectors, 'connector')     
                        connector.set('id',str(connector_id-2))    
                        connectorRep = ET.SubElement(connector, 'connectorRep') 
                        connectorRep.set('pingOffset',str(np.where(round(IDX_info.ping_time,3) == round(time_min,3))[0][0]))    
                        connectorRep.set('depth',str(max(layer_data[layer_data.ping_time == min(layer_data.ping_time)].mask_depth_lower)))   
                        
                        connector = ET.SubElement(connectors, 'connector')     
                        connector.set('id',str(connector_id-1))    
                        connectorRep = ET.SubElement(connector, 'connectorRep') 
                        connectorRep.set('pingOffset',str(np.where(round(IDX_info.ping_time,3) == round(time_max,3))[0][0]+1))    
                        connectorRep.set('depth',str(max(layer_data[layer_data.ping_time == max(layer_data.ping_time)].mask_depth_lower)))   
                        
                        
                           
                        layer = ET.SubElement(layerDefinitions, 'layer')  
                        layer.set('hasBeenVisisted','true')
                        layer.set('objectNumber',str(objectNumber))
                        objectNumber+=1
                        
                        speciesInterpretationRoot = ET.SubElement(layer, 'speciesInterpretationRoot') 
                        
                        inter = layer_data[['proportion','channel_id','acoustic_category']].drop_duplicates()
                        
            #            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
                        
                        for chn in set(inter.channel_id): 
                            if not chn=='-1.0':
                                if not chn == -1:
                                    dix =channel_ids.index(chn)
                                    speciesInterpretationRep = ET.SubElement(speciesInterpretationRoot, 'speciesInterpretationRep') 
                                    speciesInterpretationRep.set('frequency',str(frequency[dix]))
                    #                
                                    for index,row in inter[inter.channel_id==chn].iterrows():
                                        species= ET.SubElement(speciesInterpretationRep, 'species') 
                                        species.set('ID',str(row.acoustic_category))
                                        species.set('fraction',str(row.proportion))
                        
                        
                        
                        boundaries_ = ET.SubElement(layer, 'boundaries') 
                        curveBoundary = ET.SubElement(boundaries_, 'curveBoundary') 
                        curveBoundary.set('id',str(bound_id-4))
                        curveBoundary.set('isUpper','true')
                        curveBoundary = ET.SubElement(boundaries_, 'curveBoundary') 
                        curveBoundary.set('id',str(bound_id-3))
                        curveBoundary.set('isUpper','false')
                        
                        verticalBoundary = ET.SubElement(boundaries_, 'verticalBoundary') 
                        verticalBoundary.set('id',str(bound_id-2))
                        verticalBoundary = ET.SubElement(boundaries_, 'verticalBoundary') 
                        verticalBoundary.set('id',str(bound_id-1))
                
                                    
                            
        #        break
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                schoolInterpretation = ET.SubElement(data, 'schoolInterpretation')
        
                
                s_school_data = anno[anno.priority==2]
#                s_count = 1
                channel_ids=list(config['configuration'].keys())#data.channel_ids
                for s_id in set(s_school_data.object_id): 
                    school_data=s_school_data[s_school_data.object_id==s_id] 
                    school = ET.SubElement(schoolInterpretation,'schoolMaskRep')
                    
        #            times_=(min(school_data.ping_time)-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
        #            school.set('referenceTime',str('%.12E' % Decimal(times_)))
                    school.set('hasBeenVisisted','true')
                    school.set('objectNumber',str(s_count))
                    
                    spintroot = ET.SubElement(school,'speciesInterpretationRoot')
                    
                    inter = school_data[['proportion','channel_id','acoustic_category']].drop_duplicates()
        #            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
                    chnid = list(set(inter.channel_id))
                    chnid.sort()
                    for chn in chnid: 
                        if chn != -1:
                            if chn != '-1': 
                                tmp = inter[inter.channel_id == chn]
                                
                                interpRep = ET.SubElement(spintroot,'speciesInterpretationRep')
                                interpRep.set('frequency',str(frequency[channel_ids.index(chn)]))
                                for index,row in tmp.iterrows():
                                    species = ET.SubElement(interpRep,'species')
                                    species.set('ID',str(row.acoustic_category))
                                    species.set('fraction',str(row.proportion))
                            
                    p_times = list(set(school_data.ping_time))
                    p_times.sort()
                    for p in p_times: 
                        
                        
                        time_=(p-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
                    
                        ping_idx=int(np.where((abs(IDX_info.ping_time-time_))==min(abs(IDX_info.ping_time-time_)))[0][0]+1)
                        sub = school_data[school_data.ping_time==p]
                        sub.channel_id = None
                        del sub['channel_id']
                        sub=sub.drop_duplicates()
                        tmp = list(list(sub.mask_depth_upper)+list(sub.mask_depth_lower+0.1))
                        tmp.sort()
                        tmp = [str(t) for t in tmp]
                        pingMask = ET.SubElement(school,'pingMask')
                        pingMask.set('relativePingNumber',str(ping_idx))
                        pingMask.text = ' '.join(tmp)
                    s_count +=1
                
                
                # create a new XML file with the results
                mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
                myfile = open(os.path.join(out_folder,out_folder,os.path.splitext(idx_file)[0]+'.work'), "w")
                myfile.write(str(mydata))
                myfile.close()
        
        
            