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
import os, struct
from echolab2.instruments.util.date_conversion import nt_to_unix, unix_to_datetime
from echolab2.instruments.util.simrad_raw_file import RawSimradFile



class annotation_to_work (object):

    """Class for writing LSSS .work files from annotation class"""






    def __init__(self,filename='',raw_filename='',annotation=[]):
            
        
        
        def reversed_depthConverter(depth): 
            dp = list()
            for idx in range(len(depth)):
                if idx>0: 
                    dp.append(str(round(depth[idx] -depth[idx-1],7)))
                else: 
                    dp.append(str(depth[idx]))
                 
            return(' '.join(dp))
    
        
        
        
        # =============================================================================
        # Handle the file info and store the data  
        # =============================================================================
        in_files=raw_filename
        pre, ext = os.path.splitext(in_files)
        in_files=pre+'.idx'
        
        fid = RawSimradFile(in_files, 'r')
        config = fid.read(1)
        run = True
        ping_time_IDX = list()
        ping_time = list()
        while run == True:
            try: 
                idx_datagram = fid.read(1)
                raw_string=struct.unpack('4sLLLdddLL', idx_datagram)
                p_time = nt_to_unix((raw_string[1], raw_string[2]),return_datetime=False)
                ping_time_IDX.append(round(p_time,3))
                ping_time.append((unix_to_datetime(round(p_time,3)).replace(tzinfo=None)))
                
            except:
                run = False
        IDX_info = pd.DataFrame(data={'ping_time':ping_time,
                                 'ping_time_IDX':ping_time_IDX})
     
        
        
        # =============================================================================
        # Get channel info
        # =============================================================================
        channel_ids=list(config['configuration'].keys())#data.channel_ids
        frequency=[(int(config['configuration'][p]['frequency']/1000)) for p in config['configuration'].keys()]
        
        
        
        
        # =============================================================================
        # Start making the file structure
        # =============================================================================
        data = ET.Element('regionInterpretation')
        data.set('version','1.0')
        
        
        
        
        # =============================================================================
        # add first line of the work file, for some reason we need to add one houre!!!
        # =============================================================================
        times_=(min(annotation.pingTime)-np.datetime64(datetime(1970, 1, 1, 1, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
        timeRange = ET.SubElement(data, 'timeRange')
        timeRange.set('start',str('%.12E' % Decimal(times_)))
        timeRange.set('numberOfPings',str(len(np.unique(annotation.pingTime))))
        
        
        
        
        # =============================================================================
        # Process the exclude mask
        # =============================================================================
        masking = ET.SubElement(data, 'masking')
        excludedData = annotation[annotation.acousticCat==0]
        channel_ids = list(set(excludedData.ChannelID))
        channel_ids.sort()
        for chn in channel_ids: 
            sub_excludedData = excludedData[excludedData.ChannelID == chn]
            mask = ET.SubElement(masking, 'mask')
            channel_ids.index(chn)
            mask.set('channelID',str(1+channel_ids.index(chn)))
            for p_time in sorted(set(sub_excludedData.pingTime)): 
                ping = ET.SubElement(mask, 'ping')
                p_idx = np.where(IDX_info.ping_time==p_time)[0][0]
                ping.set('pingOffset',str(p_idx))
                sub_data = sub_excludedData[sub_excludedData.pingTime == p_time]
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
        s_layer_data = annotation[annotation.priority==3]
        layerInterpretation = ET.SubElement(data, 'layerInterpretation')   
        boundaries = ET.SubElement(layerInterpretation, 'boundaries')   
        connectors = ET.SubElement(layerInterpretation, 'connectors')  
        layerDefinitions = ET.SubElement(layerInterpretation, 'layerDefinitions') 
        bound_id = 0
        connector_id = 0
        channel_ids=list(config['configuration'].keys())#data.channel_ids
        for id_s in set(s_layer_data.ID):
                
            layer_data=s_layer_data[s_layer_data.ID==id_s]
            
             
            curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
            curveBoundary.set('id',str(bound_id))
            bound_id+=1
            curveBoundary.set('startConnector',str(connector_id))
            connector_id+=1
            curveBoundary.set('endConnector',str(connector_id))     
            connector_id+=1 
            curveRep = ET.SubElement(curveBoundary, 'curveRep')   
            depths = ET.SubElement(curveRep, 'depths')   
            depths.text = ' '.join([str(x) for x in layer_data.drop(['ChannelID','priority','acousticCat','proportion'], axis=1).drop_duplicates().mask_depth_upper])  
            
            curveBoundary = ET.SubElement(boundaries, 'curveBoundary')        
            curveBoundary.set('id',str(bound_id))
            bound_id+=1
            curveBoundary.set('startConnector',str(connector_id))
            connector_id+=1
            curveBoundary.set('endConnector',str(connector_id))   
            connector_id+=1   
            curveRep = ET.SubElement(curveBoundary, 'curveRep')   
            depths = ET.SubElement(curveRep, 'depths')   
            depths.text = ' '.join([str(x) for x in layer_data.drop(['ChannelID','priority','acousticCat','proportion'], axis=1).drop_duplicates().mask_depth_lower])  
            
            
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
            connectorRep.set('pingOffset',str(np.where(IDX_info.ping_time == min(layer_data.pingTime))[0][0]))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == min(layer_data.pingTime)].mask_depth_upper)))    
               
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-3))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(IDX_info.ping_time == max(layer_data.pingTime))[0][0]+1))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == max(layer_data.pingTime)].mask_depth_upper)))   
            
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-2))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(IDX_info.ping_time == min(layer_data.pingTime))[0][0]))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == min(layer_data.pingTime)].mask_depth_lower)))   
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-1))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(IDX_info.ping_time == max(layer_data.pingTime))[0][0]+1))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == max(layer_data.pingTime)].mask_depth_lower)))   
            
            layer_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
            
               
            layer = ET.SubElement(layerDefinitions, 'layer')  
            layer.set('hasBeenVisisted','true')
            layer.set('objectNumber','10')
            
            speciesInterpretationRoot = ET.SubElement(layer, 'speciesInterpretationRoot') 
            
            inter = layer_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
            
#            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
            
            for chn in set(inter.ChannelID): 
                if not chn=='-1':
                    if not chn == -1:
                        dix =channel_ids.index(chn)
                        speciesInterpretationRep = ET.SubElement(speciesInterpretationRoot, 'speciesInterpretationRep') 
                        speciesInterpretationRep.set('frequency',str(frequency[dix]))
        #                
                        for index,row in inter[inter.ChannelID==chn].iterrows():
                            species= ET.SubElement(speciesInterpretationRep, 'species') 
                            species.set('ID',str(row.acousticCat))
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

        
        s_school_data = annotation[annotation.priority==2]
        s_count = 1
        channel_ids=list(config['configuration'].keys())#data.channel_ids
        for s_id in set(s_school_data.ID): 
            s_id
            school_data=s_school_data[s_school_data.ID==s_id] 
            school = ET.SubElement(schoolInterpretation,'schoolMaskRep')
            
            times_=(min(school_data.pingTime)-np.datetime64(datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz_utc).replace(tzinfo=None))).total_seconds()
            school.set('referenceTime',str('%.12E' % Decimal(times_)))
            school.set('hasBeenVisisted','true')
            school.set('objectNumber',str(s_count))
            
            spintroot = ET.SubElement(school,'speciesInterpretationRoot')
            
            inter = school_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
#            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
            chnid = list(set(inter.ChannelID))
            chnid.sort()
            for chn in chnid: 
                if chn != -1:
                    if chn != '-1': 
                        tmp = inter[inter.ChannelID == chn]
                        
                        interpRep = ET.SubElement(spintroot,'speciesInterpretationRep')
                        interpRep.set('frequency',str(frequency[channel_ids.index(chn)]))
                        for index,row in tmp.iterrows():
                            species = ET.SubElement(interpRep,'species')
                            species.set('ID',str(row.acousticCat))
                            species.set('fraction',str(row.proportion))
                    
            p_times = list(set(school_data.pingTime))
            p_times.sort()
            for p in p_times: 
                ping_idx=int(np.where(IDX_info.ping_time==p)[0][0]+1)
                sub = school_data[school_data.pingTime==p]
                tmp = list(list(sub.mask_depth_upper)+list(sub.mask_depth_lower))
                tmp.sort()
                tmp = [str(t) for t in tmp]
                pingMask = ET.SubElement(school,'pingMask')
                pingMask.set('relativePingNumber',str(ping_idx))
                pingMask.text = ' '.join(tmp)
            s_count +=1
        
        
        # create a new XML file with the results
        mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
        myfile = open(filename, "w")
        myfile.write(str(mydata))
        myfile.close()
        
        
            