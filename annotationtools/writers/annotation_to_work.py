# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 08:46:06 2021

@author: sindrev
"""

from decimal import Decimal
from echolab2.instruments import EK60, EK80
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import numpy as np

class annotation_to_work (object):

    """Class for writing LSSS .work files from annotation class"""



    def __init__(self,filename='',raw_filename='',annotation=[]):
        
            # Detect FileType
        def ek_detect(fname):
            with open(fname, 'rb') as f:
                file_header = f.read(8)
                file_magic = file_header[-4:]
                if file_magic.startswith(b'XML'):
                    return "EK80"
                elif file_magic.startswith(b'CON'):
                    return "EK60"
                else:
                    return None
            
            
        def ek_read(fname):
            if ek_detect(fname) == "EK80":
                ek80_obj = EK80.EK80()
                ek80_obj.read_raw(fname)
                return ek80_obj
            elif ek_detect(fname) == "EK60":
                ek60_obj = EK60.EK60()
                ek60_obj.read_raw(fname)
                return ek60_obj
                    
            
        
        
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
            timestamp1=(float(timestamp/1e9+(datetime(1601, 1, 1)-datetime(1970, 1, 1,1,0)).total_seconds()))
            return(timestamp)
            
            
            
            
            
        #Read ek data    
        raw_data = ek_read(raw_filename)
        
        #make a list of the different ping times per channel
        #It may be different between each channel if they are pinging sequentually
        ping_time = [None] * raw_data.n_channels
        for i in range(raw_data.n_channels):
            p_time = raw_data.get_channel_data(channel_numbers=i+1)[i+1][0].ping_time
            ping_time[i] = np.array([time for time in p_time])#np.array([(datetime.fromtimestamp(time.astype('uint64')/1000)- datetime(1601, 1, 1)).total_seconds()*1e9 for time in p_time])
        
        
        #If all ping times for each channel is equal, just make one vector
        #        keep_list= False
        p_time = []
        for i in range(1,raw_data.n_channels):
            p_time = np.hstack((p_time,ping_time[i]))
        
        
        ping_time = np.sort(np.unique(p_time))
        channel_ids=raw_data.channel_ids
        frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
         
        
            
        ping_time= np.sort(np.unique(annotation.pingTime))
        
        
        
        # create the file structure
        data = ET.Element('regionInterpretation')
        data.set('version','1.0')
        
        
        #Set the first line where we describe the time of first ping and 
        #number of pings in file
        timeRange = ET.SubElement(data, 'timeRange')
        timeRange.set('start',str('%.12E' % Decimal(epoce_to_UNIXtime(min(annotation.pingTime)))))
        timeRange.set('numberOfPings',str(len(np.unique(annotation.pingTime))))
        
        
        
        
        #write the information of mask per channel
        masking = ET.SubElement(data, 'masking')
        excludedData = annotation[annotation.acousticCat==0]
        channel_ids = list(set(excludedData.ChannelID))
        channel_ids.sort()
        for chn in channel_ids: 
            sub_excludedData = excludedData[excludedData.ChannelID == chn]
            mask = ET.SubElement(masking, 'mask')
            channel_ids.index(chn)
            mask.set('channelID',str(1+channel_ids.index(chn)))
            
            for p_time in np.sort(np.unique(sub_excludedData.pingTime)): 
                
                ping = ET.SubElement(mask, 'ping')
                
                ping.set('pingOffset',str(list(ping_time).index(p_time)))
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
        #LAYER           
        ###################################################################
        s_layer_data = annotation[annotation.priority==3]
        
        layerInterpretation = ET.SubElement(data, 'layerInterpretation')   
        boundaries = ET.SubElement(layerInterpretation, 'boundaries')   
        connectors = ET.SubElement(layerInterpretation, 'connectors')  
        layerDefinitions = ET.SubElement(layerInterpretation, 'layerDefinitions') 
        bound_id = 0
        connector_id = 0
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
            connectorRep.set('pingOffset',str(np.where(ping_time == min(layer_data.pingTime))[0][0]))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == min(layer_data.pingTime)].mask_depth_upper)))    
               
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-3))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(ping_time == max(layer_data.pingTime))[0][0]+1))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == max(layer_data.pingTime)].mask_depth_upper)))   
            
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-2))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(ping_time == min(layer_data.pingTime))[0][0]))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == min(layer_data.pingTime)].mask_depth_lower)))   
            
            connector = ET.SubElement(connectors, 'connector')     
            connector.set('id',str(connector_id-1))    
            connectorRep = ET.SubElement(connector, 'connectorRep') 
            connectorRep.set('pingOffset',str(np.where(ping_time == max(layer_data.pingTime))[0][0]+1))    
            connectorRep.set('depth',str(max(layer_data[layer_data.pingTime == max(layer_data.pingTime)].mask_depth_lower)))   
            
            layer_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
            
               
            layer = ET.SubElement(layerDefinitions, 'layer')  
            layer.set('hasBeenVisisted','true')
            layer.set('objectNumber','10')
            
            speciesInterpretationRoot = ET.SubElement(layer, 'speciesInterpretationRoot') 
            
            inter = layer_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
            
            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
            
            for chn in set(inter.ChannelID): 
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
        for s_id in set(s_school_data.ID): 
            s_id
            school_data=s_school_data[s_school_data.ID==s_id] 
            school = ET.SubElement(schoolInterpretation,'schoolMaskRep')
            school.set('referenceTime',str('%.12E' % Decimal(epoce_to_UNIXtime(list(school_data.pingTime)[0]))))
            school.set('hasBeenVisisted','true')
            school.set('objectNumber',str(s_count))
            
            spintroot = ET.SubElement(school,'speciesInterpretationRoot')
            
            inter = school_data[['proportion','ChannelID','acousticCat']].drop_duplicates()
            frequency = [int(x/1000) for x in list(raw_data.frequency_map)]
            chnid = list(set(inter.ChannelID))
            chnid.sort()
            for chn in chnid: 
                if chn != -1:
                    tmp = inter[inter.ChannelID == chn]
                    
                    interpRep = ET.SubElement(spintroot,'speciesInterpretationRep')
                    interpRep.set('frequency',str(frequency[channel_ids.index(chn)]))
                    for index,row in tmp.iterrows():
                        print(row)
                        species = ET.SubElement(interpRep,'species')
                        species.set('ID',str(row.acousticCat))
                        species.set('fraction',str(row.proportion))
                
            p_times = list(set(school_data.pingTime))
            p_times.sort()
            for p in p_times: 
                ping_idx=int(np.where(ping_time==p)[0][0]+1)
                sub = school_data[school_data.pingTime==p]
                tmp = list(list(sub.mask_depth_upper)+list(sub.mask_depth_lower))
                tmp.sort()
                pingMask = ET.SubElement(school,'pingMask')
                pingMask.set('relativePingNumber',str(ping_idx))
                pingMask.text = ' '.join(tmp)
            s_count +=1
        
        
        # create a new XML file with the results
        mydata = minidom.parseString(ET.tostring(data)).toprettyxml(indent="   ")
        myfile = open(filename, "w")
        myfile.write(str(mydata))
        myfile.close()
        
        
            