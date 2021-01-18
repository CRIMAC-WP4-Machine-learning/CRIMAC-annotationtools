
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 08:46:06 2021

@author: sindrev
"""


from netCDF4 import Dataset 
import datetime
import numpy as np
#import xarray as xr
class annotation_to_nc (object):

    """Class for writing  .nc files from annotation class"""



    def __init__(self,filename='',annotation=[]):
        
        version_id = 1
        version_comment = 'Converted from .work'
        
        
        
        #Create the netcdf file
        f=Dataset(filename,'w',format='NETCDF4')
        
        
        
        
        ###################################################################
        #General information
        ###################################################################
        f.Conventions = 'CF-1.7, ACDD-1.3, SONAR-netCDF4-2.0'
        f.date_created = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        f.keywords = 'scrutinisation mask, echosounder'
        f.license = 'None'
        f.mask_convention_authority = 'ICES, IMR'
        f.mask_convention_name = 'SONAR-netCDF4'
        f.mask_convention_version = '0.1'
        f.rights = 'Unrestricted rights'
        f.summary = 'Contains definitions of echogram scrutiny masks'
        f.title = 'Echogram scrutiny masks'
        
        
        
        
        
        ###################################################################
        #Defining and interpretation grp
        ###################################################################
        Interpretation_grp = f.createGroup('Interpretation')
        
        
        
        
        ###################################################################
        #Version group
        ###################################################################
        version_grp = Interpretation_grp.createGroup('v'+str(version_id))
        version_grp.version = version_id
        version_grp.version_author = 'SNV'
        version_grp.version_comment = version_comment
        version_grp.version_save_date = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        
        
        
        ###################################################################
        #channel_names=tmp.info.channel_names
        ###################################################################
        version_grp.createDimension('channels', len(annotation.info.channel_names))
        channel_names=version_grp.createVariable('channel_names', 'str',('channels')) 
        channel_names.long_name = 'Echosounder channel names'
        channel_names[:]=np.array(annotation.info.channel_names)
        
        
        
        version_grp.createDimension('mask_depth_t', len(annotation.mask))
        
        
        ###################################################################
        #Add the start time of each region
        ###################################################################
        channel_names=version_grp.createVariable('start_time', 'u8',('mask_depth_t',)) 
        channel_names.axis = 'T'
        channel_names.long_name = 'Timestamp of each mask point'
        channel_names.standard_name = 'time'
        channel_names.units = 'nanoseconds since 1601-01-01 00:00:00Z'
        channel_names[:]=np.array([m.start_time for m in annotation.mask])

        
        ###################################################################
        #Add the end time of each reagion
        ###################################################################
        channel_names=version_grp.createVariable('end_time', 'u8',('mask_depth_t',)) 
        channel_names.axis = 'T'
        channel_names.long_name = 'Timestamp of each mask point'
        channel_names.standard_name = 'time'
        channel_names.units = 'nanoseconds since 1601-01-01 00:00:00Z'
        channel_names[:,]=np.array([m.end_time for m in annotation.mask])
        
        
        
        ###################################################################
        #Add the max depth of each region
        ###################################################################
        channel_names=version_grp.createVariable('max_depth', 'f4',('mask_depth_t',)) 
        channel_names.long_name = 'Maximum depth for each regions'
        channel_names.valid_min = float(0.0)
        channel_names.units = 'm'
        channel_names[:,]=np.array([m.max_depth for m in annotation.mask])
        
        
        
        ###################################################################
        #Add the min depth of each region
        ###################################################################
        channel_names=version_grp.createVariable('min_depth', 'f4',('mask_depth_t',)) 
        channel_names.long_name = 'Minimum depth for each regions'
        channel_names.valid_min = float(0.0)
        channel_names.units = 'm'
        channel_names[:,]=np.array([m.min_depth for m in annotation.mask])
        
        
        
        
        
        ###################################################################
        #Grab mask info of depth and time
        ###################################################################
        mask_depth = [(m.mask_depth) for m in annotation.mask]
        mask_times = [m.mask_times for m in annotation.mask]
        
        
        
        
        ###################################################################
        #Define a raged array structure
        ###################################################################
        masks_depths_t = version_grp.createVLType(np.float64,'masks_depths_t')
        
        
        
        
        
        ###################################################################
        #Add the mask depth intervall
        #This is not handled vell
        ###################################################################
        m_depth=version_grp.createVariable('mask_depth', masks_depths_t,('mask_depth_t',)) 
        m_depth.long_name = 'Depth pairs of mask'
        m_depth.valid_min = float(0.0)
        m_depth.units = 'm'
            
                
            
            
        
        ###################################################################
        #Add the mask times
        ###################################################################
        m_times=version_grp.createVariable('mask_times', masks_depths_t,('mask_depth_t',)) 
        m_times.axis = 'T'
        m_times.calendar = 'gregorian'
        m_times.long_name = 'Timestamp of each mask point'
        m_times.standard_name = 'time'
        m_times.valid_min = '0.0'
        m_times.units = 'nanoseconds since 1601-01-01 00:00:00Z'
        
        
        
        
        ###################################################################
        #Some workaround to adress masks with holes within it
        #Considere to split the masks depth into upper and lower
        ###################################################################
        for i in range(len(mask_depth)):
            tmp_depth = mask_depth[i]
            tmp_time = mask_times[i]
            t_depth = []
            t_time = []
            for ii in range(len(tmp_depth)):
                if ii == 0: 
                    t_depth = tmp_depth[ii]
                    t_time = np.repeat(tmp_time[ii],len(tmp_depth[ii]))
                else: 
                    t_depth = np.hstack((t_depth,tmp_depth[ii]))
                    t_time = np.hstack((t_time,np.repeat(tmp_time[ii],len(tmp_depth[ii]))))
            
            m_depth[i] =np.array(t_depth)
            m_times[i]=t_time
        
        
            
            
            
        enum_dict = {u'empty_water': 0, u'no_data': 1,u'analysis':2,u'track':3,u'marker':4}
        region_t=version_grp.createEnumType(np.uint8,'region_t',enum_dict)
    
        channel_names=version_grp.createVariable('region_type', region_t,('mask_depth_t',)) 
        channel_names.long_name = 'Region Type'
        tmp = [m.region_type for m in annotation.mask] 
        channel_names[:] =[enum_dict[t] for t in tmp]
        
        
        
        
#        channel_names=version_grp.createVariable('region_channels', masks_depths_t,('mask_depth_t',)) 
#        channel_names.description = 'Bit mask derived from channel_names (index 1 of channel_names = bit 1, index 2 = bit 2, etc). Set bits in excess of the number of channels are to be ignored.'
#        channel_names.long_name = 'Echosounder channels that this region applies to'
#        
#        tmp = [m.region_channels for m in annotation.mask]
#        for i in range(len(tmp)):
#            channel_names[i]=np.array(tmp[i])
        
        
        f.close()
            
        
        
