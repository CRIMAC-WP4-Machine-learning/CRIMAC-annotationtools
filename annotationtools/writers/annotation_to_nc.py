
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 08:46:06 2021

@author: sindrev
"""


from netCDF4 import Dataset 
import datetime
class annotation_to_nc (object):

    """Class for writing  .nc files from annotation class"""



    def __init__(self,filename='',annotation=[]):
        
        
        filename = '../data/work.nc'
        
        f=Dataset(filename,'w',format='NETCDF4')
        
        f.Conventions = 'CF-1.7, ACDD-1.3, SONAR-netCDF4-2.0'
        f.date_created = datetime.utcnow.strftime('%Y%m%dTH%M%SZ')
        f.keywords = 'scrutinisation mask, echosounder'
        f.license = 'None'
        f.mask_convention_authority = 'ICES, IMR'
        f.mask_convention_version = '0.1'
        f.rights = 'Unrestricted rights'
        f.summary = 'Contains definitions of echogram scrutiny masks'
        f.close()
        
            