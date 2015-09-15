"""
@summary Module that contains the Experiment class
@author Aimee Stewart

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
from LmCommon.common.lmconstants import EXPERIMENTS_SERVICE
from LmServer.base.serviceobject import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule

# .............................................................................
class _Experiment(ServiceObject):
   """
   @summary: superclass for all types of experiments.  
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, dbid, userid, epsgcode, createTime, modTime, 
                description=None, metadataUrl=None, 
                serviceType=LMServiceType.EXPERIMENTS, moduleType=None):
      """
      @summary Constructor for the _Experiment class
      @param dbid: The primary key for the experiment database record.
      @param userid: The id for the owner of this experiment 
      @param createTime: Create Time/Date, in Modified Julian Day (MJD) format
      @param modTime: Last modification Time/Date, in MJD format
      """
      ServiceObject.__init__(self, userid, dbid, createTime, modTime, 
                             metadataUrl=metadataUrl, 
                             serviceType=serviceType, moduleType=moduleType)
      self.description = description
      self._epsg = epsgcode
      
#    def setId(self, expid):
#       """
#       @summary: Sets the database id on the object
#       @param id: The database id for the object
#       @note: Overrides ServiceObject.setId
#       """
#       ServiceObject.setId(self, expid)
      
   # .........................................
   @property
   def epsgcode(self):
      return self._epsg

