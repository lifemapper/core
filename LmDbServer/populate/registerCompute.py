"""
@summary: Register LmCompute instances, and their primary contacts,
          authorized to access user data on this LmServer 
@author: Aimee Stewart

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from LmServer.common.localconstants import (COMPUTE_NAME, COMPUTE_IP, 
            COMPUTE_CONTACT_USERID, COMPUTE_CONTACT_EMAIL, 
            COMPUTE_CONTACT_FIRSTNAME, COMPUTE_CONTACT_LASTNAME, 
            COMPUTE_INSTITUTION, COMPUTE_ADDR1, COMPUTE_ADDR2, COMPUTE_ADDR3)
from LmDbServer.populate.computeMeta import LM_COMPUTE_INSTANCES

from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.localconstants import DATA_PATH
from LmServer.common.log import DebugLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.scribe import Scribe

import mx.DateTime as DT
import os

# ...............................................
def getOptionalVal(dict, key):
   try:
      return dict[key]
   except:
      return None
   
# ...............................................
def registerConfiguredCompute(scribe):
   try:
      if (COMPUTE_NAME is not null and COMPUTE_IP is not null and  
          COMPUTE_CONTACT_USERID is not null):
         crContact = LMUser(COMPUTE_CONTACT_USERID, COMPUTE_CONTACT_EMAIL, '', 
                            firstName=COMPUTE_CONTACT_FIRSTNAME, 
                            lastName=COMPUTE_CONTACT_FIRSTNAME, 
                            institution=COMPUTE_INSTITUTION, 
                            addr1=COMPUTE_ADDR1, addr2=COMPUTE_ADDR2, 
                            addr3=COMPUTE_ADDR3, modTime=currtime)
         crMachine = LMComputeResource(COMPUTE_NAME, COMPUTE_IP, 
                           COMPUTE_CONTACT_USERID, createTime=currtime, 
                           modTime=currtime, hbTime=currtime)
         compResource = scribe.registerComputeResource(crMachine, crContact)
         
   except Exception, e:
      raise LMError(currargs='Failed to insert LmCompute %s' % COMPUTE_NAME)
         
# ...............................................
def registerStandardComputes(scribe):
   try:
      for name, lmc in LM_COMPUTE_INSTANCES.iteritems():      
         crContact = LMUser(lmc['contactid'], lmc['email'], lmc['password'], 
                            isEncrypted=getOptionalVal(lmc,'encrypted'), 
                            firstName=getOptionalVal(lmc,'first'), 
                            lastName=getOptionalVal(lmc,'last'), 
                            institution=getOptionalVal(lmc,'institution'), 
                            addr1=getOptionalVal(lmc,'addr1'), 
                            addr2=getOptionalVal(lmc,'addr2'), 
                            addr3=getOptionalVal(lmc,'addr3'), 
                            modTime=currtime)
         crMachine = LMComputeResource(name, lmc['ip'], lmc['contactid'], 
                                       createTime=currtime, modTime=currtime, 
                                       hbTime=currtime)
         compResource = scribe.registerComputeResource(crMachine, crContact)
         
   except Exception, e:
      raise LMError(currargs='Failed to insert LmCompute %s' % name)
         
   
# ...............................................
if __name__ == '__main__':  
   logger = DebugLogger()
   currtime = DT.gmt().mjd
   scribe = Scribe(logger)
   success = scribe.openConnections()
   if success:
      try:
         # Register everything in computeMetadata
         registerStandardComputes(scribe)
 
         # Register anything in configuration file (probably site.ini)
         registerConfiguredCompute(scribe)
      except Exception, e:
         raise LMError(currargs='Failed to insert LmCompute %s' % name)
      finally:
         scribe.closeConnections()
       
#    
