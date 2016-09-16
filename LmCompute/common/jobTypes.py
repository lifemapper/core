"""
@summary: Module that determines what job types are available in plugins
@author: CJ Grady
@version: 3.0.0
@status: beta

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
from LmCompute.common.localconstants import PLUGINS_PATH
# ============================================================================
# =                                 Job Types                                =
# ============================================================================
def getJobTypes():
   """
   @summary: Uses introspection to determine what job types are available to 
                this system
   """
   import os
   pluginsNS = 'LmCompute.plugins'
   jobTypes = {}
   jobImports = []
   for f in os.listdir(PLUGINS_PATH):
      try:
         m = __import__('{pluginsNS}.{plugin}'.format(pluginsNS=pluginsNS, 
                                             plugin=f), fromlist='jobTypes')
         jobImports.extend(m.jobTypes)
      except Exception, e: # Skip if not a valid plugin
         #print str(e) # Uncomment if no plugins are showing up
         pass
   for jobTypeId, namespace, name in jobImports:
      try:
         jt = __import__('{pluginsNS}.{ns}'.format(pluginsNS=pluginsNS, 
                                                  ns=namespace), fromlist=name)
         jobTypes[jobTypeId] = {
                             'id': jobTypeId, 
                             'name': name, 
                             'constructor': jt.__getattribute__(name)
                            }
      except Exception, e:
         print("Could not import: (%s, %s, %s) -- %s" % (jobTypeId, namespace, 
                                                                 name, str(e)))
   return jobTypes
# ----------------------------------------------------------------------------

JOB_TYPES = getJobTypes()
