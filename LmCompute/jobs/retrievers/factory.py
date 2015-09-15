"""
@summary: This module contains a function that will create job retrievers based 
             on the options specified in a configuration file 
@author: CJ Grady
@version: 1.0
@status: alpha

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
import os
from LmCompute.common.localconstants import JM_HOLD_DIRECTORY, JOB_RETRIEVERS
from LmCompute.jobs.retrievers.directoryJobRetriever import DirectoryRetriever
from LmCompute.jobs.retrievers.serverJobRetriever import ServerRetriever

# .............................................................................
def getJobRetrieversDictFromConfig():
   """
   @summary: Creates a dictionary of job retriever objects from the options in 
                a configuration object
   """
   retrievers = {}
   for retKey, retDict in JOB_RETRIEVERS.iteritems():
      if retDict["retrieverType"].lower() == "directory":
         retriever = DirectoryRetriever(retDict["jobDirectory"])
         retrievers[retKey] = retriever
      elif retDict["retrieverType"].lower() == "server":
         retriever = ServerRetriever(retDict["jobDirectory"], 
                                     retDict["jobServer"], 
                                     retDict["numToPull"], 
                                     retDict["threshold"], 
                                     **retDict["options"])
         retrievers[retKey] = retriever
      else:
         raise Exception, "Unknown job retriever type for %s: %s" % (retKey, 
                                                              retrieverType)
   return retrievers
