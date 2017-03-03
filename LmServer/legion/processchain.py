"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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

from LmCommon.common.lmconstants import LMFormat
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import LMFileType
from LmServer.makeflow.cmd import MfRule
# .........................................................................
class MFChain(ProcessObject):
# .............................................................................
   META_CREATED_BY = 'createdBy'
   META_DESC = 'description'
# .............................................................................
   def __init__(self, userId, dlocation=None, priority=None, metadata=None,  
                status=None, statusModTime=None, headers=None, mfChainId=None):
      """
      @summary Class used to generate a Makeflow document with Lifemapper 
               computational jobs
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param userId: Id for the owner of this process
      @param dlocation: location for Makeflow file
      @param priority: relative priority for jobs contained within
      @param metadata: Dictionary of metadata key/values; uses class or 
                       superclass attribute constants META_* as keys
      @param headers: Optional list of (header, value) tuples
      @param mfChainId: Database unique identifier
      """
      self.jobs = []
      self.targets = []
      self.headers = []
      if headers is not None:
         self.addHeaders(headers)
      self._dlocation = dlocation
      self._userId = userId
      self.priority = priority
      self.mfMetadata = {}
      self.loadMfMetadata(metadata)
      ProcessObject.__init__(self, objId=mfChainId, processType=None, parentId=None,
                             status=status, statusModTime=statusModTime)
      
# ...............................................
   def dumpMfMetadata(self):
      return super(MFChain, self)._dumpMetadata(self.mfMetadata)
 
# ...............................................
   def loadMfMetadata(self, newMetadata):
      self.mfMetadata = super(MFChain, self)._loadMetadata(newMetadata)

# ...............................................
   def addMfMetadata(self, newMetadataDict):
      self.mfMetadata = super(MFChain, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.mtxColMetadata)

# ...............................................
   def setId(self, mfid):
      """
      @summary: Sets the database id on the object, and sets the 
                dlocation of the file if it is None.
      @param mfid: The database id for the object
      """
      self.objId = mfid
      self.setDLocation()

# ...............................................
   def getId(self):
      """
      @summary Returns the database id from the object table
      @return integer database id of the object
      """
      return self.objId
   
# .............................................................................
# Superclass methods overridden
## .............................................................................
# ...............................................
   def createLocalDLocation(self):
      """
      @summary: Create data location
      """
      dloc = None
      if self.objId is not None:
         earlJr = EarlJr()
         dloc = earlJr.createFilename(LMFileType.MF_DOCUMENT, 
                                            objCode=self.objId, 
                                            usr=self._userId)
      return dloc

# ...............................................
   def getDLocation(self):
      self.setDLocation()
      return self._dlocation

   def setDLocation(self, dlocation=None):
      """
      @note: Does NOT override existing dlocation, use clearDLocation for that
      """
      if self._dlocation is None:
         if dlocation is None:
            dlocation = self.createLocalDLocation()
         self._dlocation = dlocation

   def clearDLocation(self): 
      self._dlocation = None

# ...............................................
   def getArfFilename(self, prefix='mf'):
      """
      @summary: Return temporary dummy filename written to indicate completion  
                of this MFChain.
      """
      earlJr = EarlJr()
      pth = earlJr.createDataPath(self._userId, LMFileType.MF_DOCUMENT) 
      fname = os.path.join(pth, '{}_{}.arf'.format(prefix, self.objId))
      return fname

# ...............................................
   def getTriageFilename(self, prefix='mf'):
      """
      @summary: Return filename to contain list of temporary dummy (Arf) files.
                This file is used as input for triage to jettison failures
                from inputs to another MF.
      """
      earlJr = EarlJr()
      pth = earlJr.createDataPath(self._userId, LMFileType.MF_DOCUMENT) 
      fname = os.path.join(pth, '{}_{}{}'.format
                           (prefix, self.objId, LMFormat.TXT.ext))
      return fname

# ...............................................
   def getUserId(self):
      """
      @summary Gets the User id
      @return The User id
      """
      return self._userId

   def setUserId(self, usr):
      """
      @summary: Sets the user id on the object
      @param usr: The user id for the object
      """
      self._userId = usr

   # ...........................
   def _addJobCommand(self, outputs, cmd, dependencies=[], comment=''):
      """
      @summary: Adds a job command to the document
      @param outputs: A list of output files created by this job
      @param cmd: The command to execute
      @param dependencies: A list of dependencies (files that must exist before 
                              this job can run
      """
      job = "# {comment}\n{outputs}: {dependencies}\n   {cmd}\n".format(
         outputs=' '.join(outputs), 
         cmd=cmd, comment=comment, 
         dependencies=' '.join(dependencies))
      self.jobs.append(job)
      # Add the new targets to self.targets
      self.targets.extend(outputs)
   
   # ...........................
   def addCommands(self, ruleList):
      """
      @summary: Adds a list of commands to the Makeflow document
      @param ruleList: A list of MfRule objects
      """
      # Check if this is just a single tuple, if so, make it a list
      if isinstance(ruleList, MfRule):
         ruleList = [ruleList]
         
      # For each tuple in the list
      for rule in ruleList:
         deps = rule.dependencies
         targets = rule.targets
         cmd = rule.command
         comment = rule.comment

         # Check to see if these targets are already defined by creating a new
         #    list of targets that are not in self.targets
         newTargets = [t for t in targets if t not in self.targets]
         
         # If there are targets that have not been defined before
         if len(newTargets) > 0:
            self._addJobCommand(newTargets, cmd, dependencies=deps, 
                                comment=comment)
   
   # ...........................
   def addHeaders(self, headers):
      """
      @summary: Adds headers to the document
      @param headers: A list of (header, value) tuples
      """
      if isinstance(headers, tuple):
         headers = [headers]
      self.headers.extend(headers)
   
   # ...........................
   def write(self, filename=None):
      """
      @summary: Write the document to the specified location
      @param filename: The file location to write this document
      @raise ValueError: If no jobs exist to be computed (list is right type, 
                            empty is bad value)
      @note: May fail with IOError if there is a problem writing to a location
      """
      if not self.jobs:
         raise ValueError("No jobs to be computed, fail for empty document")
      if filename is None:
         filename = self.getDLocation()
      with open(filename, 'w') as outF:
         for header, value in self.headers:
            outF.write("{header}={value}\n".format(header=header, value=value))
         for job in self.jobs:
            # These have built-in newlines
            outF.write(job) 
      
