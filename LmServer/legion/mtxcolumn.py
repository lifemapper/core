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
import json
import os

from LmCommon.common.lmconstants import ProcessType, JobStatus
from LmServer.base.layer2 import _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.makeflow.cmd import MfRule

# .............................................................................
# .............................................................................
# .............................................................................
class MatrixColumn(_LayerParameters, ProcessObject):
   # Query to filter layer for intersect
   INTERSECT_PARAM_FILTER_STRING = 'filterString'
   # Attribute used in layer intersect
   INTERSECT_PARAM_VAL_NAME = 'valName'
   # Units of measurement for attribute used in layer intersect
   INTERSECT_PARAM_VAL_UNITS = 'valUnits'
   # Minimum spatial coverage for gridcell intersect computation
   INTERSECT_PARAM_MIN_PERCENT = 'minPercent'
   # Types of GRIM gridcell intersect computation
   INTERSECT_PARAM_WEIGHTED_MEAN = 'weightedMean'
   INTERSECT_PARAM_LARGEST_CLASS = 'largestClass'
   # Minimum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MIN_PRESENCE = 'minPresence'
   # Maximum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MAX_PRESENCE = 'maxPresence'
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, matrixId, userId, 
                # inputs if this is connected to a layer and shapegrid 
                layer=None, shapegrid=None, intersectParams={}, 
                colDLocation=None, squid=None, ident=None,
                processType=None, metadata={}, matrixColumnId=None, 
                status=None, statusModTime=None):
      """
      @summary MatrixColumn constructor
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param matrixIndex: index for column within a matrix.  For the Global 
             PAM, assembled dynamically, this will be None.
      @param matrixId: 
      @param layer: layer input to intersect
      @param shapegrid: grid input to intersect 
      @param intersectParams: parameters input to intersect
      @param colDLocation: location of MatrixColumn (vector) 
      @param squid: species unique identifier for column
      @param ident: (non-species) unique identifier for column
      """
      _LayerParameters.__init__(self, userId, paramId=matrixColumnId, 
                                matrixIndex=matrixIndex, metadata=metadata, 
                                modTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                             parentId=matrixId, status=status, 
                             statusModTime=statusModTime)
      self.layer = layer
      self.shapegrid = shapegrid
      self.intersectParams = {}
      self.loadIntersectParams(intersectParams)
      self._colDLocation = None
      self.setColumnDLocation(colDLocation, statusModTime)
      self.squid = squid
      self.ident = ident

# ...............................................
   def dumpIntersectParams(self):
      return super(MatrixColumn, self)._dumpMetadata(self.intersectParams)
 
# ...............................................
   def loadIntersectParams(self, newIntersectParams):
      self.intersectParams = super(MatrixColumn, self)._loadMetadata(newIntersectParams)

# ...............................................
   def addIntersectParams(self, newIntersectParams):
      self.intersectParams = super(MatrixColumn, self)._addMetadata(newIntersectParams, 
                                  existingMetadataDict=self.intersectParams)
   
# ...............................................
   def getColumnDLocation(self):
      return self._colDLocation
   
   def setColumnDLocation(self, colDLocation, modTime):
      self._colDLocation = colDLocation
      self.paramModTime = modTime
   
   # ...............................................
   def updateStatus(self, status, matrixIndex=None, metadata=None, modTime=None):
      """
      @summary Update status, matrixIndex, metadata, modTime attributes on the 
               Matrix layer. 
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
      @copydoc LmServer.base.layer2._LayerParameters::updateParams()
      """
      ProcessObject.updateStatus(self, status, modTime=modTime)
      _LayerParameters.updateParams(self, matrixIndex=matrixIndex, 
                                    metadata=metadata, modTime=modTime)

# ...............................................
   def computeMe(self):
      """
      @summary: Creates a command to intersect a layer and a shapegrid to 
                produce a MatrixColumn.
      """
      rules = []
      # Layer object may be an SDMProject
      if self.layer is not None:
         # Layer input
         dependentFiles = [self.layer.getDLocation()]
         try:
            status = self.layer.status
         except:
            status = JobStatus.COMPLETE
         if JobStatus.waiting(status):
            lyrRules = self.layer.computeMe()
            rules.extend(lyrRules)
            
         # Shapegrid input
         dependentFiles.append(self.shapegrid.getDLocation())
         if JobStatus.waiting(self.shapegrid.status):
            shpgrdRules = self.layer.computeMe()
            rules.extend(shpgrdRules)
       
         options = ''
         if self.squid is not None:
            options = "--squid={0}".format(self.squid)
         elif self.ident is not None:
            options = "--ident={0}".format(self.ident)
         pavFname = self.getColumnDLocation()
         
         cmdArguments = [os.getenv('PYTHON'), 
                         ProcessType.getJobRunner(self.processType), 
                         self.shapegrid.getDLocation(), 
                         self.getDLocation(),
                         pavFname,
                         self.resolution,
                         self.intersectParams[self.INTERSECT_PARAM_MIN_PRESENCE],
                         self.intersectParams[self.INTERSECT_PARAM_MAX_PRESENCE],
                         self.intersectParams[self.INTERSECT_PARAM_MIN_PERCENT],
                         options ]

      cmd = ' '.join(cmdArguments)
      rules.append(MfRule(cmd, [pavFname], dependencies=[dependentFiles]))
        
      return rules


