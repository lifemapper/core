"""
@summary Module that contains the RADExperiment class
@author Aimee Stewart
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
import mx.DateTime
import pickle
import os
from osgeo import ogr
import subprocess
from types import StringType

from LmCommon.common.lmconstants import MatrixType
from LmServer.base.lmobj import LMError, LMObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import LMFileType, LMServiceType, LMServiceModule
from LmServer.rad.matrix import Matrix                                  

# .............................................................................
class Gridset(ServiceObject):
   """
   The Gridset class contains all of the information for one view (extent and 
   resolution) of a RAD experiment.  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, name=None, metadata={}, 
                shapeGrid=None, shapeGridId=None, siteIndicesFilename=None, 
                configFilename=None, epsgcode=None, 
                pam=None, grim=None, biogeo=None, tree=None,
                userId=None, gridsetId=None, metadataUrl=None, modTime=None):
      """
      @summary Constructor for the Gridset class
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param gridsetId: dbId  for ServiceObject
      @param name: Short identifier for this gridset, not required to be unique.
      @param shapeGrid: Vector layer with polygons representing geographic sites.
      @param siteIndices: A filename containing a dictionary with keys the 
             unique/record identifiers and values the x, y coordinates of the 
             sites in a Matrix (if shapeGrid is not provided)
      @param epsgcode: The EPSG code of the spatial reference system of data.
      @param pam: A Presence Absence Matrix (MatrixType.PAM)
      @param grim: A Matrix of Environmental Values (MatrixType.GRIM)
      @param biogeo: A Matrix of Biogeographic Hypotheses (MatrixType.BIOGEO_HYPOTHESES)
      @param tree: A Tree with taxa matching those in the PAM 
      """
      if shapeGrid is not None:
         if userId is None:
            userId = shapeGrid.getUserId()
         if shapeGridId is None:
            shapeGridId = shapeGrid.getId()
         if siteIndicesFilename is None:
            siteIndicesFilename = shapeGrid.getSiteIndicesFilename()
         if epsgcode is None:
            epsgcode = shapeGrid.epsgcode
         elif epsgcode != shapeGrid.epsgcode:
            raise LMError('Gridset EPSG {} does not match Shapegrid EPSG {}'
                          .format(self._epsg, shapeGrid.epsgcode))

      ServiceObject.__init__(self, userId, gridsetId, LMServiceType.GRIDSETS, 
                             moduleType=LMServiceModule.LM, 
                             metadataUrl=metadataUrl, modTime=modTime)
      self.name = name
      self.grdMetadata = {}
      self.loadGrdMetadata(metadata)
      self._shapeGrid = shapeGrid
      self._shapeGridId = shapeGridId
      self._siteIndicesFilename = siteIndicesFilename
      self._siteIndices = None
      self.configFilename = configFilename
      self._setEPSG(epsgcode)
      # Optional Matrices
      self.setMatrix(MatrixType.PAM, mtxFileOrObj=pam)
      self.setMatrix(MatrixType.GRIM, mtxFileOrObj=grim)
      self.setMatrix(MatrixType.BIOGEO_HYPOTHESES, mtxFileOrObj=biogeo)
      
# ...............................................
   @classmethod
   def initFromFiles(cls):
      pass

# .............................................................................
# Properties
# .............................................................................
   def _setEPSG(self, epsg=None):
      if epsg is None:
         if self._shapeGrid is not None:
            epsg = self._shapeGrid.epsgcode
      self._epsg = epsg

   def _getEPSG(self):
      if self._epsg is None:
         self._setEPSG()
      return self._epsg

   epsgcode = property(_getEPSG, _setEPSG)
   
# .............................................................................
# Methods
# .............................................................................

# ...............................................
   def setId(self, expid):
      """
      Overrides ServiceObject.setId.  
      @note: ExperimentId should always be set before this is called.
      """
      ServiceObject.setId(self, expid)
      self.setPath()

# ...............................................
   def setPath(self):
      if self._path is None:
         if (self._userId is not None and 
             self.getId() and 
             self._getEPSG() is not None):
            self._path = self._earlJr.createDataPath(self._userId, 
                               LMFileType.UNSPECIFIED_RAD,
                               epsg=self._epsg, gridsetId=self.getId())
         else:
            raise LMError
         
   @property
   def path(self):
      if self._path is None:
         self.setPath()
      return self._path

# ...............................................
   def readIndices(self, indicesFilename=None):
      """
      @summary Fill the siteIndices from existing file
      """
      indices = None
      if indicesFilename is None:
         indicesFilename = self._siteIndicesFilename
      if isinstance(indicesFilename, StringType) and os.path.exists(indicesFilename):
         try:
            f = open(indicesFilename, 'r')
            indices = pickle.load(f)
         except:
            raise LMError('Failed to read indices {}'.format(indicesFilename))
         finally:
            f.close()
      self._siteIndices = indices
      

   def getSiteIndicesFilename(self):
      return self._siteIndicesFilename

# ...............................................
   def setMatrix(self, mtxType, mtxFileOrObj=None, doRead=False):
      """
      @summary Fill a Matrix object from Matrix or existing file
      """
      mtx = None
      if mtxFileOrObj is not None:
         if isinstance(mtxFileOrObj, StringType) and os.path.exists(mtxFileOrObj):
            mtx = Matrix(matrixType=mtxType, dlocation=mtxFileOrObj)
            if doRead:
               mtx.readData()            
         elif isinstance(mtxFileOrObj, Matrix):
            mtx = mtxFileOrObj
            
      if mtxType == MatrixType.PAM:
         self._pam = mtx
      elif mtxType == MatrixType.GRIM:
         self._grim = mtx
      elif mtxType == MatrixType.BIOGEO_HYPOTHESES:
         self._biogeo = mtx
                  
# ................................................
   def createLayerShapefileFromMatrix(self, shpfilename, isPresenceAbsence=True):
      """
      Only partially tested, field creation is not holding
      """
      if isPresenceAbsence:
         matrix = self.getFullPAM()
      else:
         matrix = self.getFullGRIM()
      if matrix is None or self._shapeGrid is None:
         return False
      else:
         self._shapeGrid.copyData(self._shapeGrid.getDLocation(), 
                                 targetDataLocation=shpfilename,
                                 format=self._shapeGrid.dataFormat)
         ogr.RegisterAll()
         drv = ogr.GetDriverByName(self._shapeGrid.dataFormat)
         try:
            shpDs = drv.Open(shpfilename, True)
         except Exception, e:
            raise LMError(['Invalid datasource %s' % shpfilename, str(e)])
         shpLyr = shpDs.GetLayer(0)

         mlyrCount = matrix.columnCount
         fldtype = matrix.ogrDataType
         # For each layer present, add a field/column to the shapefile
         for lyridx in range(mlyrCount):
            if (not self._layersPresent 
                or (self._layersPresent and self._layersPresent[lyridx])):
               # 8 character limit, must save fieldname
               fldname = 'lyr%s' % str(lyridx)
               fldDefn = ogr.FieldDefn(fldname, fldtype)
               if shpLyr.CreateField(fldDefn) != 0:
                  raise LMError('CreateField failed for %s in %s' 
                                % (fldname, shpfilename))             
         
#          # Debug only
#          featdef = shpLyr.GetLayerDefn()
#          featcount = shpLyr.GetFeatureCount()
#          for i in range(featdef.GetFieldCount()):
#             fld = featdef.GetFieldDefn(i)
#             print '%s  %d  %d' % (fld.name, fld.type, fld.precision)  
#          print  "done with diagnostic loop"
         # For each site/feature, fill with value from matrix
         currFeat = shpLyr.GetNextFeature()
         sitesKeys = sorted(self.getSitesPresent().keys())
         print "starting feature loop"         
         while currFeat is not None:
            #for lyridx in range(mlyrCount):
            for lyridx,exists in self._layersPresent.iteritems():
               if exists:
                  # add field to the layer
                  fldname = 'lyr%s' % str(lyridx)
                  siteidx = currFeat.GetFieldAsInteger(self._shapeGrid.siteId)
                  #sitesKeys = sorted(self.getSitesPresent().keys())
                  realsiteidx = sitesKeys.index(siteidx)
                  currval = matrix.getValue(realsiteidx,lyridx)
                  # debug
                  currFeat.SetField(fldname, currval)
            # add feature to the layer
            shpLyr.SetFeature(currFeat)
            currFeat.Destroy()
            currFeat = shpLyr.GetNextFeature()
         #print 'Last siteidx %d' % siteidx
   
         # Closes and flushes to disk
         shpDs.Destroy()
         print('Closed/wrote dataset %s' % shpfilename)
         success = True
         try:
            retcode = subprocess.call(["shptree", "%s" % shpfilename])
            if retcode != 0: 
               print 'Unable to create shapetree index on %s' % shpfilename
         except Exception, e:
            print 'Unable to create shapetree index on %s: %s' % (shpfilename, 
                                                                  str(e))
      return success
      

# .............................................................................
# Public methods
# .............................................................................
# ...............................................
   def dumpGrdMetadata(self):
      return super(Gridset, self)._dumpMetadata(self.grdMetadata)
 
# ...............................................
   def loadGrdMetadata(self, newMetadata):
      self.grdMetadata = super(Gridset, self)._loadMetadata(newMetadata)

# ...............................................
   def addGrdMetadata(self, newMetadataDict):
      self.grdMetadata = super(Gridset, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.grdMetadata)
            
# .............................................................................
# Read-0nly Properties
# .............................................................................
# ...............................................
# ...............................................
   @property
   def epsgcode(self):
      return self._epsg

   @property
   def shapeGridId(self):
      return self._shapeGridId
