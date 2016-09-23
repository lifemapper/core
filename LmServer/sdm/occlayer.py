"""
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
import mx.DateTime
import os
from osgeo import ogr

from LmCommon.common.lmconstants import (DEFAULT_EPSG, GBIF_LINK_FIELD, 
                                         LM_NAMESPACE, DEFAULT_OGR_FORMAT)
from LmServer.common.localconstants import ARCHIVE_USER

from LmServer.base.layer import Vector, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject, ProcessObject
from LmServer.common.lmconstants import (DEFAULT_WMS_FORMAT, OccurrenceFieldNames,
                    ID_PLACEHOLDER, LMFileType, LMServiceType, LMServiceModule)

# .............................................................................
# .............................................................................
class OccurrenceType(_LayerParameters):
# .............................................................................
   """
   @todo: Update string formatting when python 2.5 is gone
   The point data class interfaces with the GBIF Cache and 
   creates a point data file that can be read by openModeller.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, displayName, fromGbif, queryCount, primaryEnv, touchTime, 
                modTime, userId, occurrenceSetId, sciName=None):
      """
      @summary Initialize the _Occurrences class instance
      @param displayName: Name to be displayed for this dataset
      @param fromGbif: True if the contents of this occurrence set were  
                   populated from the GBIF cache.  If so, updates to matching 
                   points in the GBIF cache will trigger recalculation of all 
                   experiments using this occurrenceset. 
      @param queryCount: (optional) Count reported by last update to shapefile.  
                         Used if there are no features/SpecimenPoints attached 
                         to this OccurrenceSet.
      @param touchTime: For GBIF datasets, last time this was checked against
                        the GBIF database. 
      @param userId: (optional) Id for the owner of these data
      @param occurrenceSetId: (optional) The occurrenceSet id for the database
      @param sciName: (optional) ScientificName object containing further 
             information about the name associated with these data
      @todo: Remove points, count, query
      """
      _LayerParameters.__init__(self, -1, modTime, userId, occurrenceSetId)
      self.displayName = displayName
      self.primaryEnv = primaryEnv
      self.fromGbif = fromGbif
      self.queryCount = queryCount
      self._touchTime = touchTime
      self._scientificName = sciName
      
# ...............................................
   def getScientificNameId(self):
      if self._scientificName is not None:
         return self._scientificName.getId()
      else:
         return None

# ...............................................
   def getScientificName(self):
      return self._scientificName

# .............................................................................
# .............................................................................

class OccurrenceLayer(OccurrenceType, Vector, ProcessObject):
# .............................................................................
   """
   @todo: Update string formatting when python 2.5 is gone
   The point data class interfaces with the GBIF Cache and 
   creates a point data file that can be read by openModeller.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, displayName, name=None, fromGbif=False, dlocation=None, 
                metalocation=None, queryCount=-1, epsgcode=DEFAULT_EPSG, 
                ogrType=ogr.wkbPoint, ogrFormat=DEFAULT_OGR_FORMAT, bbox=None,
                featureAttributes={}, features={}, primaryEnv=None, sciName=None,
                userId=ARCHIVE_USER, occId=None, metadataUrl=None,
                verify=None, squid=None,
                createTime=None, modTime=None, touchTime=0,
                rawDLocation=None, status=None, statusModTime=None):
      """
      @todo: calculate bbox from points upon population, update as appropriate
      @summary Initialize the OccurrenceSet class instance
      @param displayName: Name to be displayed for this dataset
      @todo: Add name as a required parameter, and returned value from db views
      @todo: Add mapunits, resolution?  metalocation?
      @param name: Layer name for uploaded occurrencesets.  User/Name 
                   combination must be unique
      @param query: Query to get species data from the gbifCache.  This is
                    can be a PointQuery object or an absolute shapefile location. 
      @param fromGbif: True if the contents of this occurrence set were  
                   populated from the GBIF cache.  If so, updates to matching 
                   points in the GBIF cache will trigger recalculation of all 
                   experiments using this occurrenceset. 
      @param metadataUrl: Lifemapper layer metadata Url
      @param queryCount: (optional) Count reported by last update to shapefile.  
                         Used if there are no features/SpecimenPoints attached 
                         to this OccurrenceSet.
      @param modTime: (optional) Last modification time of the object 
                        (metadata or points) in modified julian date format; 
                        0 if points have not yet been collected and saved
      @param touchTime: For GBIF datasets, last time this was checked against
                        the GBIF database. 
      @param bbox: (optional) a length 4 tuple of (minX, minY, maxX, maxY)
      @param userId: (optional) Id for the owner of these data
      @param occId: (optional) The occurrenceSet id for the database
      @todo: Remove points, count, query
      """
      OccurrenceType.__init__(self, displayName, fromGbif, queryCount, primaryEnv, 
                              touchTime, modTime, userId, occId, sciName)
      ProcessObject.__init__(self, objId=occId, parentId=None, 
                status=status, statusModTime=statusModTime)
      Vector.__init__(self, title=displayName, bbox=bbox, epsgcode=epsgcode, 
                      dlocation=dlocation, metalocation=metalocation,
                      ogrType=ogrType, ogrFormat=ogrFormat, 
                      featureAttributes=featureAttributes, features=features,
                      svcObjId= occId, lyrId=occId, lyrUserId=userId, 
                      verify=verify, squid=squid,
                      createTime=createTime, modTime=modTime, metadataUrl=metadataUrl,
                      serviceType=LMServiceType.OCCURRENCES, moduleType=LMServiceModule.SDM)
      self._rawDLocation = rawDLocation
      self._subsetDLocation = None
      self.setId(occId)
#       if self.fromGbif:
#          self.setValAttribute(GBIF_LINK_FIELD)
                   
# .............................................................................
# Class and Static methods
# .............................................................................

   @staticmethod
   def getUserPointFeatureAttributes():
      featureAttributes = {
                  0: (Vector._localIdFieldName, Vector._localIdFieldType),
                  1: (OccurrenceFieldNames.LONGITUDE[0], ogr.OFTReal),
                  2: (OccurrenceFieldNames.LATITUDE[0],  ogr.OFTReal),
                  3: (Vector._geomFieldName, Vector._geomFieldType)
                  }
      return featureAttributes
   
# ...............................................
   @staticmethod
   def getUserPointFeature(id, x, y):
      geomwkt = OccurrenceLayer.getPointWkt(x, y)
      vals = [id, x, y, geomwkt]
      return vals

# ...............................................
   @staticmethod
   def equalPoints(wkt1, wkt2):
      if wkt1 == wkt2:
         return True
      else:
         pt1 = OccurrenceLayer.getPointFromWkt(wkt1)
         pt2 = OccurrenceLayer.getPointFromWkt(wkt2)
         if abs(pt1[0] - pt2[0]) > 1e-6:
            return False
         elif abs(pt1[1] - pt2[1]) > 1e-6:
            return False
         else:
            return True
         
# ...............................................
   @staticmethod
   def getPointFromWkt(wkt):
      if wkt is None:
         raise LMError('Missing wkt')
      start = wkt.find('(')
      end = wkt.rfind(')')
      if (start != -1 and end != -1):
         x,y = wkt[start+1:end].split()
         try:
            x = float(x)
            y = float(y)
         except:
            raise LMError ('Invalid point WKT %s' % wkt)
         else:
            return (x, y)
      
         
   @staticmethod
   def getPointWkt(x, y):
      try:
         float(x)
         float(y)
      except:
         raise LMError(currargs=['Invalid point coordinates; x = %s, y = %s' 
                                 % (str(x), str(y))])
      else:
         return 'POINT ( %s %s )' % (str(x), str(y))
              
# .............................................................................
# Properties, getters, setters
# .............................................................................
   def _getCount(self):
      """
      @summary Returns the number of new-style points (generic vector feature 
               objects)
      @return The number of points for this dataset
      """
      return self._getFeatureCount()
         
   count = property(_getCount)
   
# ...............................................
   def getTouchTime(self):
      return self._touchTime
   
   def setTouchTime(self, touchtime):
      self._touchTime = touchtime
      
# ...............................................
   def getRawDLocation(self):
      return self._rawDLocation
   
   def setRawDLocation(self, rawDLocation, touchTime):
      self._touchTime = touchTime
      self._rawDLocation = rawDLocation
   
   # ...............................................
   def updateStatus(self, status, stage=None, modTime=None, queryCount=None,
                    statusModTime=None):
      """
      @note: Added to parallel other job object methods, stage is unused for now.
      @note: Overrides ProcessObject.updateStatus
      @todo: Rethink setting touchtime, queryCount, and parametersModTime here
      @todo: Remove statusModTime
      """
      ProcessObject.updateStatus(self, status, modTime=modTime)
      self._touchTime = self.statusModTime
      if queryCount is not None: 
         self.queryCount = queryCount
         self.parametersModTime = self.statusModTime
                  
# .............................................................................
# Superclass methods overridden
## .............................................................................
   def setId(self, id):
      """
      @summary: Sets the database id on the object, and sets the 
                OccurrenceSet._dlocation of the shapefile if it is None.
      @param id: The database id for the object
      @note: Also sets OccurrenceSet._dlocation, _Layer.mapPrefix, 
             and _Layer.name.  ServiceObject.metadataUrl is constructed using
             the id on first access.
      """
      ServiceObject.setId(self, id)
      if id is not None:
         if self.name is None:
            self.name = self._earlJr.createLayername(occsetId=self.getId())
         self.setDLocation()
         self.setLocalMapFilename()
         self._setMapPrefix()
         self.resetMetadataUrl()

# ...............................................
   def getAbsolutePath(self):
      self.setDLocation()
      return Vector.getAbsolutePath(self)

# ...............................................
   @property
   def makeflowFilename(self):
      dloc = self.createLocalDLocation(makeflow=True)
      return dloc

# ...............................................
   def createLocalDLocation(self, raw=False, subset=False, makeflow=False):
      """
      @summary: Create filename for this layer.
      @param raw: If true, this indicates a raw dump of occurrences (CSV for
                    GBIF dump or User file, a URL for a BISON or iDigBio query).
      @param subset: If true, this indicates a subset of occurrences, limiting
                     the number of points to ease map display and model 
                     computation.  
      @param makeflow: If true, this indicates a makeflow document of jobs 
                       related to this object
      """
      dloc = None
      if self.getId() is not None:
         if raw:
            ftype = LMFileType.OCCURRENCE_RAW_FILE
         elif makeflow:
            ftype = LMFileType.SDM_MAKEFLOW_FILE
         else:
            ftype = LMFileType.OCCURRENCE_FILE
         dloc = self._earlJr.createFilename(ftype, occsetId=self.getId(), 
                   subset=subset, usr=self._userId)
      return dloc
   
# ...............................................
   # Overrides layer.getDLocation, allowing optional keyword 
   def getDLocation(self, subset=False):
      if subset:
         if self._subsetDLocation is None:
            self._subsetDLocation = self.createLocalDLocation(subset=True)
         return self._subsetDLocation
      else:
         self.setDLocation()
      return self._dlocation

   def setDLocation(self, dlocation=None):
      """
      @summary: Sets the OccurrenceSet._dlocation of the shapefile.  If it is 
                None and the _dbid is present, it calculates and assigns 
                the _dlocation.
      """
      if self._dlocation is None:
         if dlocation is None:
            dlocation = self.createLocalDLocation()
            
         Vector.setDLocation(self, dlocation)

# ...............................................
   def isValidDataset(self, subset=False):
      """
      @summary: Check to see if the dataset at self.dlocations is a valid 
                occurrenceset readable by OGR.  If dlocation is None, fill
                it in first.
      @return: True if dlocation is a valid occurrenceset; False if not
      """
      dlocation = self.getDLocation(subset=subset)
#       self.setDLocation()
      valid = Vector.isValidDataset(self, dlocation=dlocation)
      return valid

# ...............................................
# ...............................................
   def _createMapPrefix(self):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
             map=<mapname>, and layers=<layername> key/value pairs.  
      @note: If the object has not yet been inserted into the database, a 
             placeholder is used until replacement after database insertion.
      """
      occid = self.getId()
      if occid is None:
         occid = ID_PLACEHOLDER
      mapprefix = self._earlJr.constructMapPrefix(ftype=LMFileType.SDM_MAP, 
                     mapname=self.mapName, occsetId=occid, usr=self._userId)
      return mapprefix
   
   def _setMapPrefix(self):
      mapprefix = self._createMapPrefix()
      self._mapPrefix = mapprefix
          
   @property
   def mapPrefix(self): 
      return self._mapPrefix
   
# ...............................................
   @property
   def mapLayername(self):
      lyrname = None
      if self._dbId is not None:
         lyrname = self._earlJr.createLayername(occsetId=self._dbId)
      return lyrname 

# ...............................................
   def createLocalMapFilename(self):
      """
      @summary: Find mapfile containing this layer.  
      """
      mapfilename = self._earlJr.createFilename(LMFileType.SDM_MAP, 
                                    usr=self._userId, occsetId=self.getId())
      return mapfilename     
   
# ...............................................
   def setLocalMapFilename(self, mapfname=None):
      """
      @note: Overrides existing _mapFilename
      @summary: Find mapfile containing layers for this model's occurrenceSet.
      @param mapfname: Previously constructed mapfilename
      """
      if mapfname is None and self.getId() is not None:
         mapfname = self.createLocalMapFilename()
      self._mapFilename = mapfname

# ...............................................
   @property
   def mapFilename(self):
      self.setLocalMapFilename()
      return self._mapFilename

# ...............................................
   @property
   def mapName(self):
      mapname = None
      if self._mapFilename is None:
         self.setLocalMapFilename()      
      pth, fname = os.path.split(self._mapFilename)
      mapname, ext = os.path.splitext(fname)         
      return mapname

# ...............................................
   @property
   def layerName(self):
      return self._earlJr.createLayername(occsetId=self.getId())
   
# ...............................................
   def clearLocalMapfile(self):
      """
      @summary: Delete the mapfile containing this layer
      """
      if self._mapFilename is None:
         self.setLocalMapFilename()
      self.deleteLocalMapfile()
      self._mapFilename = None
 
# ...............................................
   def deleteLocalMapfile(self):
      """
      @summary: Delete the mapfile containing this layer
      """
      success, msg = self._deleteFile(self._mapFilename, deleteDir=True)
# .............................................................................
# Public methods
# .............................................................................
   def getGBIFKeys(self):
      keys = []
      if self.fromGbif:
         return self.features.keys()
      
# ...............................................
   def getWMSRequest(self, width, height, bbox, color=None, format=DEFAULT_WMS_FORMAT):
      """
      @summary Return a GET query for the Lifemapper WMS GetMap request
      @param color: (optional) color in hex format RRGGBB or predefined palette 
             name. Color is applied only to Occurrences or Projection. Valid 
             palette names: 'gray', 'red', 'green', 'blue', 'safe', 'pretty', 
             'bluered', 'bluegreen', 'greenred'. 
      @param format: (optional) image file format, default is 'image/png'
      """
      wmsUrl = self._earlJr.constructLMMapRequest(self.mapPrefix, width, height, bbox, 
                                                color, self.SRS, format)
      return wmsUrl   
   
## ...............................................         
   def getFeaturesIdLongLat(self):
      """
      @summary: Returns a list of feature/point tuples - (FID, x, y)
      """
      microVals = []
      if self._localIdIdx is None:
         self.getLocalIdIndex()
            
      geomIdx = self.getFieldIndex(self._geomFieldName)
      for featureFID in self._features.keys():
         fid = self.getFeatureValByFieldIndex(self._localIdIdx, featureFID)
         wkt = self.getFeatureValByFieldIndex(geomIdx, featureFID)
         x, y = self.getPointFromWkt(wkt)
         # returns values id, longitude(x), latitude(y)
         microVals.append((fid, x, y))
      return microVals
         
# ..............................................................................
   def getWkt(self):
      wkt = ''
      if self._features and self._featureAttributes:
         pttxtlst = []
         self._setGeometryIndex()
         for pt in self._features.values():
            wkt = pt[self._geomIdx]
            pttxtlst.append(wkt.strip('POINT'))
         multipointstr = ', '.join(pttxtlst)
         wkt = 'MULTIPOINT( %s )' % (multipointstr)
      return wkt

# ...............................................
   def readShapefile(self, subset=False, dlocation=None):
      """
      @note: calls Vector.readData to create points from features. This
             will be removed when we switch to only using features
      @param subset: Indicates if the subset should be retrieved
      @param dlocation: Overrides the object's dlocation (possibly for 
                           temporary file)
      """
      self.clearFeatures()
      if dlocation is None:
         dlocation = self.getDLocation(subset=subset)
      Vector.readData(self, dlocation=dlocation)

# ...............................................
   def readXMLPoints(self, data):
      from LmCommon.common.lmXml import fromstring
      from LmServer.base.utilities import getXmlValueFromTree
      
      self.clearFeatures()
      tree = fromstring(data)
      occSetBranch = tree.find("{%s}occurrenceSet" % LM_NAMESPACE)
      if self.displayName is None:
         self.displayName = getXmlValueFromTree(occSetBranch, 
                                                     'displayName')
         if self.displayName is None:
            raise LMError('Display name not specified')
      
      allPoints = occSetBranch.find("{%s}points" % LM_NAMESPACE).findall(
                                              "{%s}point" % LM_NAMESPACE)
      featAttrs = self.getUserPointFeatureAttributes()
      feats = {}
      idname = None
      xname = 'longitude'
      yname = 'latitude'
      for pt in allPoints:
         if idname is None:
            for name in OccurrenceFieldNames.LOCAL_ID:
               val = getXmlValueFromTree(pt, name)
               if val is not None:
                  idname = name
                  break
                           
         id = getXmlValueFromTree(pt, idname)
         x = getXmlValueFromTree(pt, xname)
         y = getXmlValueFromTree(pt, yname)
         feats[id] = self.getUserPointFeature(id, x, y)

      self.setFeatures(feats, featAttrs)
      
      
# ...............................................
   def readCSVPoints(self, data, featureLimit=None):
      """
      @note: We are saving only latitude, longitude and localid if it exists.  
             If localid does not exist, we create one.
      @todo: Save the rest of the fields using Vector.splitCSVPointsToShapefiles
      @todo: remove featureLimit, read subsetDLocation if there is a limit 
      """
      import csv
      minX = minY = maxX = maxY = None
      localid = None
      
      self.clearFeatures()
      infile = open(self._dlocation, 'rU')
      reader = csv.reader(infile)
      rowone = reader.next()
      
      ((idName, idPos), (xName, xPos), (yName, yPos)) = Vector._getIdXYNamePos(rowone)
          
      if not idPos:
         # If no id column, create it
         if (xPos and yPos):
            localid = 0
         # If no headers, assume columns 1 = id, 2 = longitude, 3 = latitude
         else:
            idPos = 0
            xPos = 1
            yPos = 2

      if xPos is None or yPos is None:
         raise LMError('Must supply longitude and latitude')
      
      featAttrs = self.getUserPointFeatureAttributes()
      feats = {}
      Xs = []
      Ys = []
      for row in reader:
         try:
            if localid is None:
               thisid = row[idPos]
            else:
               localid += 1
               thisid = localid
            x = float(row[xPos])
            y = float(row[yPos])
            Xs.append(x)
            Ys.append(y)
            feats[thisid] = self.getUserPointFeature(thisid, x, y)
            if featureLimit is not None and len(feats) >= featureLimit:
               break
         except Exception, e:
            # Skip point if fails.  This could be a blank row or something
            pass
      
      if len(feats) == 0:
         raise LMError('Unable to read points from CSV') 
      
      try:
         minX = min(Xs)
         minY = min(Ys)
         maxX = max(Xs)
         maxY = max(Ys)
      except Exception, e:
         raise LMError('Failed to get valid coordinates (%s)' % str(e))
         
      infile.close()
      self.setFeatures(feats, featAttrs)
      return (minX, minY, maxX, maxY)
