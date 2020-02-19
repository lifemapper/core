"""Module containing classes and functions for occurrence sets
"""
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.time import gmt
from LmServer.base.layer2 import Vector, _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.common.lmconstants import (ID_PLACEHOLDER, LMFileType,
                                         LMServiceType, OccurrenceFieldNames)
from osgeo import ogr


# .............................................................................
# .............................................................................
class OccurrenceType(_LayerParameters, ProcessObject):
# .............................................................................
    """
    @todo: Update string formatting when python 2.5 is gone
    The point data class interfaces with the GBIF Cache and 
    creates a point data file that can be read by openModeller.
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, displayName, queryCount, mod_time, userId,
                     occurrenceSetId, metadata={}, sciName=None,
                     rawDLocation=None, processType=None,
                     status=None, statusModTime=None):
        """
        @summary Initialize the _Occurrences class instance
        @copydoc LmServer.base.layer._LayerParameters::__init__()
        @copydoc LmServer.base.service_object.ProcessObject::__init__()
        @param displayName: Name to be displayed for this dataset
        @param queryCount: Count reported by last update to shapefile.  
                                 Used if there are no features attached to this
                                 OccurrenceSet.
        @param occurrenceSetId: The occurrenceSet id for the database
        @param sciName: ScientificName object containing further 
                 information about the name associated with these data
        @param rawDLocation: URL or file location of raw data to be processed
        """
        _LayerParameters.__init__(self, userId, paramId=occurrenceSetId,
                                          matrixIndex=-1, metadata=metadata,
                                          mod_time=mod_time)
        ProcessObject.__init__(
            self, objId=occurrenceSetId, processType=processType,
            status=status, statusModTime=statusModTime)
        self.displayName = displayName
        self.queryCount = queryCount
        self._rawDLocation = rawDLocation
        self._bigDLocation = None
        self._scientificName = sciName

# ...............................................
    def getScientificNameId(self):
        if self._scientificName is not None:
            return self._scientificName.get_id()
        else:
            return None

# ...............................................
    def getScientificName(self):
        return self._scientificName

# ...............................................
    def setScientificName(self, sciName):
        self._scientificName = sciName

# ...............................................
    def getRawDLocation(self):
        return self._rawDLocation

    def setRawDLocation(self, rawDLocation, mod_time):
        self._rawDLocation = rawDLocation
        self.paramModTime = mod_time

    # ...............................................
    def updateStatus(self, status, mod_time=gmt().mjd,
                     queryCount=None):
        """
        @note: Overrides ProcessObject.updateStatus
        """
        ProcessObject.updateStatus(self, status, mod_time)
        if queryCount is not None:
            self.queryCount = queryCount
            self.paramModTime = self.statusModTime

# .............................................................................
# .............................................................................


class OccurrenceLayer(OccurrenceType, Vector):

# .............................................................................
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, displayName, userId, epsgcode, queryCount, lyrId=None,
                 squid=None, verify=None, dlocation=None, rawDLocation=None,
                 rawMetaDLocation=None, lyrMetadata={},
                 dataFormat=LMFormat.SHAPE.driver, valUnits=None,
                 valAttribute=None, nodataVal=None, minVal=None, maxVal=None,
                 mapunits=None, resolution=None, bbox=None,
                 occurrenceSetId=None, serviceType=LMServiceType.OCCURRENCES,
                 metadataUrl=None, parentMetadataUrl=None, featureCount=0,
                 featureAttributes={}, features={}, fidAttribute=None,
                 occMetadata={}, sciName=None, objId=None, processType=None,
                 status=None, statusModTime=None):
        """
        @todo: calculate bbox from points upon population, update as appropriate
        @summary Initialize the OccurrenceSet class instance
        @copydoc LmServer.base.layer.Vector::__init__()
        @copydoc LmServer.legion.occlayer.OccurrenceType::__init__()
        @todo: Remove count?
        @note: Vector.name is constructed in OccurrenceLayer.setId()
        """
        OccurrenceType.__init__(
            self, displayName, queryCount, statusModTime, userId,
            occurrenceSetId, metadata=occMetadata, sciName=sciName,
            rawDLocation=rawDLocation, processType=processType, status=status,
            statusModTime=statusModTime)
        Vector.__init__(
            self, None, userId, epsgcode, lyrId=occurrenceSetId, squid=squid,
            verify=verify, dlocation=dlocation, metadata=lyrMetadata,
            dataFormat=dataFormat, ogrType=ogr.wkbPoint, valUnits=valUnits,
            valAttribute=valAttribute, nodataVal=nodataVal, minVal=minVal,
            maxVal=maxVal, mapunits=mapunits, resolution=resolution, bbox=bbox,
            svcObjId=occurrenceSetId, serviceType=serviceType,
            metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl,
            mod_time=statusModTime, featureCount=featureCount,
            featureAttributes=featureAttributes, features=features,
            fidAttribute=fidAttribute)
        self.rawMetaDLocation = rawMetaDLocation
        self.setId(occurrenceSetId)

# .............................................................................
# Class and Static methods
# .............................................................................

# ...............................................
    @staticmethod
    def getUserPointFeatureAttributes():
        featureAttributes = {
            0 : (Vector._localIdFieldName, Vector._localIdFieldType),
            1 : (OccurrenceFieldNames.LONGITUDE[0], ogr.OFTReal),
            2 : (OccurrenceFieldNames.LATITUDE[0], ogr.OFTReal),
            3 : (Vector._geomFieldName, Vector._geomFieldType)
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
            x, y = wkt[start + 1:end].split()
            try:
                x = float(x)
                y = float(y)
            except:
                raise LMError ('Invalid point WKT {}'.format(wkt))
            else:
                return (x, y)

    @staticmethod
    def getPointWkt(x, y):
        """
        @summary: Creates a well-known-text string representing the point
        @note: Rounds the float to 4 decimal points 
        """
        try:
            float(x)
            float(y)
        except:
            raise LMError('Invalid point coordinates; x = {}, y = {}'
                                            .format(x, y))
        else:
            x = round(x, 4)
            y = round(y, 4)
            return 'POINT ( {} {} )'.format(x, y)

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

# .............................................................................
# Superclass methods overridden
# # .............................................................................
    def setId(self, occid):
        """
        @summary: Sets the database id on the object, and sets the 
                     OccurrenceSet._dlocation of the shapefile if it is None.
        @param occid: The database id for the object
        @note: Also sets OccurrenceSet._dlocation, _Layer.mapPrefix, and
            Vector.name.  ServiceObject.metadataUrl is constructed using the id
            on first access.
        """
        super(OccurrenceLayer, self).setId(occid)
        if occid is not None:
            if self.name is None:
                self.name = self._earlJr.createLayername(occsetId=self.get_id())
            self.set_dlocation()
            self.resetMetadataUrl()
            self.setLocalMapFilename()
            self._setMapPrefix()

# ...............................................
    def getAbsolutePath(self):
        self.set_dlocation()
        return Vector.getAbsolutePath(self)

# ...............................................
    @property
    def makeflowFilename(self):
        dloc = self.create_local_dlocation(makeflow=True)
        return dloc

# ...............................................
    def create_local_dlocation(self, raw=False, largeFile=False, makeflow=False):
        """
        @summary: Create filename for this layer.
        @param raw: If true, this indicates a raw dump of occurrences (CSV for
            GBIF dump or User file, a URL for a BISON or iDigBio query).
        @param largeFile: If true, this indicates a too-big file of occurrences
        @param makeflow: If true, this indicates a makeflow document of jobs
            related to this object
        """
        dloc = None
        if self.get_id() is not None:
            if raw:
                ftype = LMFileType.OCCURRENCE_RAW_FILE
            elif makeflow:
                ftype = LMFileType.MF_DOCUMENT
            elif largeFile:
                ftype = LMFileType.OCCURRENCE_LARGE_FILE
            else:
                ftype = LMFileType.OCCURRENCE_FILE
            occid = self.get_id()
            dloc = self._earlJr.createFilename(
                ftype, occsetId=occid, objCode=occid, usr=self._userId)
        return dloc

# ...............................................
    # Overrides layer.get_dlocation, allowing optional keyword
    def get_dlocation(self, largeFile=False):
        if largeFile:
            if self._bigDLocation is None:
                self._bigDLocation = self.create_local_dlocation(
                    largeFile=largeFile)
            return self._bigDLocation
        else:
            self.set_dlocation()
        return self._dlocation

# ...............................................
    def isValidDataset(self, largeFile=False):
        """
        @summary: Check to see if the dataset at self.dlocations is a valid 
            occurrenceset readable by OGR.  If dlocation is None, fill it in
            first.
        @return: True if dlocation is a valid occurrenceset; False if not
        """
        dlocation = self.get_dlocation(largeFile=largeFile)
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
        occid = self.get_id()
        if occid is None:
            occid = ID_PLACEHOLDER
        lyrname = self._earlJr.createBasename(
            LMFileType.OCCURRENCE_FILE, objCode=occid, usr=self._userId,
            epsg=self.epsgcode)
        mapprefix = self._earlJr.constructMapPrefixNew(
            urlprefix=self.metadataUrl, ftype=LMFileType.SDM_MAP,
            mapname=self.mapName, lyrname=lyrname, usr=self._userId)
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
        occid = self.get_id()
        mapfilename = self._earlJr.createFilename(
            LMFileType.SDM_MAP, occsetId=occid, objCode=occid, usr=self._userId)
        return mapfilename

# ...............................................
    def setLocalMapFilename(self, mapfname=None):
        """
        @note: Overrides existing _mapFilename
        @summary: Find mapfile containing layers for this model's occurrenceSet.
        @param mapfname: Previously constructed mapfilename
        """
        if self._mapFilename is None:
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
        if self._mapFilename is None:
            self.setLocalMapFilename()
        pth, fname = os.path.split(self._mapFilename)
        mapname, ext = os.path.splitext(fname)
        return mapname

# ...............................................
    @property
    def layerName(self):
        return self._earlJr.createLayername(occsetId=self.get_id())

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
    def clearOutputFiles(self):
        self.delete_data()
        self.clearDLocation()

# ...............................................
    def deleteLocalMapfile(self):
        """
        @summary: Delete the mapfile containing this layer
        """
        success, msg = self.deleteFile(self._mapFilename, deleteDir=True)
# .............................................................................
# Public methods
# .............................................................................

# ...............................................
    def copyForUser(self, userId):
        newOcc = OccurrenceLayer(
            self.displayName, userId, self.epsgcode, self.queryCount,
            squid=self.squid, verify=self.verify, valUnits=self.valUnits,
            valAttribute=self.getValAttribute(), nodataVal=self.nodataVal,
            minVal=self.minVal, maxVal=self.maxVal, mapunits=self.mapUnits,
            resolution=self.resolution, bbox=self.bbox,
            occMetadata=self.paramMetadata, sciName=self._scientificName,
            status=self.status, statusModTime=self.statusModTime)
        return newOcc

# # ...............................................
    def getFeaturesIdLongLat(self):
        """
        @summary: Returns a list of feature/point tuples - (FID, x, y)
        """
        microVals = []
        if self._localIdIdx is None:
            self.getLocalIdIndex()

        geomIdx = self.getFieldIndex(self._geomFieldName)
        for featureFID in list(self._features.keys()):
            fid = self.getFeatureValByFieldIndex(self._localIdIdx, featureFID)
            wkt = self.getFeatureValByFieldIndex(geomIdx, featureFID)
            x, y = self.getPointFromWkt(wkt)
            # returns values id, longitude(x), latitude(y)
            microVals.append((fid, x, y))
        return microVals

# ..............................................................................
    def getWkt(self):
        wkt = None
        if self._features and self._featureAttributes:
            pttxtlst = []
            self._setGeometryIndex()
            for pt in list(self._features.values()):
                wkt = pt[self._geomIdx]
                pttxtlst.append(wkt.strip('POINT'))
            multipointstr = ', '.join(pttxtlst)
            wkt = 'MULTIPOINT( {} )'.format(multipointstr)
        return wkt

# ...............................................
    def readShapefile(self, largeFile=False, dlocation=None):
        """
        @note: calls Vector.readData to create points from features. This
                 will be removed when we switch to only using features
        @param largeFile: Indicates if the largeFile should be retrieved
        @param dlocation: Overrides the object's dlocation (possibly for 
                                    temporary file)
        """
        self.clearFeatures()
        if dlocation is None:
            dlocation = self.get_dlocation(largeFile=largeFile)
        Vector.readData(self, dlocation=dlocation, doReadData=True)
