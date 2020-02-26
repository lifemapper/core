"""Module containing class and functions representing shapegrid objects
"""
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (LMFormat, ProcessType)
from LmCommon.common.time import gmt
from LmCommon.shapes.build_shapegrid import build_shapegrid
from LmServer.base.layer import _LayerParameters, Vector
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType, LMServiceType)
from osgeo import ogr, osr


# .............................................................................
class Shapegrid(_LayerParameters, Vector, ProcessObject):
# .............................................................................
    """
    shape grid class inherits from Vector
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, name, userId, epsgcode, cellsides, cellsize, mapunits,
                 bbox, siteId='siteid', siteX='centerX', siteY='centerY',
                 size=None, lyrId=None, verify=None, dlocation=None,
                 metadata={}, resolution=None, metadataUrl=None,
                 parentMetadataUrl=None, mod_time=None, featureCount=0,
                 feature_attributes={}, features={}, fidAttribute=None,
                 status=None, statusModTime=None):
        """
        @copydoc LmServer.base.service_object.ProcessObject::__init__()
        @copydoc LmServer.base.layer._LayerParameters::__init__()
        @copydoc LmServer.base.layer.Vector::__init__()
        @param cellsides: Number of sides in each cell of a site (i.e. square =4,
                          hexagon = 6).
        @param cellsize: Size in mapunits of each cell.  For cellSides = 6 
                          (hexagon).HEXAGON, this is the measurement between two 
                          vertices.
        @param siteId: Attribute identifying the site number for each cell 
        @param siteX: Attribute identifying the center X coordinate for each cell 
        @param siteY: Attribute identifying the center Y coordinate for each cell 
        @param size: Total number of cells in shapegrid
        """
        # siteIndices are a dictionary of {siteIndex: (FID, centerX, centerY)}
        # created from the data file and passed to lmpy.Matrix for headers
        self._siteIndices = None
        # field with unique values identifying each site
        self.siteId = siteId
        # field with longitude of the centroid of a site
        self.siteX = siteX
        # field with latitude of the centroid of a site
        self.siteY = siteY

        _LayerParameters.__init__(self, userId, paramId=lyrId, mod_time=mod_time)
        Vector.__init__(
            self, name, userId, epsgcode, lyrId=lyrId, verify=verify,
            dlocation=dlocation, metadata=metadata,
            dataFormat=LMFormat.SHAPE.driver, ogrType=ogr.wkbPolygon,
            mapunits=mapunits, resolution=resolution, bbox=bbox,
            svcObjId=lyrId, serviceType=LMServiceType.SHAPEGRIDS,
            metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl,
            mod_time=mod_time, featureCount=featureCount,
            feature_attributes=feature_attributes, features=features,
            fidAttribute=fidAttribute)
        ProcessObject.__init__(
            self, objId=lyrId, processType=ProcessType.RAD_BUILDGRID,
            status=status, statusModTime=statusModTime)
        # Don't necessarily need centroids (requires reading shapegrid)
#         self._setMapPrefix()
        self._setCellsides(cellsides)
        self.cellsize = cellsize
        self._size = None
        self._setCellMeasurements(size)

# ...............................................
    @classmethod
    def initFromParts(cls, vector, cellsides, cellsize, siteId='siteid',
                      siteX='centerX', siteY='centerY', size=None,
                      siteIndicesFilename=None, status=None,
                      statusModTime=None):
        shpGrid = Shapegrid(
            vector.name, vector.getUserId(), vector.epsgcode, cellsides,
            cellsize, vector.mapUnits, vector.bbox, siteId=siteId, siteX=siteX,
            siteY=siteY, size=size, lyrId=vector.getLayerId(),
            verify=vector.verify, dlocation=vector.get_dlocation(),
            metadata=vector.lyrMetadata, resolution=vector.resolution,
            metadataUrl=vector.metadataUrl,
            parentMetadataUrl=vector.parentMetadataUrl, mod_time=vector.mod_time,
            featureCount=vector.featureCount,
            feature_attributes=vector.feature_attributes,
            features=vector.features, fidAttribute=vector.fidAttribute,
            status=status, statusModTime=statusModTime)
        return shpGrid

    # ...............................................
    def updateStatus(self, status, matrixIndex=None, metadata=None,
                     mod_time=gmt().mjd):
        """
        @copydoc LmServer.base.service_object.ProcessObject::updateStatus()
        @copydoc LmServer.base.service_object.ServiceObject::updateModtime()
        @copydoc LmServer.base.layer._LayerParameters::updateParams()
        """
        ProcessObject.update_status(self, status, mod_time)
        ServiceObject.update_mod_time(self, mod_time)
        _LayerParameters.update_params(
            self, mod_time, matrixIndex=matrixIndex, metadata=metadata)

# ...............................................
    def _setCellsides(self, cellsides):
        try:
            cellsides = int(cellsides)
        except:
            raise LMError('Number of cell sides must be an integer')
        else:
            if cellsides in (4, 6):
                self._cellsides = cellsides
            else:
                raise LMError(
                    'Invalid cellshape. Only 4 (square) and 6 (hexagon) ' +
                    'sides are currently supported')

    @property
    def cellsides(self):
        return self._cellsides

# ...............................................
    def _setCellMeasurements(self, size=None):
        if size is not None and isinstance(size, int):
            self._size = size
        else:
            self._size = self._getFeatureCount()

    @property
    def size(self):
        return self._size

# ...............................................
    def getSiteIndices(self):
        """
        @TODO: make sure to add this to base object
        """
        siteIndices = {}
        if not(self._features) and self.get_dlocation() is not None:
            self.readData()
        if self._features:
            for siteidx in list(self._features.keys()):
                if siteidx is None:
                    print('WTF?')
                geom = self._features[siteidx][self._geomIdx]
                siteIndices[siteidx] = geom
        return siteIndices

# ...............................................
    def create_local_dlocation(self):
        """
        @summary: Calculates and returns the local _dlocation.
        """
        dloc = self._earl_jr.createFilename(
            LMFileType.SHAPEGRID, lyrname=self.name, usr=self._userId,
            epsg=self._epsg)
        return dloc

# ...............................................
    def checkbbox(self, minx, miny, maxx, maxy, resol):
        """
        @summary: checks the extent envelop to ensure minx is less than
        maxx etc. 
        """

        if (not(minx < maxx) and not(miny < maxy)):
            raise Exception(
                "min x is greater than max x, and min y is greater than max y")
        if not(minx < maxx):
            raise Exception("min x is greater than max x")
        if not(miny < maxy):
            raise Exception("min y is greater than max y")

        if ((maxx - minx) < resol):
            raise Exception("resolution of cellsize is greater than x range")
        if ((maxy - miny) < resol):
            raise Exception("resolution of cellsize is greater than y range")

# ...................................................
    def cutout(self, cutoutWKT, removeOrig=False, dloc=None):
        """
        @summary: create a new shapegrid from original using cutout
        @todo: Check this, it may fail on newer versions of OGR - 
                 old: CreateFeature vs new: SetFeature
        """
        if not removeOrig and dloc == None:
            raise LMError(
                "If not modifying existing shapegrid you must provide new dloc")
        ods = ogr.Open(self._dlocation)
        origLayer = ods.GetLayer(0)
        if not origLayer:
            raise LMError("Could not open Layer at: %s" % self._dlocation)
        if removeOrig:
            newdLoc = self._dlocation
            for ext in LMFormat.SHAPE.get_extensions():
                success, _ = self.deleteFile(
                    self._dlocation.replace('.shp', ext))
        else:
            newdLoc = dloc
            if os.path.exists(dloc):
                raise LMError("Shapegrid file already exists at: %s" % dloc)
            else:
                self.ready_filename(dloc, overwrite=False)

        t_srs = osr.SpatialReference()
        t_srs.ImportFromEPSG(self.epsgcode)
        drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
        ds = drv.CreateDataSource(newdLoc)
        newlayer = ds.CreateLayer(
            ds.GetName(), geom_type=ogr.wkbPolygon, srs=t_srs)
        origLyrDefn = origLayer.GetLayerDefn()
        origFieldCnt = origLyrDefn.GetFieldCount()
        for fieldIdx in range(0, origFieldCnt):
            origFldDef = origLyrDefn.GetFieldDefn(fieldIdx)
            newlayer.CreateField(origFldDef)
        # create geom from wkt
        selectedpoly = ogr.CreateGeometryFromWkt(cutoutWKT)
        minx, maxx, miny, maxy = selectedpoly.GetEnvelope()
        origFeature = origLayer.GetNextFeature()
#         siteIdIdx = origFeature.GetFieldIndex(self.siteId)
        newSiteId = 0
        while origFeature is not None:
            clone = origFeature.Clone()
            cloneGeomRef = clone.GetGeometryRef()
            if cloneGeomRef.Intersect(selectedpoly):
                # clone.SetField(siteIdIdx,newSiteId)
                newlayer.CreateFeature(clone)
                newSiteId += 1
            origFeature = origLayer.GetNextFeature()
        ds.Destroy()
        return minx, miny, maxx, maxy

# ...................................................
    def buildShape(self, cutout=None, overwrite=False):
        # After build, set_dlocation, write shapefile
        if os.path.exists(self._dlocation) and not overwrite:
            print((
                'Shapegrid file already exists at: {}'.format(self._dlocation)))
            self.readData(doReadData=False)
            self._setCellMeasurements()
            return
        self.ready_filename(self._dlocation, overwrite=overwrite)
        cellCount = build_shapegrid(
            self._dlocation, self.minX, self.minY, self.maxX, self.maxY,
            self.cellsize, self.epsgcode, self._cellsides, siteId=self.siteId,
            siteX=self.siteX, siteY=self.siteY, cutoutWKT=cutout)
        self._setCellMeasurements(size=cellCount)
        self.setVerify()

    # ................................
    def getTargetFiles(self, workDir=None):
        if workDir is None:
            workDir = ''
        targetFiles = []
        targetDir = os.path.join(
            workDir, os.path.splitext(self.getRelativeDLocation())[0])
        baseName = os.path.splitext(os.path.basename(self.get_dlocation()))[0]

        for ext in ['.shp', '.dbf', '.prj', '.shx']:
            targetFiles.append(
                os.path.join(targetDir, '{}{}'.format(baseName, ext)))
        return targetFiles
