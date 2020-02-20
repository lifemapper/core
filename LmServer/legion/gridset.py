"""Module that contains the RADExperiment class
"""
import os
import subprocess
from types import StringType

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (LMFormat, MatrixType)
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import (ID_PLACEHOLDER, LMFileType,
                                                      LMServiceType)
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.tree import Tree
from osgeo import ogr

# TODO: Move these to localconstants
NUM_RAND_GROUPS = 30
NUM_RAND_PER_GROUP = 2


# .............................................................................
class Gridset(ServiceObject):  # LMMap
    """
    The Gridset class contains all of the information for one view (extent and 
    resolution) of a RAD experiment.  
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, name=None, metadata={},
                     shapeGrid=None, shapeGridId=None, tree=None, treeId=None,
                     siteIndicesFilename=None,
                     dlocation=None, epsgcode=None, matrices=None,
                     userId=None, gridsetId=None, metadataUrl=None, mod_time=None):
        """
        @summary Constructor for the Gridset class
        @copydoc LmServer.base.service_object.ServiceObject::__init__()
        @param gridsetId: dbId  for ServiceObject
        @param name: Short identifier for this gridset, unique for userid.
        @param shapeGrid: Vector layer with polygons representing geographic sites.
        @param siteIndices: A filename containing a dictionary with keys the 
                 unique/record identifiers and values the x, y coordinates of the 
                 sites in a Matrix (if shapeGrid is not provided)
        @param epsgcode: The EPSG code of the spatial reference system of data.
        @param matrices: list of matrices for this gridset
        @param tree: A Tree with taxa matching those in the PAM 
        """
        if shapeGrid is not None:
            if userId is None:
                userId = shapeGrid.getUserId()
            if shapeGridId is None:
                shapeGridId = shapeGrid.get_id()
            if epsgcode is None:
                epsgcode = shapeGrid.epsgcode
            elif epsgcode != shapeGrid.epsgcode:
                raise LMError('Gridset EPSG {} does not match Shapegrid EPSG {}'
                                  .format(self._epsg, shapeGrid.epsgcode))
            bbox = shapeGrid.bbox
            mapunits = shapeGrid.mapUnits

        ServiceObject.__init__(self, userId, gridsetId, LMServiceType.GRIDSETS,
                                      metadataUrl=metadataUrl, mod_time=mod_time)
        title = 'Matrix map for Gridset {}'.format(name)
        # LMMap.__init__(self, name, title, self._mapPrefix,
        #                    epsgcode, bbox, mapunits, mapType=LMFileType.OTHER_MAP)
        # TODO: Aimee, do you want to move this somewhere else?
        self._dlocation = None
        self._mapFilename = None
        self._setMapPrefix()
        self.name = name
        self.grdMetadata = {}
        self.loadGrdMetadata(metadata)
        self._shapeGrid = shapeGrid
        self._shapeGridId = shapeGridId
        self._dlocation = None
        if dlocation is not None:
            self.set_dlocation(dlocation=dlocation)
        self._setEPSG(epsgcode)
        self._matrices = []
        self.setMatrices(matrices, doRead=False)
        self._tree = tree

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

# ...............................................
    @property
    def treeId(self):
        try:
            return self._tree.get_id()
        except:
            return None

    @property
    def tree(self):
        return self._tree

# ...............................................
    def setLocalMapFilename(self, mapfname=None):
        """
        @note: Overrides existing _mapFilename
        @summary: Set absolute mapfilename containing all computed layers for this 
                     Gridset. 
        """
        if mapfname is None:
            mapfname = self._earlJr.createFilename(LMFileType.RAD_MAP,
                                                            gridsetId=self.get_id(),
                                                            usr=self._userId)
        self._mapFilename = mapfname

# ...............................................
    def clearLocalMapfile(self):
        """
        @summary: Delete the mapfile containing this layer
        """
        if self.mapfilename is None:
            self.setLocalMapFilename()
        success, msg = self.deleteFile(self.mapfilename)

# ...............................................
    def _createMapPrefix(self):
        """
        @summary: Construct the endpoint of a Lifemapper WMS URL for 
                     this object.
        @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
                 map=<mapname>, and key/value pairs.  
        @note: If the object has not yet been inserted into the database, a 
                 placeholder is used until replacement after database insertion.
        """
        grdid = self.get_id()
        if grdid is None:
            grdid = ID_PLACEHOLDER
        mapprefix = self._earlJr.constructMapPrefixNew(urlprefix=self.metadataUrl,
                                        ftype=LMFileType.RAD_MAP, mapname=self.mapName,
                                        usr=self._userId)
        return mapprefix

    def _setMapPrefix(self):
        mapprefix = self._createMapPrefix()
        self._mapPrefix = mapprefix

    @property
    def mapPrefix(self):
        return self._mapPrefix

# ...............................................
    @property
    def mapFilename(self):
        if self._mapFilename is None:
            self.setLocalMapFilename()
        return self._mapFilename

# ...............................................
    @property
    def mapName(self):
        mapname = None
        if self._mapFilename is not None:
            pth, mapfname = os.path.split(self._mapFilename)
            mapname, ext = os.path.splitext(mapfname)
        return mapname

# .............................................................................
# Private methods
# .............................................................................
# .............................................................................
# Methods
# .............................................................................

# ...............................................
    def getShapegrid(self):
        return self._shapeGrid

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
                 self.get_id() and
                 self._getEPSG() is not None):
                self._path = self._earlJr.createDataPath(self._userId,
                                         LMFileType.UNSPECIFIED_RAD,
                                         epsg=self._epsg, gridsetId=self.get_id())
            else:
                raise LMError()

    @property
    def path(self):
        if self._path is None:
            self.setPath()
        return self._path

# ...............................................
    def create_local_dlocation(self):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        dloc = self._earlJr.createFilename(LMFileType.GRIDSET_PACKAGE,
                                                      objCode=self.get_id(),
                                                      gridsetId=self.get_id(),
                                                      usr=self.getUserId())
        return dloc

    def get_dlocation(self):
        """
        @summary: Return the _dlocation attribute; create and set it if empty
        """
        self.set_dlocation()
        return self._dlocation

    def set_dlocation(self, dlocation=None):
        """
        @summary: Set the _dlocation attribute if it is None.  Use dlocation
                     if provided, otherwise calculate it.
        @note: Does NOT override existing dlocation, use clear_dlocation for that
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    def clear_dlocation(self):
        self._dlocation = None

    # .............................
    def getPackageLocation(self):
        """
        @summary: Get the file path for storing or retrieving a gridset package
        @todo: Aimee, please change this as you see fit.  If you change the 
                     function name, modify the package formatter.
        """
        return os.path.join(os.path.dirname(self.get_dlocation()),
                            'gs_{}_package{}'.format(self.get_id(), LMFormat.ZIP.ext))

# ...............................................
    def setMatrices(self, matrices, doRead=False):
        """
        @summary Fill a Matrix object from Matrix or existing file
        """
        if matrices is not None:
            for mtx in matrices:
                try:
                    self.addMatrix(mtx)
                except Exception as e:
                    raise LMError('Failed to add matrix {}'.format(mtx))

# ...............................................
    def addTree(self, tree, doRead=False):
        """
        @summary Fill the Tree object, updating the tree dlocation
        """
        if isinstance(tree, Tree):
            tree.setUserId(self.getUserId())
            # Make sure to set the parent Id and URL
            if self.get_id() is not None:
                tree.parentId = self.get_id()
                tree.setParentMetadataUrl(self.metadataUrl)
            self._tree = tree

# ...............................................
    def addMatrix(self, mtxFileOrObj, doRead=False):
        """
        @summary Fill a Matrix object from Matrix or existing file
        """
        mtx = None
        if mtxFileOrObj is not None:
            usr = self.getUserId()
            if isinstance(mtxFileOrObj, StringType) and os.path.exists(mtxFileOrObj):
                mtx = LMMatrix(dlocation=mtxFileOrObj, userId=usr)
                if doRead:
                    mtx.readData()
            elif isinstance(mtxFileOrObj, LMMatrix):
                mtx = mtxFileOrObj
                mtx.setUserId(usr)
            if mtx is not None:
                # Make sure to set the parent Id and URL
                if self.get_id() is not None:
                    mtx.parentId = self.get_id()
                    mtx.setParentMetadataUrl(self.metadataUrl)
                if mtx.get_id() is None:
                    self._matrices.append(mtx)
                else:
                    existingIds = [m.get_id() for m in self._matrices]
                    if mtx.get_id() not in existingIds:
                        self._matrices.append(mtx)

    def getMatrices(self):
        return self._matrices

    def _getMatrixTypes(self, mtypes):
        if type(mtypes) is int:
            mtypes = [mtypes]
        mtxs = []
        for mtx in self._matrices:
            if mtx.matrixType in mtypes:
                mtxs.append(mtx)
        return mtxs

    def getAllPAMs(self):
        return  self._getMatrixTypes([MatrixType.PAM, MatrixType.ROLLING_PAM])

    def getPAMs(self):
        return  self._getMatrixTypes(MatrixType.PAM)

    def getRollingPAMs(self):
        return  self._getMatrixTypes(MatrixType.ROLLING_PAM)

    def getGRIMs(self):
        return self._getMatrixTypes(MatrixType.GRIM)

    def getBiogeographicHypotheses(self):
        return self._getMatrixTypes(MatrixType.BIOGEO_HYPOTHESES)

    def getGRIMForCodes(self, gcmCode, altpredCode, dateCode):
        for grim in self.getGRIMs():
            if (grim.gcmCode == gcmCode and
                grim.altpredCode == altpredCode and
                grim.dateCode == dateCode):
                return grim
        return None

    def getPAMForCodes(self, gcmCode, altpredCode, dateCode, algorithmCode):
        for pam in self.getAllPAMs():
            if (pam.gcmCode == gcmCode and
                 pam.altpredCode == altpredCode and
                 pam.dateCode == dateCode and
                 pam.algorithmCode == algorithmCode):
                return pam
        return None

    def setMatrixProcessType(self, processType, matrixTypes=[], matrixId=None):
        if type(matrixTypes) is int:
            matrixTypes = [matrixTypes]
        matching = []
        for mtx in self._matrices:
            if matrixTypes:
                if mtx.matrixType in matrixTypes:
                    matching.append(mtx)
            elif matrixId is not None:
                matching.append(mtx)
                break
        for mtx in matching:
            mtx.processType = processType

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
            self._shapeGrid.copy_data(self._shapeGrid.get_dlocation(),
                                            target_dlocation=shpfilename,
                                            format=self._shapeGrid.dataFormat)
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(self._shapeGrid.dataFormat)
            try:
                shpDs = drv.Open(shpfilename, True)
            except Exception as e:
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

            # For each site/feature, fill with value from matrix
            currFeat = shpLyr.GetNextFeature()
            sitesKeys = sorted(self.getSitesPresent().keys())
            print("starting feature loop")
            while currFeat is not None:
                # for lyridx in range(mlyrCount):
                for lyridx, exists in self._layersPresent.items():
                    if exists:
                        # add field to the layer
                        fldname = 'lyr%s' % str(lyridx)
                        siteidx = currFeat.GetFieldAsInteger(self._shapeGrid.siteId)
                        # sitesKeys = sorted(self.getSitesPresent().keys())
                        realsiteidx = sitesKeys.index(siteidx)
                        currval = matrix.getValue(realsiteidx, lyridx)
                        # debug
                        currFeat.SetField(fldname, currval)
                # add feature to the layer
                shpLyr.SetFeature(currFeat)
                currFeat.Destroy()
                currFeat = shpLyr.GetNextFeature()
            # print 'Last siteidx %d' % siteidx

            # Closes and flushes to disk
            shpDs.Destroy()
            print(('Closed/wrote dataset %s' % shpfilename))
            success = True
            try:
                retcode = subprocess.call(["shptree", "%s" % shpfilename])
                if retcode != 0:
                    print('Unable to create shapetree index on %s' % shpfilename)
            except Exception as e:
                print('Unable to create shapetree index on %s: %s' % (shpfilename,
                                                                                        str(e)))
        return success

    # ...............................................
    def writeMap(self, mapfilename):
        pass
        # LMMap.writeMap(self, mapfilename, shpGrid=self._shapeGrid,
        #                    matrices=self._matrices)

#     # ...............................................
#     def updateModtime(self, mod_time=gmt().mjd):
#         """
#         @copydoc LmServer.base.service_object.ProcessObject::updateModtime()
#         """
#         ServiceObject.updateModtime(self, mod_time)

# .............................................................................
# Public methods
# .............................................................................
# ...............................................
    def dumpGrdMetadata(self):
        return super(Gridset, self)._dump_metadata(self.grdMetadata)

# ...............................................
    def loadGrdMetadata(self, newMetadata):
        self.grdMetadata = super(Gridset, self)._load_metadata(newMetadata)

# ...............................................
    def addGrdMetadata(self, newMetadataDict):
        self.grdMetadata = super(Gridset, self)._add_metadata(newMetadataDict,
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
