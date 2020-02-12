"""Module that contains the Matrix class
"""
import os

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import CSV_INTERFACE, MatrixType
from LmCommon.common.time import gmt
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMServiceType, LMFileType)
from lmpy import Matrix


# .............................................................................
class LMMatrix(Matrix, ServiceObject, ProcessObject):
    """
    The Matrix class contains a 2-dimensional numeric matrix.
    """

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, matrix, headers=None,
                     matrixType=MatrixType.PAM,
                     processType=None,
                     # TODO: replace 3 codes with scenarioId
                     scenarioid=None,
                     gcmCode=None, altpredCode=None, dateCode=None,
                     algCode=None,
                     metadata={},
                     dlocation=None,
                     metadataUrl=None,
                     userId=None,
                     gridset=None,
                     matrixId=None,
                     status=None, statusModTime=None):
        """
        @param matrix: data (numpy) array for Matrix base object
        @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
        @param gcmCode: Code for the Global Climate Model used to create these data
        @param altpredCode: Code for the alternate prediction (i.e. IPCC scenario 
                 or Representative Concentration Pathways/RCPs) used to create 
                 these data
        @param dateCode: Code for the time period for which these data are predicted.
        @param metadata: dictionary of metadata using Keys defined in superclasses
        @param dlocation: file location of the array
        @param gridset: parent gridset of this MatrixupdateModtime
        @param matrixId: dbId  for ServiceObject
        """
        self.matrixType = matrixType
        self._dlocation = dlocation
        # TODO: replace 3 codes with scenarioId
        self.scenarioId = scenarioid
        self.gcmCode = gcmCode
        self.altpredCode = altpredCode
        self.dateCode = dateCode
        self.algorithmCode = algCode
        self.mtxMetadata = {}
        self.loadMtxMetadata(metadata)
        self._gridset = gridset
        # parent values
        gridsetUrl = gridsetId = None
        if gridset is not None:
            gridsetUrl = gridset.metadataUrl
            gridsetId = gridset.get_id()
        Matrix.__init__(self, matrix, headers=headers)
        ServiceObject.__init__(self, userId, matrixId, LMServiceType.MATRICES,
                                      metadataUrl=metadataUrl,
                                      parentMetadataUrl=gridsetUrl,
                                      parentId=gridsetId,
                                      mod_time=statusModTime)
        ProcessObject.__init__(self, objId=matrixId, processType=processType,
                                      status=status, statusModTime=statusModTime)

# ...............................................
    @classmethod
    def initFromParts(cls, baseMtx, gridset=None,
                            processType=None, metadataUrl=None, userId=None,
                            status=None, statusModTime=None):
        mtxobj = LMMatrix(None, matrixType=baseMtx.matrixType,
                                processType=processType, metadata=baseMtx.mtxMetadata,
                                dlocation=baseMtx.getDLocation(),
                                columnIndices=baseMtx.getColumnIndices(),
                                columnIndicesFilename=baseMtx.getColumnIndicesFilename(),
                                metadataUrl=metadataUrl, userId=userId, gridset=gridset,
                                matrixId=baseMtx.getMatrixId(), status=baseMtx.status,
                                statusModTime=baseMtx.statusModTime)
        return mtxobj

    # ...............................................
    def updateStatus(self, status, metadata=None, mod_time=gmt().mjd):
        """
        @summary: Updates matrixIndex, paramMetadata, and mod_time.
        @param metadata: Dictionary of Matrix metadata keys/values; key constants  
                              are ServiceObject class attributes.
        @copydoc LmServer.base.service_object.ProcessObject::updateStatus()
        @copydoc LmServer.base.service_object.ServiceObject::updateModtime()
        @note: Missing keyword parameters are ignored.
        """
        if metadata is not None:
            self.loadMtxMetadata(metadata)
        ProcessObject.updateStatus(self, status, mod_time)
        ServiceObject.updateModtime(self, mod_time)

# ...............................................
    @property
    def gridsetName(self):
        name = None
        if self._gridset is not None:
            name = self._gridset.name
        return name

# ...............................................
    @property
    def gridsetId(self):
        gid = None
        if self._gridset is not None:
            gid = self._gridset.get_id()
        return gid

# ...............................................
    @property
    def gridsetUrl(self):
        url = None
        if self._gridset is not None:
            url = self._gridset.metadataUrl
        return url

# ...............................................
    def getDataUrl(self, interface=CSV_INTERFACE):
        durl = self._earlJr.constructLMDataUrl(self.serviceType, self.get_id(),
                                                            interface)
        return durl

# ...............................................
    def getRelativeDLocation(self):
        """
        @summary: Return the relative filepath from object attributes
        @note: If the object does not have an ID, this returns None
        @note: This is to be pre-pended with a relative directory name for data  
                 used by a single workflow/Makeflow 
        """
        basename = None
        self.setDLocation()
        if self._dlocation is not None:
            pth, basename = os.path.split(self._dlocation)
        return basename

    def createLocalDLocation(self):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        ftype = LMFileType.getMatrixFiletype(self.matrixType)
        if self.parentId is None:
            raise LMError('Must have parent gridset ID for filepath')
        dloc = self._earlJr.createFilename(ftype, gridsetId=self.parentId,
                                                     objCode=self.get_id(),
                                                     usr=self.getUserId())
        return dloc

    def getDLocation(self):
        """
        @summary: Return the _dlocation attribute; create and set it if empty
        """
        self.setDLocation()
        return self._dlocation

    def setDLocation(self, dlocation=None):
        """
        @summary: Set the _dlocation attribute if it is None.  Use dlocation
                     if provided, otherwise calculate it.
        @note: Does NOT override existing dlocation, use clearDLocation for that
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.createLocalDLocation()
            self._dlocation = dlocation

    def clearDLocation(self):
        self._dlocation = None

# ...............................................
    def getGridset(self):
        return self._gridset

# ...............................................
    def getShapegrid(self):
        return self._gridset.getShapegrid()

# ...............................................
    def dumpMtxMetadata(self):
        return super(LMMatrix, self)._dump_metadata(self.mtxMetadata)

# ...............................................
    def addMtxMetadata(self, newMetadataDict):
        self.mtxMetadata = super(LMMatrix, self)._add_metadata(newMetadataDict,
                                             existingMetadataDict=self.mtxMetadata)

# ...............................................
    def loadMtxMetadata(self, newMetadata):
        self.mtxMetadata = super(LMMatrix, self)._load_metadata(newMetadata)

    # .............................
    def write(self, dlocation=None, overwrite=False):
        """
        @summary: Writes this matrix to the file system
        """
        if dlocation is None:
            dlocation = self.getDLocation()
        self.ready_filename(dlocation, overwrite=overwrite)

        with open(dlocation, 'w') as outF:
            self.save(outF)
