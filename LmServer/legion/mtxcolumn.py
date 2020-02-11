"""
"""
from lmpy import Matrix

from LmCommon.common.lmconstants import LMFormat, BoomKeys
from LmCommon.common.time import gmt

from LmServer.base.layer2 import _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType

# .............................................................................
# .............................................................................
# .............................................................................
class MatrixColumn(Matrix, _LayerParameters, ServiceObject, ProcessObject):
    # BoomKeys uses these strings, prefixed by 'INTERSECT_'
    # Query to filter layer for intersect
    INTERSECT_PARAM_FILTER_STRING = 'filter_string'
    # Attribute used in layer intersect
    INTERSECT_PARAM_VAL_NAME = 'val_name'
    # Value of feature for layer intersect (for biogeographic hypotheses)
    INTERSECT_PARAM_VAL_VALUE = 'val_value'
    # Units of measurement for attribute used in layer intersect
    INTERSECT_PARAM_VAL_UNITS = 'val_units'
    # Minimum spatial coverage for gridcell intersect computation
    INTERSECT_PARAM_MIN_PERCENT = 'min_percent'
    # Minimum percentage of acceptable value for PAM gridcell intersect computation 
    INTERSECT_PARAM_MIN_PRESENCE = 'min_presence'
    # Maximum percentage of acceptable value for PAM gridcell intersect computation 
    INTERSECT_PARAM_MAX_PRESENCE = 'max_presence'

    # Types of GRIM gridcell intersect computation
    INTERSECT_PARAM_WEIGHTED_MEAN = 'weighted_mean'
    INTERSECT_PARAM_LARGEST_CLASS = 'largest_class'
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, matrixIndex, matrixId, userId, 
                     # inputs if this is connected to a layer and shapegrid 
                     layer=None, layerId=None, 
                     shapegrid=None, shapeGridId=None, 
                     intersectParams={}, 
                     squid=None, ident=None,
                     processType=None, 
                     metadata={}, 
                     matrixColumnId=None, 
                     postToSolr=True,
                     status=None, statusModTime=None):
        """
        @summary MatrixColumn constructor
        @copydoc LmServer.base.layer2._LayerParameters::__init__()
        @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
        @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
        @param matrixIndex: index for column within a matrix.  For the Global 
                 PAM, assembled dynamically, this will be None.
        @param matrixId: 
        @param layer: layer input to intersect
        @param shapegrid: grid input to intersect 
        @param intersectParams: parameters input to intersect
        @param squid: species unique identifier for column
        @param ident: (non-species) unique identifier for column
        """
        _LayerParameters.__init__(self, userId, paramId=matrixColumnId, 
                                          matrixIndex=matrixIndex, metadata=metadata, 
                                          mod_time=statusModTime)
        ServiceObject.__init__(self,  userId, matrixColumnId, 
                                      LMServiceType.MATRIX_COLUMNS, parentId=matrixId, 
                                      mod_time=statusModTime)
        ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                                      status=status, statusModTime=statusModTime)
        self.layer = layer
        self._layerId = layerId
        self.shapegrid = shapegrid
        self.intersectParams = {}
        self.loadIntersectParams(intersectParams)
        self.squid = squid
        self.ident = ident
        self.postToSolr = postToSolr

# ...............................................
    def set_id(self, mtxcolId):
        """
        @summary: Sets the database id on the object, and sets the 
                     dlocation of the file if it is None.
        @param mtxcolId: The database id for the object
        """
        self.objId = mtxcolId

# ...............................................
    def get_id(self):
        """
        @summary Returns the database id from the object table
        @return integer database id of the object
        """
        return self.objId
    
# ...............................................
    def getLayerId(self):
        if self.layer is not None:
            return self.layer.get_id()
        elif self._layerId is not None:
            return self._layerId
        return None
    
# ...............................................
    @property
    def displayName(self):
        try:
            dname = self.layer.displayName
        except:
            try:
                dname = self.layer.name
            except:
                dname = self.squid
        return dname

# ...............................................
    def dumpIntersectParams(self):
        return super(MatrixColumn, self)._dump_metadata(self.intersectParams)
 
# ...............................................
    def loadIntersectParams(self, newIntersectParams):
        self.intersectParams = super(MatrixColumn, self)._load_metadata(newIntersectParams)

# ...............................................
    def addIntersectParams(self, newIntersectParams):
        self.intersectParams = super(MatrixColumn, self)._add_metadata(newIntersectParams, 
                                             existingMetadataDict=self.intersectParams)
    
# ...............................................
    def updateStatus(self, status, matrixIndex=None, metadata=None, mod_time=gmt().mjd):
        """
        @summary Update status, matrixIndex, metadata, mod_time attributes on the 
                    Matrix layer. 
        @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
        @copydoc LmServer.base.layer2._LayerParameters::updateParams()
        """
        ProcessObject.updateStatus(self, status, mod_time)
        _LayerParameters.updateParams(self, mod_time, matrixIndex=matrixIndex, 
                                                metadata=metadata)

# ...............................................
    def getTargetFilename(self):
        """
        @summary: Return temporary filename for output.
        @todo: Replace with consistent file construction from 
               LmServer.common.datalocator.EarlJr.createBasename!
        """
        relFname = 'mtxcol_{}{}'.format(self.get_id(), LMFormat.MATRIX.ext)
        return relFname
