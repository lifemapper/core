"""Module containing MatrixColumn class
"""
from LmCommon.common.lmconstants import LMFormat
from LmCommon.common.time import gmt
from LmServer.base.layer import _LayerParameters
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType


# .............................................................................
class MatrixColumn(_LayerParameters, ServiceObject, ProcessObject):
    """Class representing individual matrix columns."""
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
    # Minimum percentage of acceptable value for PAM gridcell intersect
    #    computation
    INTERSECT_PARAM_MIN_PRESENCE = 'min_presence'
    # Maximum percentage of acceptable value for PAM gridcell intersect
    #    computation
    INTERSECT_PARAM_MAX_PRESENCE = 'max_presence'

    # Types of GRIM gridcell intersect computation
    INTERSECT_PARAM_WEIGHTED_MEAN = 'weighted_mean'
    INTERSECT_PARAM_LARGEST_CLASS = 'largest_class'

    # ....................................
    def __init__(self, matrix_index, matrix_id, user_id,
                 # inputs if this is connected to a layer and shapegrid
                 layer=None, layer_id=None, shapegrid=None,
                 # shapegrid_id=None,
                 intersect_params=None, squid=None, ident=None,
                 metadata=None, matrix_column_id=None, post_to_solr=True,
                 process_type=None, status=None, status_mod_time=None):
        """Constructor

        Args:
            matrix_index: index for column within a matrix.  For the Global
                PAM, assembled dynamically, this will be None.
            matrix_id: parent matrix database id for ServiceObject
            user_id: id for the owner of these data
            layer: layer input to intersect
            layer_id: database id for layer
            shapegrid: grid input to intersect
            intersect_params: parameters input to intersect
            squid: species unique identifier for column
            ident: (non-species) unique identifier for column
            metadata: dictionary of metadata keys/values; key constants are
                class attributes.
            matrix_column_id: database id of the object
            post_to_solr: flag indicating whether to write values to Solr index
            process_type: Integer code LmCommon.common.lmconstants.ProcessType
            status: status of processing
            status_mod_time: last status modification time in MJD format
        """
        _LayerParameters.__init__(
            self, user_id, param_id=matrix_column_id,
            matrix_index=matrix_index, metadata=metadata,
            mod_time=status_mod_time)
        ServiceObject.__init__(
            self, user_id, matrix_column_id, LMServiceType.MATRIX_COLUMNS,
            parent_id=matrix_id, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, obj_id=matrix_column_id, process_type=process_type,
            status=status, status_mod_time=status_mod_time)
        self.layer = layer
        self._layer_id = layer_id
        self.shapegrid = shapegrid
        self.intersect_params = {}
        self.load_intersect_params(intersect_params)
        self.squid = squid
        self.ident = ident
        self.post_to_solr = post_to_solr

    # ....................................
    def set_id(self, mtx_col_id):
        """Set the database identifier on the object

        Args:
            mtx_col_id: The database id for the object
        """
        self.obj_id = mtx_col_id

    # ....................................
    def get_id(self):
        """Returns the database id from the object table

        Returns:
            int - The database id of the object
        """
        return self.obj_id

    # ....................................
    def get_layer_id(self):
        """Return the layer identifier for the matrix column."""
        if self.layer is not None:
            return self.layer.get_id()
        if self._layer_id is not None:
            return self._layer_id
        return None

    # ....................................
    @property
    def display_name(self):
        """Return the display name for the matrix column."""
        try:
            return self.layer.display_name
        except AttributeError:
            try:
                return self.layer.name
            except AttributeError:
                return self.squid
        return None

    # ....................................
    def dump_intersect_params(self):
        """Dump intersect parameters to a string
        """
        return super(MatrixColumn, self)._dump_metadata(self.intersect_params)

    # ....................................
    def load_intersect_params(self, new_intersect_params):
        """Load intersect parameters as a string."""
        self.intersect_params = super(MatrixColumn, self)._load_metadata(
            new_intersect_params)

    # ....................................
    def add_intersect_params(self, new_intersect_params):
        """Add intersect parameters for the matrix column."""
        self.intersect_params = super(MatrixColumn, self)._add_metadata(
            new_intersect_params, existing_metadata_dict=self.intersect_params)

    # ....................................
    def update_status(self, status, matrix_index=None, metadata=None,
                      mod_time=gmt().mjd):
        """Update status of matrix column and update metadata."""
        ProcessObject.update_status(self, status, mod_time)
        _LayerParameters.update_params(
            self, mod_time, matrix_index=matrix_index, metadata=metadata)

    # ....................................
    def get_target_filename(self):
        """Return temporary filename for output.

        Todo:
            Replace with consistent file construction from
                EarlJr.create_basename.
        """
        return 'mtxcol_{}{}'.format(self.get_id(), LMFormat.MATRIX.ext)
