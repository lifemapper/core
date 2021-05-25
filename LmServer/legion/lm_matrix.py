"""Module that contains the Matrix class
"""
import os

from lmpy import Matrix

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import CSV_INTERFACE, MatrixType
from LmCommon.common.time import gmt
from LmServer.base.service_object import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMServiceType, LMFileType)


# .............................................................................
class LMMatrix(ServiceObject, ProcessObject):
    """The Matrix class contains a 2-dimensional numeric matrix."""
    # ....................................
    def __init__(self, matrix, headers=None, matrix_type=MatrixType.PAM,
                 process_type=None, scenario_id=None, gcm_code=None,
                 alt_pred_code=None, date_code=None, alg_code=None,
                 metadata=None, dlocation=None, metadata_url=None,
                 user_id=None, gridset=None, matrix_id=None, status=None,
                 status_mod_time=None):
        """Constructor

        Args:
            matrix (numpy.ndarray): Numpy data array for Matrix base object.
            matrix_type (int): Constant from MatrixType.
            gcm_code: Code for the Global Climate Model used to create these
                data.
            alt_pred_code: Code for the alternate prediction (i.e. IPCC
                scenario or Representative Concentration Pathways/RCPs) used to
                create these data
            date_code: Code for the time period for which these data are
                predicted.
            metadata: dictionary of metadata using Keys defined in superclasses
            dlocation: file location of the array
            gridset: parent gridset of this Matrixupdate_mod_time
            matrix_id: Database identifier for the matrix object.

        Todo:
            Replace 3 codes with scenario id
        """
        self.matrix_type = matrix_type
        self._dlocation = dlocation
        # TODO: replace 3 codes with scenario_id
        self.scenario_id = scenario_id
        self.gcm_code = gcm_code
        self.alt_pred_code = alt_pred_code
        self.date_code = date_code
        self.algorithm_code = alg_code
        self.matrix_metadata = {}
        self.load_matrix_metadata(metadata)
        self._gridset = gridset
        # parent values
        gridset_url = gridset_id = None
        if gridset is not None:
            gridset_url = gridset.metadata_url
            gridset_id = gridset.get_id()
        # Note: CJG - 04/01/2020 -
        # We really shouldn't inherit from Matrix as this object will not have
        #    the same functionality.  Use the 'matrix' attribute to get the
        #    Matrix object.
        # Matrix.__init__(self, matrix, headers=headers)
        self.matrix = matrix
        ServiceObject.__init__(
            self, user_id, matrix_id, LMServiceType.MATRICES,
            metadata_url=metadata_url, parent_metadata_url=gridset_url,
            parent_id=gridset_id, mod_time=status_mod_time)
        ProcessObject.__init__(
            self, obj_id=matrix_id, process_type=process_type, status=status,
            status_mod_time=status_mod_time)

    # ....................................
    @classmethod
    def init_from_parts(cls, base_matrix, gridset=None, process_type=None,
                        metadata_url=None, user_id=None, status=None,
                        status_mod_time=None):
        """Initialize a matrix from its parts
        """
        return cls(
            None, matrix_type=base_matrix.matrix_type,
            process_type=process_type, metadata=base_matrix.matrix_metadata,
            dlocation=base_matrix.get_dlocation(),
            metadata_url=metadata_url, user_id=user_id, gridset=gridset,
            matrix_id=base_matrix.get_matrix_id(), status=base_matrix.status,
            status_mod_time=base_matrix.status_mod_time)

    # ....................................
    @classmethod
    def init_from_file(cls, filename, matrix_type=MatrixType.PAM,
                       process_type=None, scenario_id=None, gcm_code=None,
                       alt_pred_code=None, date_code=None, alg_code=None,
                       metadata=None, metadata_url=None, user_id=None,
                       gridset=None, matrix_id=None, status=None,
                       status_mod_time=None):
        """Initialize a matrix from a file.

        Args:
            dlocation: The location of the matrix data.
            matrix_type (int): Constant from MatrixType.
            gcm_code: Code for the Global Climate Model used to create these
                data.
            alt_pred_code: Code for the alternate prediction (i.e. IPCC
                scenario or Representative Concentration Pathways/RCPs) used to
                create these data
            date_code: Code for the time period for which these data are
                predicted.
            metadata: dictionary of metadata using Keys defined in superclasses
            dlocation: file location of the array
            gridset: parent gridset of this Matrixupdate_mod_time
            matrix_id: Database identifier for the matrix object.

        Todo:
            Replace 3 codes with scenario id
        """
        mtx = Matrix.load(filename)
        return cls(mtx, matrix_type=matrix_type, process_type=process_type,
                   scenario_id=scenario_id, gcm_code=gcm_code,
                   alt_pred_code=alt_pred_code, date_code=date_code,
                   alg_code=alg_code, metadata=metadata, dlocation=filename,
                   metadata_url=metadata_url, user_id=user_id, gridset=gridset,
                   matrix_id=matrix_id, status=status,
                   status_mod_time=status_mod_time)

    # ....................................
    def update_status(self, status, metadata=None, mod_time=gmt().mjd):
        """Update status and metadata.

        Args:
            metadata: Dictionary of Matrix metadata keys/values; key constants
                are ServiceObject class attributes.

        Note:
            Missing keyword parameters are ignored.
        """
        if metadata is not None:
            self.load_matrix_metadata(metadata)
        ProcessObject.update_status(self, status, mod_time)
        ServiceObject.update_mod_time(self, mod_time)

    # ....................................
    @property
    def gridset_name(self):
        """Return the name of the gridset."""
        name = None
        if self._gridset is not None:
            name = self._gridset.name
        return name

    # ....................................
    @property
    def gridset_id(self):
        """Return the gridset database identifier."""
        gid = None
        if self._gridset is not None:
            gid = self._gridset.get_id()
        return gid

    # ....................................
    @property
    def gridset_url(self):
        """Return the gridset metadata url."""
        url = None
        if self._gridset is not None:
            url = self._gridset.metadata_url
        return url

    # ....................................
    def get_data_url(self, interface=CSV_INTERFACE):
        """Return a data url for this matrix object."""
        return self._earl_jr.construct_lm_data_url(
            self.service_type, self.get_id(), interface)

    # ....................................
    def get_relative_dlocation(self):
        """Return the relative filepath from object attributes.

        Note:
            - If the object does not have an ID, this returns None
            - This is to be pre-pended with a relative directory name for data
                used by a single workflow/Makeflow
        """
        basename = None
        self.set_dlocation()
        if self._dlocation is not None:
            _, basename = os.path.split(self._dlocation)
        return basename

    # ....................................
    def create_local_dlocation(self):
        """Create an absolute filepath from object attributes.

        Note:
            If the object does not have an ID, this returns None
        """
        ftype = LMFileType.get_matrix_filetype(self.matrix_type)
        if self.parent_id is None:
            raise LMError('Must have parent gridset ID for filepath')
        return self._earl_jr.create_filename(
            ftype, gridset_id=self.parent_id, obj_code=self.get_id(),
            usr=self.get_user_id())

    # ....................................
    def get_dlocation(self):
        """Return the _dlocation attribute; create and set it if empty."""
        self.set_dlocation()
        return self._dlocation

    # ....................................
    def set_dlocation(self, dlocation=None):
        """Set the dlocation of the matrix.

        Set the _dlocation attribute if it is None.  Use dlocation if provided,
        otherwise calculate it.

        Note:
            Does NOT override existing dlocation, use clear_dlocation for that
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    # ....................................
    def clear_dlocation(self):
        """Clear the dlocation for the matrix object."""
        self._dlocation = None

    # ....................................
    def get_gridset(self):
        """Get the gridset that this matrix belongs to."""
        return self._gridset

    # ....................................
    def get_shapegrid(self):
        """Get the shapegrid for the gridset that this matrix belongs to."""
        return self._gridset.get_shapegrid()

    # ....................................
    def dump_matrix_metadata(self):
        """Return the matrix metadata as a string."""
        return super(LMMatrix, self)._dump_metadata(self.matrix_metadata)

    # ....................................
    def add_matrix_metadata(self, new_metadata_dict):
        """Add matrix metadata."""
        self.matrix_metadata = super(LMMatrix, self)._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.matrix_metadata)

    # ....................................
    def load_matrix_metadata(self, new_metadata):
        """Load the metadata for this matrix."""
        self.matrix_metadata = super(LMMatrix, self)._load_metadata(
            new_metadata)

    # ....................................
    def write(self, dlocation=None, overwrite=False):
        """Write this matrix to the file system."""
        if dlocation is None:
            dlocation = self.get_dlocation()
        self.ready_filename(dlocation, overwrite=overwrite)

        self.matrix.write(dlocation)

    # ....................................
    def set_data(self, new_data, headers=None):
        """Set the data value for the matrix."""
        self.matrix = Matrix(new_data, headers=headers)
