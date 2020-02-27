"""Module containing tree service object class
"""
import os

from lmpy import TreeWrapper

from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import JSON_INTERFACE, DEFAULT_TREE_SCHEMA
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMFileType


# .........................................................................
class Tree(TreeWrapper, ServiceObject):
    """Class to hold Tree data
    """

    # ................................
    def __init__(self, name, metadata=None, dlocation=None, data=None,
                 schema=DEFAULT_TREE_SCHEMA, metadata_url=None, user_id=None,
                 gridset_id=None, tree_id=None, mod_time=None):
        """Constructor for the tree class.

        Args:
            name: The user-provided name of this tree
            dlocation: file of data for TreeWrapper base object
            tree_id: db_id  for ServiceObject
        """
        if metadata is None:
            metadata = {}
        ServiceObject.__init__(
            self, user_id, tree_id, LMServiceType.TREES,
            metadata_url=metadata_url, parent_id=gridset_id, mod_time=mod_time)
        self.name = name
        self._dlocation = dlocation
        self.tree_metadata = {}
        self.load_tree_metadata(metadata)

        # Read tree if available
        if dlocation is None:
            dlocation = self.get_dlocation()

        if data is not None:
            self.get(data=data, schema=schema)
        elif dlocation is not None:
            if os.path.exists(dlocation):
                self.from_filename(dlocation)

    # ................................
    def read(self, dlocation=None, schema=DEFAULT_TREE_SCHEMA):
        """Reads tree data, either from the dlocation or get_dlocation
        """
        if dlocation is None:
            dlocation = self.get_dlocation()
        self.get(path=dlocation, schema=schema)

    # ................................
    def set_tree(self, tree):
        """Set the value of the tree
        """
        self.get(
            data=tree.as_string(schema=DEFAULT_TREE_SCHEMA),
            schema=DEFAULT_TREE_SCHEMA)

    # ................................
    def write_tree(self):
        """Writes the tree to disk
        """
        dloc = self.get_dlocation()
        self.write(path=dloc, schema=DEFAULT_TREE_SCHEMA)

    # ................................
    def get_relative_dlocation(self):
        """Return the relative filepath from object attributes

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

    # ................................
    def create_local_dlocation(self):
        """Create an absolute filepath from object attributes

        Note:
            If the object does not have an ID, this returns None
        """
        return self._earl_jr.create_filename(
            LMFileType.TREE, obj_code=self.get_id(), usr=self.get_user_id())

    # ................................
    def get_dlocation(self):
        """Get the data location for this tree
        """
        self.set_dlocation()
        return self._dlocation

    # ................................
    def set_dlocation(self, dlocation=None):
        """Set the dlocation attribute if it is None.

        Set the _dlocation attribute if it is None.  Use dlocation if provided,
        otherwise calculate it.

        Note:
            Does NOT override existing dlocation, use clear_dlocation for that
        """
        if self._dlocation is None:
            if dlocation is None:
                dlocation = self.create_local_dlocation()
            self._dlocation = dlocation

    # ................................
    def clear_dlocation(self):
        """Clear the dlocation attribute
        """
        self._dlocation = None

    # ................................
    def dump_tree_metadata(self):
        """Dump tree metadata
        """
        return LMObject._dump_metadata(self.tree_metadata)

    # ................................
    def load_tree_metadata(self, new_metadata):
        """Load tree metadata
        """
        self.tree_metadata = LMObject._load_metadata(new_metadata)

    # ................................
    def add_tree_metadata(self, new_metadata_dict):
        """Add additional tree metadata
        """
        self.tree_metadata = LMObject._add_metadata(
            new_metadata_dict, existing_metadata_dict=self.tree_metadata)

    # ................................
    def get_data_url(self, interface=JSON_INTERFACE):
        """Get a data service url for this tree
        """
        return self._earl_jr.construct_lm_data_url(
            self.service_type, self.get_id(), interface)
