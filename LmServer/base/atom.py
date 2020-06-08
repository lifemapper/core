"""Module containing Atom class
"""
from LmBackend.common.lmobj import LMObject


# ..............................................................................
class Atom(LMObject):
    """Used for returning simple objects for REST url construction
    """

    # ................................
    def __init__(self, obj_id, name, url, mod_time, epsg=None):
        """Constructor

        Args:
            obj_id: The database identifier for the object
            name: A name for the object
            url: A url for object metadata
            mod_time: The date / time that the object was last modified
            epsg: The EPSG code for the object if it is spatial
        """
        super().__init__()
        self.object_id = obj_id
        self.name = name
        self.url = url
        self.mod_time = mod_time
        self.epsg_code = epsg

    # ................................
    def get_id(self):
        """Return the database id for this object
        """
        return self.object_id
