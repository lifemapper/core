"""Module containing base service object classes
"""
from LmBackend.common.lmobj import LMObject
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import ID_PLACEHOLDER


# .............................................................................
class ServiceObject(LMObject):
    """Class for service objects

    The ServiceObject class contains all of the information for subclasses
    to be exposed in a webservice.
    """
    META_TITLE = 'title'
    META_AUTHOR = 'author'
    META_DESCRIPTION = 'description'
    META_KEYWORDS = 'keywords'
    META_CITATION = 'citation'
    META_PARAMS = 'parameters'

    # ....................................
    def __init__(self, user_id, db_id, service_type, metadata_url=None,
                 parent_metadata_url=None, parent_id=None, mod_time=None):
        """Constructor for the abstract ServiceObject class

        Args:
            user_id: id for the owner of these data
            db_id: database id of the object
            service_type: constant from LMServiceType
            metadata_url: URL for retrieving the metadata
            parent_metadata_url: URL for retrieving the metadata of a parent
                container object
            parent_id: Id of container, if any, associated with one instance of
                this parameterized object
            mod_time: Last modification Time/Date, in MJD format
        """
        self._earl_jr = EarlJr()

        self._user_id = user_id
        self._db_id = db_id
        self.service_type = service_type
        self._metadata_url = metadata_url
        # Moved from ProcessObject
        self.parent_id = parent_id
        self._parent_metadata_url = parent_metadata_url
        self.mod_time = mod_time
        if service_type is None:
            raise Exception(
                'Object {} does not have service_type'.format(type(self)))

    # ....................................
    def get_id(self):
        """Returns the database id from the object table

        Returns:
            int - Database id of the object
        """
        return self._db_id

    # ....................................
    def set_id(self, db_id):
        """Sets the database id on the object

        Args:
            db_id: The database id for the object
        """
        self._db_id = db_id

    # ....................................
    def get_user_id(self):
        """Gets the User id
        """
        return self._user_id

    # ....................................
    def set_user_id(self, usr):
        """Sets the user id on the object
        """
        self._user_id = usr

    # ....................................
    @property
    def metadata_url(self):
        """Return the metadata url for the object.

        Return the SGUID (Somewhat Globally Unique IDentifier), aka
        metadata_url, for this object

        Returns:
            URL string representing a webservice request for this object
        """
        if self._metadata_url is None:
            try:
                self._metadata_url = self.construct_metadata_url()
            except Exception as e:
                print(str(e))
        return self._metadata_url

    # ....................................
    def set_parent_metadata_url(self, url):
        """Set the parent metdata url
        """
        self._parent_metadata_url = url

    # ....................................
    @property
    def parent_metadata_url(self):
        """Get the metadata url of the parent object
        """
        return self._parent_metadata_url

    # ....................................
    def reset_metadata_url(self):
        """Gets the REST service URL for this object

        Returns:
            URL string representing a webservice request for metadata of this
                object
        """
        self._metadata_url = self.construct_metadata_url()
        return self._metadata_url

    # ....................................
    def construct_metadata_url(self):
        """Gets the REST service URL for this object

        Returns:
            str - URL string representing a webservice request for metadata of
                this object
        """
        obj_id = self.get_id()
        if obj_id is None:
            obj_id = ID_PLACEHOLDER
        return self._earl_jr.construct_lm_metadata_url(
            self.service_type, obj_id,
            parent_metadata_url=self._parent_metadata_url)

    # ....................................
    def get_url(self, format_=None):
        """Return a GET query for the Lifemapper WCS GetCoverage request

        Args:
            format_: optional string indicating the URL response format
                desired; Supported formats are GDAL Raster Format Codes,
                available at http://www.gdal.org/formats_list.html, and driver
                values in LmServer.common.lmconstants LMFormat GDAL formats.
        """
        data_url = self.metadata_url
        if format_ is not None:
            data_url = '{}/{}'.format(self.metadata_url, format_)
        return data_url

    # ....................................
    def update_mod_time(self, mod_time):
        """Update the modification time of the object
        """
        self.mod_time = mod_time

    # ....................................
    # The database id of the object
    id = property(get_id)

    # The user id of the object
    user = property(get_user_id)


# .............................................................................
class ProcessObject(LMObject):
    """Class to hold information about a parameterized object for processing.
    """
    # ....................................
    def __init__(self, obj_id=None, process_type=None, status=None,
                 status_mod_time=None):
        """Constructor

        Args:
            obj_id: Unique identifier for this parameterized object
            process_type: Integer code LmCommon.common.lmconstants.ProcessType
            status: status of processing
            status_mod_time: last status modification time in MJD format

        Note:
            The object with obj_id can be instantiated for each container, all
                use the same base object, but will be subject to different
                processes (for example PALayers intersected for every bucket)
        """
        self.obj_id = obj_id
        self.process_type = process_type
        self._status = status
        self._status_mod_time = status_mod_time

    # ....................................
    @property
    def status(self):
        """Get the status of this object
        """
        return self._status

    # ....................................
    @property
    def status_mod_time(self):
        """Return the status modification time for the object
        """
        return self._status_mod_time

    # ....................................
    def update_status(self, status, mod_time):
        """Update the status of this object
        """
        self._status = status
        self._status_mod_time = mod_time
