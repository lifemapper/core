"""Module contianin Atom class
"""
from LmBackend.common.lmobj import LMObject

# ..............................................................................
class Atom(LMObject):
    """
    Used for returning simple objects for REST url construction 
    """
    
    def __init__(self, id, name, url, modTime, epsg=None):
        """
        @summary: Constructor for the Atom class
        @param id: database id of the object
        @param name: name of the object
        @param modTime: time/date last modified
        """
        LMObject.__init__(self)
        self.id = id
        self.name = name
        self.url = url
        self.modTime = modTime
        self.epsgcode = epsg

# ...............................................
# included only to allow use of full or atom objects in Scribe/Peruser methods
    def getId(self):
        """
        @summary: Return the database id for this object
        """
        return self.id
    
