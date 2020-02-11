"""
"""
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.verify import compute_hash

# ..............................................................................
class ScientificName(LMObject):
    def __init__(self, scientificName, rank=None, canonicalName=None, 
                     userId=None, squid=None,
                     kingdom=None, phylum=None, txClass=None, txOrder=None, 
                     family=None, genus=None, lastOccurrenceCount=None, 
                     mod_time=None, 
                     taxonomySourceId=None, taxonomySourceKey=None, 
                     taxonomySourceGenusKey=None, taxonomySourceSpeciesKey=None, 
                     taxonomySourceKeyHierarchy=None, scientificNameId=None):
        """
        @summary: Constructor for the ScientificName class
        """
        LMObject.__init__(self)
        # species
        self.scientificName = scientificName
        self.canonicalName = canonicalName
        self.rank = rank
        self.userId = userId
        self.kingdom = kingdom 
        self.phylum = phylum
        self.txClass = txClass
        self.txOrder = txOrder
        self.family = family
        self.genus = genus
        self.lastOccurrenceCount = lastOccurrenceCount 
        self.mod_time = mod_time
        self._sourceId = taxonomySourceId 
        self._sourceKey = taxonomySourceKey
        self._sourceGenusKey = taxonomySourceGenusKey
        self._sourceSpeciesKey = taxonomySourceSpeciesKey
        self._sourceKeyHierarchy = taxonomySourceKeyHierarchy
        self._dbId = scientificNameId 
        self._squid = None
        self._setSquid(squid)
        
# .............................................................................
# Public methods
# .............................................................................
    def get_id(self):
        """
        @summary Returns the database id from the object table
        @return integer database id of the object
        """
        return self._dbId
    
    def set_id(self, id):
        """
        @summary: Sets the database id on the object
        @param id: The database id for the object
        """
        self._dbId = id
        
    @property
    def name(self):
        if self.canonicalName is None:
            return self.scientificName
        return self.canonicalName

    @property
    def taxonomySourceId(self):
        return self._sourceId

    @property
    def sourceTaxonKey(self):
        return self._sourceKey

    @property
    def sourceSpeciesKey(self):
        return self._sourceSpeciesKey

    @property
    def sourceGenusKey(self):
        return self._sourceGenusKey

    @property
    def sourceKeyHierarchy(self):
        return self._sourceKeyHierarchy
    
    @property
    def squid(self):
        return self._squid

    def _setSquid(self, squid=None):
        if squid is None:            
            if self._sourceId is not None and self._sourceKey is not None:
                squid = compute_hash(content='{}:{}'.format(self._sourceId, self._sourceKey))
            elif self.userId is not None and self.scientificName is not None:
                mod_sciname = self.scientificName.replace('_', ' ')
                squid = compute_hash(content='{}:{}'.format(self.userId, mod_sciname))
            else:
                raise LMError('Scientific name requires unique identifier comprised of: '+
                                    'taxonomySourceId/taxonomySourceKey OR userid/scientificName')
        self._squid = squid
