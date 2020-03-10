"""Module containing scientific name class
"""
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.verify import compute_hash


# ..............................................................................
class ScientificName(LMObject):
    """CLass containing taxonomic information and LM specifics
    """
    # ................................
    def __init__(self, scientific_name, rank=None, canonical_name=None,
                 user_id=None, squid=None, kingdom=None, phylum=None,
                 class_=None, order_=None, family=None, genus=None,
                 last_occurrence_count=None, mod_time=None,
                 taxonomy_source_id=None, taxonomy_source_key=None,
                 taxonomy_source_genus_key=None,
                 taxonomy_source_species_key=None,
                 taxonomy_source_key_hierarchy=None, scientific_name_id=None):
        """Constructor for the ScientificName class
        """
        # species
        self.scientific_name = scientific_name
        self.canonical_name = canonical_name
        self.rank = rank
        self.user_id = user_id
        self.kingdom = kingdom
        self.phylum = phylum
        self.class_ = class_
        self.order_ = order_
        self.family = family
        self.genus = genus
        self.last_occurrence_count = last_occurrence_count
        self.mod_time = mod_time
        self._source_id = taxonomy_source_id
        self._source_key = taxonomy_source_key
        self._source_genus_key = taxonomy_source_genus_key
        self._source_species_key = taxonomy_source_species_key
        self._source_key_hierarchy = taxonomy_source_key_hierarchy
        self._db_id = scientific_name_id
        self._squid = None
        self._set_squid(squid)

    # ................................
    def get_id(self):
        """Returns the database id from the object table

        Returns:
            int - database id of the object
        """
        return self._db_id

    # ................................
    def set_id(self, db_id):
        """Sets the database id on the object

        Args:
            db_id (int): The database id for the object
        """
        self._db_id = db_id

    # ................................
    @property
    def name(self):
        """Return the canonical or scientific name of the taxon
        """
        if self.canonical_name is None:
            return self.scientific_name
        return self.canonical_name

    # ................................
    @property
    def taxonomy_source_id(self):
        """Return the taxonomy source id
        """
        return self._source_id

    # ................................
    @property
    def source_taxon_key(self):
        """Return the taxon source key
        """
        return self._source_key

    # ................................
    @property
    def source_species_key(self):
        """Return the species key
        """
        return self._source_species_key

    # ................................
    @property
    def source_genus_key(self):
        """Return the genus key
        """
        return self._source_genus_key

    # ................................
    @property
    def source_key_hierarchy(self):
        """Return the source key hierarchy
        """
        return self._source_key_hierarchy

    # ................................
    @property
    def squid(self):
        """Return the squid for this taxon
        """
        return self._squid

    # ................................
    def _set_squid(self, squid=None):
        if squid is None:
            if self._source_id is not None and self._source_key is not None:
                squid = compute_hash(
                    content='{}:{}'.format(self._source_id, self._source_key))
            elif self.user_id is not None and self.scientific_name is not None:
                mod_sciname = self.scientific_name.replace('_', ' ')
                squid = compute_hash(
                    content='{}:{}'.format(self.user_id, mod_sciname))
            else:
                raise LMError(
                    ('Scientific name requires unique identifier comprised of:'
                     ' taxonomySourceId/taxonomySourceKey OR '
                     'userid/scientificName'))
        self._squid = squid
