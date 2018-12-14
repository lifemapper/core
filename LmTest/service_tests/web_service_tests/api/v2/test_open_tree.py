"""This module tests the Open Tree induce subtree service wrapper

Note:
    * These tests use the client library (filled in by pytest) to make requests

Todo:
    * Keys should be in constants somewhere and used by both service and tests
"""
from copy import deepcopy
from random import randint, shuffle

import dendropy
import pytest

NONTREE_GBIF_IDS_KEY = 'nontree_ids'
TREE_DATA_KEY = 'tree_data'
TREE_FORMAT_KEY = 'tree_format'
TREE_NAME_KEY = 'tree_name'
UNMATCHED_GBIF_IDS_KEY = 'unmatched_ids'

# .............................................................................
class Test_open_tree_services(object):
    """Test the Open Tree service wrapper
    """
    # .....................................
    def test_open_tree_bad_taxon_ids(self, public_client, taxon_ids):
        """Tests that a tree can be retrieved from "bad" taxon ids

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            taxon_ids (:obj:`TaxonIds`): A test constant class that has
                attributes for different sets of taxon ids.  This should be
                provided via pytest.
        """
        bad_tax_ids = deepcopy(taxon_ids.BAD_TAXON_IDS)
        shuffle(bad_tax_ids)
        # Get a random number of good taxon ids
        used_tax_ids = bad_tax_ids[:randint(3, len(bad_tax_ids))]
        resp = public_client.deserialize(
            public_client.post_open_tree_induce_subtree(used_tax_ids))

        # Check that no GBIF ids were found in open tree
        assert len(resp[NONTREE_GBIF_IDS_KEY]) == len(used_tax_ids)
        
        # Check that none of the taxon ids were included in the tree
        assert len(resp[UNMATCHED_GBIF_IDS_KEY]) == len(used_tax_ids)

        # Check that tree load fails
        with pytest.raises(dendropy.UnexpectedEndOfStreamError):
            dendropy.Tree.get(
                data=resp[TREE_DATA_KEY], schema=resp[TREE_FORMAT_KEY])

    # .....................................
    def test_open_tree_good_taxon_ids(self, public_client, taxon_ids):
        """Tests that a tree can be retrieved from "good" taxon ids

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            taxon_ids (:obj:`TaxonIds`): A test constant class that has
                attributes for different sets of taxon ids.  This should be
                provided via pytest.
        """
        good_tax_ids = deepcopy(taxon_ids.GOOD_TAXON_IDS)
        shuffle(good_tax_ids)
        # Get a random number of good taxon ids
        used_tax_ids = good_tax_ids[:randint(3, len(good_tax_ids))]
        resp = public_client.deserialize(
            public_client.post_open_tree_induce_subtree(used_tax_ids))

        # Check that all GBIF ids were found in open tree
        assert len(resp[NONTREE_GBIF_IDS_KEY]) == 0
        
        # Check that all of the taxon ids were included in the tree
        assert len(resp[UNMATCHED_GBIF_IDS_KEY]) == 0

        # Check that a tree name and format were returned
        assert resp[TREE_FORMAT_KEY]
        assert resp[TREE_NAME_KEY]

        # Attempt to load the tree
        tree = dendropy.Tree.get(
            data=resp[TREE_DATA_KEY], schema=resp[TREE_FORMAT_KEY])

    # .....................................
    def test_open_tree_mixed_taxon_ids(self, public_client, taxon_ids):
        """Tests that a tree can be retrieved from good and bad taxon ids

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            taxon_ids (:obj:`TaxonIds`): A test constant class that has
                attributes for different sets of taxon ids.  This should be
                provided via pytest.
        """
        good_tax_ids = deepcopy(taxon_ids.GOOD_TAXON_IDS)
        shuffle(good_tax_ids)
        bad_tax_ids = deepcopy(taxon_ids.BAD_TAXON_IDS)
        shuffle(bad_tax_ids)
        # Get a random number of good taxon ids
        good_used_tax_ids = good_tax_ids[:randint(3, len(good_tax_ids))]
        bad_used_tax_ids = bad_tax_ids[:randint(3, len(bad_tax_ids))]
        used_tax_ids = []
        used_tax_ids.extend(good_used_tax_ids)
        used_tax_ids.extend(bad_used_tax_ids)

        resp = public_client.deserialize(
            public_client.post_open_tree_induce_subtree(used_tax_ids))

        # Check that non of the bad GBIF ids were found in open tree
        assert len(resp[NONTREE_GBIF_IDS_KEY]) == len(bad_used_tax_ids)
        
        # Check that none of the bad taxon ids were included in the tree
        assert len(resp[UNMATCHED_GBIF_IDS_KEY]) == len(bad_used_tax_ids)

        # Check that a tree name and format were returned
        assert resp[TREE_FORMAT_KEY]
        assert resp[TREE_NAME_KEY]

        # Attempt to load the tree
        tree = dendropy.Tree.get(
            data=resp[TREE_DATA_KEY], schema=resp[TREE_FORMAT_KEY])

