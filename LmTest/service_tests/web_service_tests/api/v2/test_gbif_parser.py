"""This module tests the GBIF names parser services

Note:
    * These tests use the client library (filled in by pytest) to make requests

Todo:
    * Keys should be in constants somewhere and used by both service and tests
"""
from copy import deepcopy
from random import randint, shuffle

# TODO: Get these from constants
ACCEPTED_NAME_KEY = 'accepted_name'
SEARCH_NAME_KEY = 'search_name'
TAXON_ID_KEY = 'taxon_id'

# .............................................................................
class Test_gbif_parser_services(object):
    """Test the GBIF names service wrapper
    """
    # .....................................
    def test_gbif_bad_names(self, public_client, species_names):
        """Tests that "bad" names are not found

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            species_names (:obj:`SpeciesNames`): A test constant class that has
                attributes for different sets of species names.  This should be
                provided va pytest.
        """
        bad_names = deepcopy(species_names.BAD_NAMES)
        shuffle(bad_names)
        used_names = bad_names[:randint(3, len(bad_names))]
        resp = public_client.deserialize(
            public_client.post_gbif_names_parser(used_names))

        # Test that names did not resolve
        for name_info in resp:
            assert name_info[TAXON_ID_KEY] is None
            assert name_info[ACCEPTED_NAME_KEY] is None

    # .....................................
    def test_gbif_good_names(self, public_client, species_names):
        """Tests that "good" names are resolved

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            species_names (:obj:`SpeciesNames`): A test constant class that has
                attributes for different sets of species names.  This should be
                provided va pytest.
        """
        good_names = deepcopy(species_names.GOOD_NAMES)
        shuffle(good_names)
        used_names = good_names[:randint(3, len(good_names))]

        resp = public_client.deserialize(
            public_client.post_gbif_names_parser(used_names))

        # Test that names resolved
        for name_info in resp:
            assert name_info[TAXON_ID_KEY] is not None
            assert name_info[ACCEPTED_NAME_KEY] is not None

    # .....................................
    def test_gbif_mixed_names(self, public_client, species_names):
        """Tests that a mix of good and bad names works as expected

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            species_names (:obj:`SpeciesNames`): A test constant class that has
                attributes for different sets of species names.  This should be
                provided va pytest.
        """
        good_names = deepcopy(species_names.GOOD_NAMES)
        shuffle(good_names)
        good_used_names = good_names[:randint(3, len(good_names))]

        bad_names = deepcopy(species_names.BAD_NAMES)
        shuffle(bad_names)
        bad_used_names = bad_names[:randint(3, len(bad_names))]

        used_names = []
        used_names.extend(good_used_names)
        used_names.extend(bad_used_names)

        resp = public_client.deserialize(
            public_client.post_gbif_names_parser(used_names))

        # Check each name, good ones should resolve, bad should not
        for name_info in resp:
            if name_info[SEARCH_NAME_KEY] in good_used_names:
                assert name_info[TAXON_ID_KEY] is not None
                assert name_info[ACCEPTED_NAME_KEY] is not None
            else:
                assert name_info[TAXON_ID_KEY] is None
                assert name_info[ACCEPTED_NAME_KEY] is None
