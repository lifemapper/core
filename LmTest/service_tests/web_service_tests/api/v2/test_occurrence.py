"""Tests for the occurrence set web services
"""
import argparse
import contextlib
import json
import unittest
import urllib2
import warnings

from LmCommon.common.lmconstants import (CSV_INTERFACE, EML_INTERFACE,
                           GEO_JSON_INTERFACE, JSON_INTERFACE, KML_INTERFACE, 
                           SHAPEFILE_INTERFACE, HTTPStatus)

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.occlayer import OccurrenceLayer

from LmTest.formatTests.csvValidator import validate_csv
from LmTest.formatTests.emlValidator import validate_eml
from LmTest.formatTests.geoJsonValidator import validate_geojson
from LmTest.formatTests.jsonValidator import validate_json
from LmTest.formatTests.kmlValidator import validate_kml
from LmTest.formatTests.shapefileValidator import validate_shapefile
import pytest
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.algorithm import Algorithm

# .............................................................................
class Test_occurrence_layer_web_services(object):
    """Test class for occurrence set services
    """
    # ............................
    def test_count_occurrence_sets_no_parameters(self, public_client, scribe):
        """Tests the count service with the default parameters

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            scribe (:obj:`BorgScribe`): A Lifemapper BorgScribe object used for
                querying the database
        """
        scribe_count = scribe.countOccurrenceSets()
        service_count = public_client.deserialize(
            public_client.count_occurrence_sets())
        assert scribe_count == service_count
        assert scribe_count >= 0
    
    # ............................
    def test_count_occurrence_sets_with_parameters(self, public_client,
                                                   scribe):
        """Tests the count service with the default parameters

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            scribe (:obj:`BorgScribe`): A Lifemapper BorgScribe object used for
                querying the database
        """
        scribe_count = scribe.countOccurrenceSets(minOccurrenceCount=10)
        service_count = public_client.deserialize(
            public_client.count_occurrence_sets(minimumNumberOfPoints=10))
        assert scribe_count == service_count
        assert scribe_count >= 0

    # ............................
    def test_delete_occ_user_owns(self, public_client, scribe):
        """Tests that user can delete one of their occurrence sets

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            scribe (:obj:`BorgScribe`): A Lifemapper BorgScribe object used for
                querying the database
        """
        dummy_occ = OccurrenceLayer('dummy test occ', PUBLIC_USER, 4326, 1)
        inserted_dummy_occ = scribe.findOrInsertOccurrenceSet(dummy_occ)

        test_occ_id = inserted_dummy_occ.getId()

        try:
            resp = public_client.delete_occurrence_set(test_occ_id)
            resp_status = resp.code
        except Exception as e_info:
            # If there was a failure, try to delete the dummy occ with the
            #    scribe to clean things up
            scribe.deleteObject(inserted_dummy_occ)
            print(str(e_info))
            raise e_info
        assert resp_status == HTTPStatus.NO_CONTENT
        # Check that test occurrence set was deleted
        assert scribe.getOccurrenceSet(occId=test_occ_id) is None

    # ............................
    def test_delete_occ_user_owns_with_children(self, public_client, scribe):
        """Tests that user can delete occurrence set and dependants

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            scribe (:obj:`BorgScribe`): A Lifemapper BorgScribe object used for
                querying the database
        """
        dummy_occ = OccurrenceLayer('dummy test occ', PUBLIC_USER, 4326, 1)
        inserted_dummy_occ = scribe.findOrInsertOccurrenceSet(dummy_occ)

        test_occ_id = inserted_dummy_occ.getId()

        scn_id = scribe.listScenarios(0, 1)[0].id
        scn = scribe.getScenario(scn_id)

        algo = Algorithm('ATT_MAXENT')
        algo.fillWithDefaults()
        prj = SDMProjection(inserted_dummy_occ, algo, scn, scn)
        inserted_dummy_prj = scribe.findOrInsertSDMProject(prj)
        
        test_prj_id = inserted_dummy_prj.getId()

        try:
            resp = public_client.delete_occurrence_set(test_occ_id)
            resp_status = resp.code
        except Exception as e_info:
            # If there was a failure, try to delete the dummy objects with the
            #    scribe to clean things up
            scribe.deleteObject(inserted_dummy_prj)
            scribe.deleteObject(inserted_dummy_occ)
            print(str(e_info))
            raise e_info
        assert resp_status == HTTPStatus.NO_CONTENT
        # Check that test occurrence set was deleted
        assert scribe.getOccurrenceSet(occId=test_occ_id) is None
        assert scribe.getSDMProject(test_prj_id) is None

    # ............................
    def test_delete_occ_does_not_exist(self, public_client):
        """Tests that user cannot delete occurrence set that does not exist

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.
            scribe (:obj:`BorgScribe`): A Lifemapper BorgScribe object used for
                querying the database
        """
        bad_occ_id = 999999999
        with pytest.raises(urllib2.HTTPError) as e_info:
            public_client.delete_occurrence_set(bad_occ_id)
            assert e_info.code == HTTPStatus.NOT_FOUND
    
    # Get
    # List




