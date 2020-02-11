"""This module tests OGC services over HTTP

Note:
    * These tests use the public_client library (filled in by pytest) to retrieve
        objects and then make OGC requests

Todo:
    * Randomly select from lists
    * Use constants or some sort of response processing
"""
from LmCommon.common.lmconstants import JobStatus


# .............................................................................
class Test_ogc_services_public(object):
    """Tests OGC services over HTTP with public data
    """
    # .....................................
    def test_occurrence_map(self, public_client):
        """Test that a map can be retrieved for a public occurrence set

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.

        Todo:
            * Use constants of response processing
            * Attempt to load image
            * Use multiple image formats
        """
        occ_list = public_client.deserialize(
            public_client.list_occurrence_sets(status=JobStatus.COMPLETE))
        occ_id = occ_list[0]['id']
        occ = public_client.deserialize(
            public_client.get_occurrence_set(occ_id))

        map_name = occ['map_name']
        bbox = ','.join([str(i) for i in occ['spatialVector']['bbox']])
        color = '#00ffff'
        layer = occ['layerName']
        height = 200
        width = 400
        epsg = occ['spatialVector']['epsg']
        
        ogc_flo = public_client.get_ogc(
            map_name, bbox=bbox, color=color, height=height, layer=layer,
            request='GetMap', format='image/png', service='WMS',
            version='1.1.0', width=width, srs='EPSG:{}'.format(epsg))
        image_data = ogc_flo.read()
        ogc_flo.close()

    # .....................................
    def test_projection_data(self, public_client):
        """Test that a raster can be retrieved for a public projection

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.

        Todo:
            * Use constants of response processing
            * Attempt to load raster
            * Use multiple image formats
        """
        prj_list = public_client.deserialize(
            public_client.list_sdm_projections(status=JobStatus.COMPLETE))
        prj_id = prj_list[0]['id']
        prj = public_client.deserialize(
            public_client.get_sdm_projection(prj_id))

        map_name = prj['map_name']
        bbox = ','.join([str(i) for i in prj['spatialRaster']['bbox']])
        layer = prj['layerName']
        epsg = prj['spatialRaster']['epsg']
        
        ogc_flo = public_client.get_ogc(
            map_name, bbox=bbox, coverage=layer, request='GetCoverage',
            format='image/tiff', service='WCS', version='1.1.0',
            crs='EPSG:{}'.format(epsg))
        image_data = ogc_flo.read()
        ogc_flo.close()

    # .....................................
    def test_projection_map(self, public_client):
        """Test that a map can be retrieved for a public projection

        Args:
            public_client (:obj:`LmWebclient`): A Lifemapper web service client
                instance for the public user.  This will be provided via
                pytest.

        Todo:
            * Use constants of response processing
            * Attempt to load image
            * Use multiple image formats
        """
        prj_list = public_client.deserialize(
            public_client.list_sdm_projections(status=JobStatus.COMPLETE))
        prj_id = prj_list[0]['id']
        prj = public_client.deserialize(
            public_client.get_sdm_projection(prj_id))

        map_name = prj['map_name']
        bbox = ','.join([str(i) for i in prj['spatialRaster']['bbox']])
        color = '#00ffff'
        layer = prj['layerName']
        height = 200
        width = 400
        epsg = prj['spatialRaster']['epsg']
        
        ogc_flo = public_client.get_ogc(
            map_name, bbox=bbox, color=color, height=height, layer=layer,
            request='GetMap', format='image/png', service='WMS',
            version='1.1.0', width=width, srs='EPSG:{}'.format(epsg))
        image_data = ogc_flo.read()
        ogc_flo.close()
