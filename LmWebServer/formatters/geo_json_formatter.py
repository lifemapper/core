"""Module functions for converting object to GeoJSON
"""
import json

import cherrypy
import ogr

from lmpy import Matrix

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmServer.base.layer import Vector
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.shapegrid import Shapegrid


# .............................................................................
def identity_func(value):
    """Return the value provided
    """
    return value


# .............................................................................
def right_hand_rule(coordinates):
    """Converts the coordinates to right hand rule to meet GeoJSON spec

    Todo:
        Handle cases that are not simply reversed polygon coordinates

    Note:
        Coordinates will be a list of polygons lists where each is a list of
            x,y lists
    """
    # Reverse each item in coordinates list
    for coords in coordinates:
        coords.reverse()


# .............................................................................
def geo_jsonify_flo(flo, shp_file_name, matrix=None, mtx_join_attrib=None,
                    ident=4, header_lookup_filename=None,
                    transform=identity_func):
    """
    @summary: A string generator for matrix GeoJSON
    """
    if isinstance(ident, int):
        ident = ' ' * ident

    flo.write('{\n')
    flo.write('{}"type" : "FeatureCollection",\n'.format(ident))
    if header_lookup_filename:
        flo.write('{}"propertyLookupFilename" : "{}",\n'.format(
            ident, header_lookup_filename))
    flo.write('{}"features" : [\n'.format(ident))

    row_lookup = {}

    if matrix is not None:
        col_headers = matrix.get_column_headers()
        col_enum = [(j, str(k)) for j, k in enumerate(col_headers)]
        row_headers = matrix.get_row_headers()

        for i, row_hdr in enumerate(row_headers):
            row_lookup[row_hdr[mtx_join_attrib]] = i

        # Define cast function, necessary if matrix if full of booleans
        if matrix.dtype == bool:
            cast_func = int
        else:
            cast_func = identity_func

    # Build features list
    drv = ogr.GetDriverByName(LMFormat.get_default_ogr().driver)
    dataset = drv.Open(shp_file_name, 0)
    lyr = dataset.GetLayer()

    # Get number of features
    num_feats = lyr.GetFeatureCount()
    count = 0

    for feat in lyr:
        count += 1
        # Get the GeoJSON for the feature
        feat_json = json.loads(feat.ExportToJson())
        right_hand_rule(feat_json['geometry']['coordinates'])
        join_attrib = feat.GetFID()
        # TODO: Remove this if updated library adds first id correctly
        feat_json['id'] = feat.GetFID()

        # Join matrix attributes
        if join_attrib in row_lookup:
            i = row_lookup[join_attrib]

            # Set data or individuals
            if header_lookup_filename:
                feat_json['properties'] = {
                    'data': transform(
                        [cast_func(j.item()) for j in matrix[i]])
                    }
            else:
                feat_json['properties'] = {
                    k: transform(cast_func(matrix[i, j].item())
                                 ) for j, k in col_enum}
            # Need to conditionally write comma
            if count >= num_feats:
                flo.write('{}\n'.format(json.dumps(feat_json)))
            else:
                flo.write('{},\n'.format(json.dumps(feat_json)))
    dataset = None

    flo.write('{}]\n'.format(ident))
    flo.write('}')


# .............................................................................
def geo_jsonify(shp_file_name, matrix=None, mtx_join_attrib=None):
    """Creates GeoJSON for the features in a shapefile.
    """
    att_lookup = {}

    # Build matrix lookup
    if matrix is not None:
        col_headers = matrix.get_column_headers()
        row_headers = matrix.get_row_headers()

        # Define a cast function, necessary if the matrix is full of booleans
        #     because they cannot be encoded correctly for JSON
        if matrix.dtype == bool:
            cast_func = int
        else:
            cast_func = identity_func

        for i, row_hdr in enumerate(row_headers):
            join_att = row_hdr[mtx_join_attrib]
            att_lookup[join_att] = {}

            for j, col_hdr in enumerate(col_headers):
                try:
                    att_lookup[join_att][col_hdr] = cast_func(
                        matrix[i, j].item())
                except Exception:
                    pass

    # Build features list
    features = []
    drv = ogr.GetDriverByName(LMFormat.get_default_ogr().driver)
    dataset = drv.Open(shp_file_name, 0)
    lyr = dataset.GetLayer()
    for feat in lyr:
        # Get the GeoJSON for the feature
        feat_json = json.loads(feat.ExportToJson())
        # TODO: Remove this if updated library adds first id correctly
        feat_json['id'] = feat.GetFID()
        right_hand_rule(feat_json['geometry']['coordinates'])
        join_attrib = feat.GetFID()

        # Join matrix attributes
        if join_attrib in att_lookup:
            if 'properties' not in feat_json:
                feat_json['properties'] = {}
            feat_json['properties'].update(att_lookup[join_attrib])

        # Add feature to features list
        features.append(feat_json)
    dataset = None

    doc = {
        'type': 'FeatureCollection',
        'features': features
    }

    return doc


# .............................................................................
def geo_json_object_formatter(obj):
    """Looks at object and converts to JSON based on its type
    """
    response = _format_object(obj)

    return json.dumps(response, indent=4)


# .............................................................................
def _format_object(obj):
    """Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'
                              ] = LMFormat.GEO_JSON.get_mime_type()
    if isinstance(obj, (OccurrenceLayer, Shapegrid, Vector)):
        cherrypy.response.headers[
            'Content-Disposition'
            ] = 'attachment; filename="{}.geojson"'.format(obj.name)
        return geo_jsonify(obj.get_dlocation())

    if isinstance(obj, LMMatrix):
        if obj.matrix_type in (
                MatrixType.PAM, MatrixType.ROLLING_PAM, MatrixType.ANC_PAM,
                MatrixType.SITES_COV_OBSERVED, MatrixType.SITES_COV_RANDOM,
                MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM):

            shapegrid = obj.get_gridset().get_shapegrid()
            mtx = Matrix.load(obj.get_dlocation())
            cherrypy.response.headers[
                'Content-Disposition'
                ] = 'attachment; filename="mtx_{}.geojson"'.format(
                    obj.get_id())
            return geo_jsonify(
                shapegrid.get_dlocation(), matrix=mtx, mtx_join_attrib=0)

        raise TypeError(
            'Cannot format matrix type: {}'.format(obj.matrix_type))

    raise TypeError('Cannot format object of type: {}'.format(type(obj)))
