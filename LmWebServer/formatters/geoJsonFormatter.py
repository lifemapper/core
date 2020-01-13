"""Module functions for converting object to GeoJSON
"""
import cherrypy
import json
import ogr

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmCommon.common.matrix import Matrix

from LmServer.base.layer2 import Vector
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.shapegrid import ShapeGrid

# .............................................................................
def right_hand_rule(coordinates):
    """
    @summary: Converts the coordinates to right hand rule to meet GeoJSON spec
    @todo: Handle cases that are not simply reversed polygon coordinates
    @note: Coordinates will be a list of polygons lists where each is a list of 
                 x,y lists
    """
    for i in range(len(coordinates)):
        coordinates[i].reverse()

# .............................................................................
def geoJsonify_flo(flo, shpFilename, matrix=None, mtxJoinAttrib=None, 
                                ident=3, headerLookupFilename=None, 
                                transform=lambda x: x):
    """
    @summary: A string generator for matrix GeoJSON
    """
    if isinstance(ident, int):
        ident = ' '*ident
    
    flo.write('{\n')
    flo.write('{}"type" : "FeatureCollection",\n'.format(ident))
    if headerLookupFilename:
        flo.write('{}"propertyLookupFilename" : "{}",\n'.format(
            ident, headerLookupFilename))
    flo.write('{}"features" : [\n'.format(ident))
    
    rowLookup = {}
    
    if matrix is not None:
        colHeaders = matrix.getColumnHeaders()
        colEnum = [(j, str(k)) for j, k in enumerate(colHeaders)]
        rowHeaders = matrix.getRowHeaders()
        
        for i in range(len(rowHeaders)):
            rowLookup[rowHeaders[i][mtxJoinAttrib]] = i
        
        # Define cast function, necessary if matrix if full of booleans
        if matrix.data.dtype == bool:
            castFunc = lambda x: int(x)
        else:
            castFunc = lambda x: x
            
    # Build features list
    drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
    ds = drv.Open(shpFilename, 0)
    lyr = ds.GetLayer()
    
    # Get number of features
    num_feats = lyr.GetFeatureCount()
    x = 0
    
    for feat in lyr:
        x += 1
        # Get the GeoJSON for the feature
        ft = json.loads(feat.ExportToJson())
        right_hand_rule(ft['geometry']['coordinates'])
        joinAttrib = feat.GetFID()
        # TODO: Remove this if updated library adds first id correctly
        ft['id'] = feat.GetFID()
        
        # Join matrix attributes
        if joinAttrib in rowLookup:
            i = rowLookup[joinAttrib]
            
            # Set data or individuals
            if headerLookupFilename:
                ft['properties'] = {
                    'data' : transform(
                        [castFunc(j.item()) for j in matrix.data[i]])
                    }
            else:
                ft['properties'] = dict(
                    [(k, transform(
                        castFunc(
                            matrix.data[i,j].item()))) for j, k in colEnum])
            # Need to conditionally write comma
            if x >= num_feats:
                flo.write('{}\n'.format(json.dumps(ft)))
            else:
                flo.write('{},\n'.format(json.dumps(ft)))
    ds = None


    flo.write('{}]\n'.format(ident))
    flo.write('}')

# .............................................................................
def geoJsonify(shpFilename, matrix=None, mtxJoinAttrib=None):
    """
    @summary: Creates GeoJSON for the features in a shapefile.  If a matrix is 
                     provided, attempt to join the features contained.
    """
    attLookup = {}
    
    # Build matrix lookup
    if matrix is not None:
        colHeaders = matrix.getColumnHeaders()
        rowHeaders = matrix.getRowHeaders()
        
        # Define a cast function, necessary if the matrix is full of booleans 
        #     because they cannot be encoded correctly for JSON
        if matrix.data.dtype == bool:
            castFunc = lambda x: int(x)
        else:
            castFunc = lambda x: x
        
        for i in range(len(rowHeaders)):
            
            joinAtt = rowHeaders[i][mtxJoinAttrib]
            
            attLookup[joinAtt] = {}
            
            for j in range(len(colHeaders)):
                try:
                    attLookup[joinAtt][colHeaders[j]
                                       ] = castFunc(matrix.data[i,j].item())
                except:
                    pass
    
    # Build features list
    features = []
    drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
    ds = drv.Open(shpFilename, 0)
    lyr = ds.GetLayer()
    for feat in lyr:
        # Get the GeoJSON for the feature
        ft = json.loads(feat.ExportToJson())
        # TODO: Remove this if updated library adds first id correctly
        ft['id'] = feat.GetFID()
        right_hand_rule(ft['geometry']['coordinates'])
        joinAttrib = feat.GetFID()
        
        # Join matrix attributes
        if joinAttrib in attLookup:
            if 'properties' not in ft:
                ft['properties'] = {}
            ft['properties'].update(attLookup[joinAttrib])
        
        # Add feature to features list
        features.append(ft)
    ds = None

    doc = {
        'type' : 'FeatureCollection',
        'features' : features
    }

    return doc

# .............................................................................
def geoJsonObjectFormatter(obj):
    """
    @summary: Looks at object and converts to JSON based on its type
    """
    response = _formatObject(obj)
    
    return json.dumps(response, indent=3)

# .............................................................................
def _formatObject(obj):
    """
    @summary: Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'] = LMFormat.GEO_JSON.getMimeType()
    if isinstance(obj, (OccurrenceLayer, ShapeGrid, Vector)):
        cherrypy.response.headers[
            'Content-Disposition'
            ] = 'attachment; filename="{}.geojson"'.format(obj.name)
        return geoJsonify(obj.getDLocation())
    elif isinstance(obj, LMMatrix):
        if obj.matrixType in (
            MatrixType.PAM, MatrixType.ROLLING_PAM, MatrixType.ANC_PAM,
            MatrixType.SITES_COV_OBSERVED, MatrixType.SITES_COV_RANDOM,
            MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM):
            
            sg = obj.getGridset().getShapegrid()
            mtx = Matrix.load(obj.getDLocation())
            cherrypy.response.headers[
                'Content-Disposition'
                ] = 'attachment; filename="mtx_{}.geojson"'.format(obj.getId())
            return geoJsonify(sg.getDLocation(), matrix=mtx, mtxJoinAttrib=0)
        else:
            raise TypeError(
                'Cannot format matrix type: {}'.format(obj.matrixType))
    else:
        raise TypeError("Cannot format object of type: {}".format(type(obj)))

