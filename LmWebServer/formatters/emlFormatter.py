"""Module functions for converting object to EML
"""
import cherrypy

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmCommon.common.matrix import Matrix
from LmCommon.common.lmXml import Element, SubElement, tostring

from LmServer.legion.gridset import Gridset
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.envlayer import EnvLayer

# .............................................................................
def _create_data_table_section(data_table):
    """Create a data table subsection for an object

    Args:
        data_table (:obj: `Matrix`): A matrix object formatted as CSV
    """
    dt_el = Element(
        'otherEntity', attrib={'id' : 'mtx_{}'.format(data_table.getId())})
    SubElement(dt_el, 'entityName', value='mtx_{}'.format(data_table.getId()))
    phys = SubElement(dt_el, 'physical')
    SubElement(
        phys, 'objectName', value='mtx_{}.csv'.format(data_table.getId()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(
            SubElement(phys, 'dataFormat'), 
            'externallyDefinedFormat'),
        'formatName', 
        value='Lifemapper Matrix Json')
    
    alEl = SubElement(dt_el, 'attributeList')
    mtx = Matrix.load(data_table.getDLocation())
    for colHeader in mtx.getColumnHeaders():
        SubElement(alEl, 'attribute', value=colHeader)
    return dt_el

# .............................................................................
def _create_other_entity(entity):
    """Create an 'otherEntity' subsection for an object

    Args:
        entity (:obj: `Gridset`): A gridset object to format as EML
    """
    entity_element = Element(
        'otherEntity', attrib={'id' : 'mtx_{}'.format(entity.getId())})
    SubElement(
        entity_element, 'entityName', value='mtx_{}'.format(entity.getId()))
    phys = SubElement(entity_element, 'physical')
    SubElement(phys, 'objectName', value='tree_{}.nex'.format(entity.getId()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(
            SubElement(phys, 'dataFormat'), 
            'externallyDefinedFormat'),
        'formatName', 
        value='nexus')
    SubElement(entity_element, 'entityType', value='tree')
    return entity_element

# .............................................................................
def _create_spatial_raster(spatial_raster):
    """Create a 'spatialRaster' subsection for an object
    """
    sr_element = Element('spatialRaster')
    SubElement(
        sr_element, 'cellSizeXDirection', value=spatial_raster.resolution)
    SubElement(
        sr_element, 'cellSizeYDirection', value=spatial_raster.resolution)
    SubElement(
        sr_element, 'numberOfBands', value='1')
    min_x, min_y, max_x, max_y = spatial_raster.bbox
    num_cols = int((max_x - min_x) / spatial_raster.resolution)
    num_rows = int((max_y - min_y) / spatial_raster.resolution)
    SubElement(sr_element, 'rasterOrigin', value='Lower Left')
    SubElement(sr_element, 'rows', value=str(num_rows))
    SubElement(sr_element, 'cols', value=str(num_cols))
    SubElement(
        sr_element, 'verticals', value='1')
    SubElement(
        sr_element, 'cellGeometry', value='pixel')
    return sr_element

# .............................................................................
def _create_spatial_vector(spatial_vector):
    """Create a 'spatialVector' subsection for an object

    Args:
        spatial_vector (:obj: `Vector`): A vector object to format as EML
    """
    vector_id = 'mtx_{}'.format(spatial_vector.getId())
    sv_element = Element('spatialVector', attrib={'id' : vector_id})
    phys = SubElement(sv_element, 'physical')
    SubElement(
        phys, 'objectName', value='mtx_{}.geojson'.format(
            spatial_vector.getId()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(
            SubElement(phys, 'dataFormat'),
            'externallyDefinedFormat'), 'formatName', value='geojson')
    
    attrib_list_element = SubElement(sv_element, 'attributeList')
    mtx = Matrix.load(spatial_vector.getDLocation())
    for colHeader in mtx.getColumnHeaders():
        SubElement(attrib_list_element, 'attribute', value=colHeader)
        
    SubElement(sv_element, 'geometry', value='polygon')
    
    return sv_element

# .............................................................................
def makeEml(my_obj):
    """
    @summary: Generate an EML document representing metadata for the provided 
                     object
    """
    # TODO: Add name
    if isinstance(my_obj, Gridset):
        topEl = Element('eml', 
                             attrib={
                                 # TODO: Better package ids
                                'packageId' : 'org.lifemapper.gridset.{}'.format(
                                    my_obj.getId()),
                                'system' : 'http://svc.lifemapper.org'
                              })
        dsEl = SubElement(
            topEl, 'dataset', attrib={'id' : 'gridset_{}'.format(
                my_obj.getId())})
        # Contact
        SubElement(
            SubElement(dsEl, 'contact'), 'organizationName',
            value='Lifemapper')
        
        try:
            dsName = my_obj.name
        except:
            dsName = 'Gridset {}'.format(my_obj.getId())
        
        SubElement(dsEl, 'name', value=dsName)
        
        for mtx in my_obj.getMatrices():
            # TODO: Enable GRIMs
            if mtx.matrixType in [
                MatrixType.ANC_PAM, #MatrixType.GRIM,
                MatrixType.PAM, MatrixType.SITES_OBSERVED]:
                dsEl.append(_create_spatial_vector(mtx))
            elif mtx.matrixType in [MatrixType.ANC_STATE, 
                                            MatrixType.DIVERSITY_OBSERVED, 
                                            MatrixType.MCPA_OUTPUTS,
                                            MatrixType.SPECIES_OBSERVED]:
                dsEl.append(_create_data_table_section(mtx))
        if my_obj.tree is not None:
            dsEl.append(_create_other_entity(my_obj.tree))
    elif isinstance(my_obj, SDMProjection):
        topEl = Element(
            'eml',
            attrib={
                'packageId' : 'org.lifemapper.sdmproject.{}'.format(
                    my_obj.getId()),
                'system' : 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            topEl, 'dataset',
            attrib={'id' : 'sdmproject_{}'.format(my_obj.getId())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_raster(my_obj))
    elif isinstance(my_obj, EnvLayer):
        topEl = Element(
            'eml',
            attrib={
                'packageId' : 'org.lifemapper.envlayer.{}'.format(
                    my_obj.getId()),
                'system' : 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            topEl, 'dataset',
            attrib={'id' : 'envlayer_{}'.format(my_obj.getId())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_raster(my_obj))
    else:
        raise Exception(
            'Cannot create eml for {} currently'.format(my_obj.__class__))
    return topEl
    
# .............................................................................
def emlObjectFormatter(obj):
    """
    @summary: Looks at object and converts to EML based on its type
    """
    response = _formatObject(obj)
    
    return tostring(response)

# .............................................................................
def _formatObject(obj):
    """
    @summary: Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'] = LMFormat.EML.getMimeType()
    
    if isinstance(obj, Gridset):
        cherrypy.response.headers['Content-Disposition'] = 'attachment; filename="{}.eml"'.format(obj.name)
        return makeEml(obj)
    else:
        raise TypeError, "Cannot format object of type: {}".format(type(obj))

