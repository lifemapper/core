"""Module functions for converting object to EML
"""
import os

import cherrypy
from lmpy import Matrix

from LmCommon.common.lm_xml import Element, SubElement, tostring
from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmServer.base.layer import Raster, Vector
from LmServer.legion.env_layer import EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.sdm_proj import SDMProjection


# .............................................................................
def _create_data_table_section(data_table):
    """Create a data table subsection for an object

    Args:
        data_table (:obj: `Matrix`): A matrix object formatted as CSV
    """
    dt_el = Element(
        'otherEntity', attrib={'id': 'mtx_{}'.format(data_table.get_id())})
    SubElement(dt_el, 'entityName', value='mtx_{}'.format(data_table.get_id()))
    phys = SubElement(dt_el, 'physical')
    SubElement(
        phys, 'objectName', value='mtx_{}.csv'.format(data_table.get_id()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(
            SubElement(phys, 'dataFormat'), 'externallyDefinedFormat'),
        'formatName', value='Lifemapper Matrix Json')

    att_list_el = SubElement(dt_el, 'attributeList')
    mtx = Matrix.load(data_table.get_dlocation())
    for col_header in mtx.get_column_headers():
        SubElement(att_list_el, 'attribute', value=col_header)
    return dt_el


# .............................................................................
def _create_other_entity(entity):
    """Create an 'otherEntity' subsection for an object

    Args:
        entity (:obj: `Gridset`): A gridset object to format as EML
    """
    entity_element = Element(
        'otherEntity', attrib={'id': 'mtx_{}'.format(entity.get_id())})
    SubElement(
        entity_element, 'entityName', value='mtx_{}'.format(entity.get_id()))
    phys = SubElement(entity_element, 'physical')
    SubElement(phys, 'objectName', value='tree_{}.nex'.format(entity.get_id()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(SubElement(phys, 'dataFormat'), 'externallyDefinedFormat'),
        'formatName', value='nexus')
    SubElement(entity_element, 'entityType', value='tree')
    return entity_element


# .............................................................................
def _create_spatial_raster(spatial_raster):
    """Create a 'spatialRaster' subsection for an object
    """
    sr_element = Element('spatialRaster')
    SubElement(
        sr_element, 'cellSizeXDirection', value=str(spatial_raster.resolution))
    SubElement(
        sr_element, 'cellSizeYDirection', value=str(spatial_raster.resolution))
    SubElement(sr_element, 'numberOfBands', value='1')
    min_x, min_y, max_x, max_y = spatial_raster.bbox
    num_cols = int((max_x - min_x) / spatial_raster.resolution)
    num_rows = int((max_y - min_y) / spatial_raster.resolution)
    SubElement(sr_element, 'rasterOrigin', value='Lower Left')
    SubElement(sr_element, 'rows', value=str(num_rows))
    SubElement(sr_element, 'cols', value=str(num_cols))
    SubElement(sr_element, 'verticals', value='1')
    SubElement(sr_element, 'cellGeometry', value='pixel')
    return sr_element


# .............................................................................
def _create_spatial_vector(spatial_vector):
    """Create a 'spatialVector' subsection for an object

    Args:
        spatial_vector (:obj: `Vector`): A vector object to format as EML
    """
    vector_id = 'mtx_{}'.format(spatial_vector.get_id())
    sv_element = Element('spatialVector', attrib={'id': vector_id})
    phys = SubElement(sv_element, 'physical')
    SubElement(
        phys, 'objectName', value='mtx_{}.geojson'.format(
            spatial_vector.get_id()))
    SubElement(phys, 'encodingMethod', value='ASCII')
    SubElement(
        SubElement(
            SubElement(phys, 'dataFormat'),
            'externallyDefinedFormat'), 'formatName', value='geojson')

    attrib_list_element = SubElement(sv_element, 'attributeList')
    if isinstance(spatial_vector, LMMatrix):
        mtx = Matrix.load(spatial_vector.get_dlocation())
        for col_header in mtx.get_column_headers():
            SubElement(attrib_list_element, 'attribute', value=col_header)
        SubElement(sv_element, 'geometry', value='polygon')
    else:
        for _, val in list(spatial_vector.feature_attributes.items()):
            SubElement(attrib_list_element, 'attribute', value=val[0])
        SubElement(sv_element, 'geometry', value='polygon')

    return sv_element


# .............................................................................
def make_eml(my_obj):
    """Generate an EML document representing metadata for the provided object
    """
    # TODO: Add name
    if isinstance(my_obj, Gridset):
        top_el = Element(
            'eml',
            attrib={
                # TODO: Better package ids
                'packageId': 'org.lifemapper.gridset.{}'.format(
                    my_obj.get_id()),
                'system': 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            top_el, 'dataset', attrib={'id': 'gridset_{}'.format(
                my_obj.get_id())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'), 'organizationName',
            value='Lifemapper')

        try:
            ds_name = my_obj.name
        except AttributeError:
            ds_name = 'Gridset {}'.format(my_obj.get_id())

        SubElement(ds_el, 'name', value=ds_name)

        for mtx in my_obj.get_matrices():
            if os.path.exists(mtx.get_dlocation()):
                # TODO: Enable GRIMs
                if mtx.matrix_type in [
                        MatrixType.ANC_PAM,  # MatrixType.GRIM,
                        MatrixType.PAM, MatrixType.SITES_OBSERVED]:
                    ds_el.append(_create_spatial_vector(mtx))
                elif mtx.matrix_type in [
                        MatrixType.ANC_STATE, MatrixType.DIVERSITY_OBSERVED,
                        MatrixType.MCPA_OUTPUTS, MatrixType.SPECIES_OBSERVED]:
                    ds_el.append(_create_data_table_section(mtx))
        if my_obj.tree is not None:
            ds_el.append(_create_other_entity(my_obj.tree))
    elif isinstance(my_obj, SDMProjection):
        top_el = Element(
            'eml',
            attrib={
                'packageId': 'org.lifemapper.sdmproject.{}'.format(
                    my_obj.get_id()),
                'system': 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            top_el, 'dataset',
            attrib={'id': 'sdmproject_{}'.format(my_obj.get_id())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_raster(my_obj))
    elif isinstance(my_obj, EnvLayer):
        top_el = Element(
            'eml',
            attrib={
                'packageId': 'org.lifemapper.envlayer.{}'.format(
                    my_obj.get_id()),
                'system': 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            top_el, 'dataset',
            attrib={'id': 'envlayer_{}'.format(my_obj.get_id())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_raster(my_obj))
    elif isinstance(my_obj, Raster):
        top_el = Element(
            'eml',
            attrib={
                'packageId': 'org.lifemapper.layer.{}'.format(my_obj.get_id()),
                'system': 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            top_el, 'dataset',
            attrib={'id': 'layer_{}'.format(my_obj.get_id())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_raster(my_obj))
    elif isinstance(my_obj, Vector):
        top_el = Element(
            'eml',
            attrib={
                'packageId': 'org.lifemapper.layer.{}'.format(my_obj.get_id()),
                'system': 'http://svc.lifemapper.org'})
        ds_el = SubElement(
            top_el, 'dataset',
            attrib={'id': 'layer_{}'.format(my_obj.get_id())})
        # Contact
        SubElement(
            SubElement(ds_el, 'contact'),
            'organizationName', value='Lifemapper')
        ds_el.append(_create_spatial_vector(my_obj))
    else:
        raise Exception(
            'Cannot create eml for {} currently'.format(my_obj.__class__))
    return top_el


# .............................................................................
def eml_object_formatter(obj):
    """Looks at object and converts to EML based on its type
    """
    response = _format_object(obj)

    return tostring(response)


# .............................................................................
def _format_object(obj):
    """Helper method to format an individual object based on its type
    """
    cherrypy.response.headers['Content-Type'] = LMFormat.EML.get_mime_type()

    if isinstance(obj, (EnvLayer, Gridset, SDMProjection, Raster, Vector)):
        cherrypy.response.headers[
            'Content-Disposition'] = 'attachment; filename="{}.eml"'.format(
                obj.name)
        return make_eml(obj)

    raise TypeError("Cannot format object of type: {}".format(type(obj)))
