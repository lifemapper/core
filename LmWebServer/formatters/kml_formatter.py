"""Module containing KML formatter clas

Note:
    This needs to be cleaned up a lot.  This is a patch to get us through
        until we can spend some time on this
"""
import cherrypy
from osgeo import ogr

from LmCommon.common.lm_xml import (
    CDATA, Element, register_namespace, set_default_namespace, SubElement,
    tostring)
from LmCommon.common.lmconstants import LMFormat
from LmServer.base.utilities import format_time_human
from LmServer.common.lmconstants import OccurrenceFieldNames, WEBSERVICES_ROOT
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.sdm_proj import SDMProjection

KML_NS = "http://www.opengis.net/kml/2.2"


# .............................................................................
def add_occurrence_set(parent, occ):
    """Adds an SDM Occurrence Set to the KML output

    Args:
        parent: The parent element to add it to
        occ: The occurrence set object to add
    """
    SubElement(
        parent, 'name', value='{} points (Occ Id: {})'.format(
            occ.display_name, occ.get_id()))
    SubElement(parent, 'open', value='1')
    SubElement(
        parent, 'description', value='{} points (Occ Id: {})'.format(
            occ.display_name, occ.get_id()))

    # TODO: Look at feature attributes and decide what to read
    for point in occ.features:
        add_point(parent, point)


# .............................................................................
def add_point(parent, point):
    """Create a point subelement
    """
    name = get_name_for_point(point)
    lat, lon = get_lat_lon_for_point(point)

    placemark_el = SubElement(parent, 'Placemark')
    SubElement(placemark_el, 'name', value=name)
    SubElement(placemark_el, 'styleUrl', value='#lmUserOccurrenceBalloon')

    point_el = SubElement(placemark_el, 'Point')
    SubElement(point_el, 'coordinates', value='{},{},0'.format(lon, lat))

    ext = SubElement(placemark_el, 'ExtendedData')
    lat_el = SubElement(ext, 'Data', attrib={'name': 'latitude'})
    SubElement(lat_el, 'value', value=lat)

    lon_el = SubElement(ext, 'Data', attrib={'name': 'longitude'})
    SubElement(lon_el, 'value', value=lon)


# .............................................................................
def add_projection(parent, prj, visibility, indent=0):
    """Adds a projection to the KML output

    Args:
        parent: The parent element to add it to
        point: The projection to add
    """
    prj_name = 'Lifemapper projection {} - {}'.format(
        prj.get_id(), prj.speciesName)
    if indent == 0:
        SubElement(parent, 'name', value=prj_name)
        SubElement(parent, 'description', value=prj_name)

    # Ground Overlay
    ground_overlay_el = SubElement(parent, 'GroundOverlay')
    SubElement(ground_overlay_el, 'styleUrl', value='#lmProjectionBalloon')
    SubElement(ground_overlay_el, 'name', value=prj_name)
    SubElement(ground_overlay_el, 'visibility', value=visibility)

    # Look at
    look_at_el = SubElement(ground_overlay_el, 'LookAt')
    SubElement(look_at_el, 'latitude', value='0.0')
    SubElement(look_at_el, 'longitude', value='0.0')
    SubElement(look_at_el, 'altitude', value='0.0')
    SubElement(look_at_el, 'range', value='500000')
    SubElement(look_at_el, 'tilt', value='0.0')
    SubElement(look_at_el, 'heading', value='0.0')

    # Icon
    icon_el = SubElement(ground_overlay_el, 'Icon')

    map_url = prj._earl_jr.construct_lm_map_request(
        '{}/{}{}'.format(WEBSERVICES_ROOT, 'api/v2/ogc', prj._map_prefix),
        400, 200, prj.bbox, color='ff0000')
    SubElement(icon_el, 'href', value=map_url)

    # Latitude Longitude Box
    lat_lon_box_el = SubElement(ground_overlay_el, 'LatLonBox')
    SubElement(lat_lon_box_el, 'north', value=prj.bbox[3])
    SubElement(lat_lon_box_el, 'south', value=prj.bbox[1])
    SubElement(lat_lon_box_el, 'west', value=prj.bbox[0])
    SubElement(lat_lon_box_el, 'east', value=prj.bbox[2])
    SubElement(lat_lon_box_el, 'rotation', value='0.0')

    # Extended Data
    ext_data = SubElement(ground_overlay_el, 'ExtendedData')

    last_mod_el = SubElement(ext_data, 'Data', attrib={'name': 'lastModified'})
    SubElement(last_mod_el, 'value', value=format_time_human(prj.mod_time))

    scn_title_el = SubElement(
        ext_data, 'Data', attrib={'name': 'scenarioTitle'})
    # TODO: Get the title for this scenario
    SubElement(scn_title_el, 'value', value=prj._proj_scenario.code)


# .............................................................................
def get_kml(my_obj):
    """Gets a KML document for the object

    Args:
        my_obj: The object to return in KML
    """
    register_namespace('', KML_NS)
    set_default_namespace(KML_NS)
    root = Element('kml')
    doc = SubElement(root, 'Document')
    SubElement(doc, 'styleUrl', value='#lmBalloon')

    # lmBalloon style
    lm_balloon = SubElement(doc, 'Style', attrib={'id': 'lmBalloon'})

    # Nested parent elements that don't add extra attributes
    SubElement(SubElement(lm_balloon, 'BalloonStyle'), 'text').append(
        CDATA("""\
                    <table>
                        <tr>
                            <td>
                                <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                            </td>
                            <td>
                                <h3>$[name]</h3>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                $[description]
                            </td>
                        </tr>
                    </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

    # lmLayerBalloon style
    lm_layer_balloon = SubElement(
        doc, 'Style', attrib={'id': 'lmLayerBalloon'})

    # Nested parent elements that don't add extra attributes
    SubElement(SubElement(lm_layer_balloon, 'BalloonStyle'), 'text').append(
        CDATA("""\
                    <table>
                        <tr>
                            <td>
                                <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                            </td>
                            <td>
                                <h3>$[name]</h3>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <table width="300">
                                    <tr>
                                        <th align="right">
                                            Title:
                                        </th>
                                        <td>
                                            $[layerTitle]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Last Modified:
                                        </th>
                                        <td>
                                            $[lastModified]
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

    # lmGbifOccurrenceBalloon
    lm_gbif = SubElement(
        doc, 'Style', attrib={'id': 'lmGbifOccurrenceBalloon'})

    # Nested parent elements that don't add extra attributes
    SubElement(SubElement(lm_gbif, 'BalloonStyle'), 'text').append(CDATA("""\
                    <table>
                        <tr>
                            <td>
                                <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                            </td>
                            <td>
                                <h3>$[name]</h3>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <table width="300">
                                    <tr>
                                        <th align="right">
                                            Provider:
                                        </th>
                                        <td>
                                            $[providerName]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Resource:
                                        </th>
                                        <td>
                                            $[resourceName]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Latitude:
                                        </th>
                                        <td>
                                            $[latitude]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Longitude:
                                        </th>
                                        <td>
                                            $[longitude]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Collector:
                                        </th>
                                        <td>
                                            $[collector]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Collection Date:
                                        </th>
                                        <td>
                                            $[colDate]
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

    # lmUserOccurrenceBalloon
    lm_user = SubElement(
        doc, 'Style', attrib={'id': 'lmUserOccurrenceBalloon'})

    # Nested parent elements that don't add extra attributes
    SubElement(SubElement(lm_user, 'BalloonStyle'), 'text').append(CDATA("""\
                    <table>
                        <tr>
                            <td>
                                <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                            </td>
                            <td>
                                <h3>$[name]</h3>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <table width="300">
                                    <tr>
                                        <th align="right">
                                            Latitude:
                                        </th>
                                        <td>
                                            $[latitude]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Longitude:
                                        </th>
                                        <td>
                                            $[longitude]
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

    # lmProjectionBalloon
    lm_prj = SubElement(doc, 'Style', attrib={'id': 'lmProjectionBalloon'})

    # Nested parent elements that don't add extra attributes
    SubElement(SubElement(lm_prj, 'BalloonStyle'), 'text').append(CDATA("""\
                    <table>
                        <tr>
                            <td>
                                <img src="{WEBSITE}/images/lmlogosmall.jpg" />
                            </td>
                            <td>
                                <h3>$[name]</h3>
                            </td>
                        </tr>
                        <tr>
                            <td colspan="2">
                                <table width="300">
                                    <tr>
                                        <th align="right">
                                            Scenario Title:
                                        </th>
                                        <td>
                                            $[scenarioTitle]
                                        </td>
                                    </tr>
                                    <tr>
                                        <th align="right">
                                            Last Modified:
                                        </th>
                                        <td>
                                            $[lastModified]
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>""".format(WEBSITE=WEBSERVICES_ROOT)))

    # Add object
    if isinstance(my_obj, SDMProjection):
        add_projection(doc, my_obj, 1)
    elif isinstance(my_obj, OccurrenceLayer):
        my_obj.read_shapefile()
        add_occurrence_set(doc, my_obj)

    temp = tostring(root)
    temp = temp.replace('&lt;', '<')
    temp = temp.replace('&gt;', '>')
    return temp


# .............................................................................
def kml_object_formatter(obj):
    """Looks at object and converts to KML based on its type
    """
    # cherrypy.response.headers['Content-Type'] = LMFormat.JSON.getMimeType()
    cherrypy.response.headers['Content-Type'] = LMFormat.KML.get_mime_type()
    cherrypy.response.headers[
        'Content-Disposition'] = 'attachment; filename="{}.kml"'.format(
            obj.name)
    kml_str = get_kml(obj)
    return kml_str


# .............................................................................
def get_name_for_point(point):
    """Get a name for a point.  Try taxon name, falls back to local id

    Args:
        pt: A point object
    """
    name = None

    try:
        return point.sciname
    except AttributeError:
        try:
            return point.occurid
        except AttributeError:
            pass

    for att in OccurrenceFieldNames.DATANAME:
        try:
            name = point.__getattribute__(att)
            return name
        except Exception:
            pass

    # If no data name fields were available
    for att in OccurrenceFieldNames.LOCAL_ID:
        try:
            name = point.__getattribute__(att)
            return name
        except Exception:
            pass

    # Return unknown if we can't find a name
    return 'Unknown'


# .............................................................................
def get_lat_lon_for_point(point):
    """Get's the x and y for a point

    Args:
        point: A point object

    Note:
        Tries to get this from the geometry first, falls back to attributes
    """
    # Try wkt first
    wkt = None
    for att in OccurrenceFieldNames.GEOMETRY_WKT:
        try:
            wkt = point._attrib[att]
            break
        except Exception:
            pass

    if wkt is not None:
        lon, lat, _ = ogr.CreateGeometryFromWkt(wkt).GetPoint()
        return lat, lon

    # Find lat and lon
    lat = None
    for att in OccurrenceFieldNames.LATITUDE:
        try:
            lat = point._attrib[att]
            break
        except Exception:
            pass

    lon = None
    for att in OccurrenceFieldNames.LONGITUDE:
        try:
            lon = point._attrib[att]
            break
        except Exception:
            pass

    if lat is not None and lon is not None:
        return lat, lon

    # Raise exception if we get to here without determining lat and lon
    raise Exception('Could not retrieve latitude and / or longitude for point')


# .............................................................................
def get_local_id_for_point(point):
    """Get a local id for a point.

    Args:
        point: A point object.
    """
    local_id = None

    # If no data name fields were available
    for att in OccurrenceFieldNames.LOCAL_ID:
        try:
            local_id = point.__getattribute__(att)
            return local_id
        except Exception:
            pass

    # Return unknown if we can't find a name
    return 'Unknown'
