"""Determine how and where to dispatch a web service request

The Lifemapper dispatcher looks for a format as the last portion of the
path_info string from the request.  If one is found, it is removed from
path_info and the accept header is updated.  This dispatcher also sets the
path_info string to lower case so that the services can be case insensitive.
Finally, forward to the MethodDispatcher base class
"""
import cherrypy
from cherrypy._cpdispatch import MethodDispatcher

from LmCommon.common.lmconstants import (
    CSV_INTERFACE, EML_INTERFACE, GEO_JSON_INTERFACE, GEOTIFF_INTERFACE,
    JSON_INTERFACE, KML_INTERFACE, LMFormat, NEXUS_INTERFACE, NEWICK_INTERFACE,
    PACKAGE_INTERFACE, PROGRESS_INTERFACE, SHAPEFILE_INTERFACE)


# .............................................................................
class LmDispatcher(MethodDispatcher):
    """MethodDispatcher subclass that sets accept headers based on url
    """

    # ...........................
    def __call__(self, path_info):

        path_info_pieces = path_info.lower().strip('/').split('/')
        new_path_info = path_info.lower()
        set_accept = None
        last_segment = path_info_pieces[-1]

        if last_segment == JSON_INTERFACE:
            set_accept = LMFormat.JSON.get_mime_type()
        elif last_segment == GEO_JSON_INTERFACE:
            set_accept = LMFormat.GEO_JSON.get_mime_type()
        elif last_segment == CSV_INTERFACE:
            set_accept = LMFormat.CSV.get_mime_type()
        elif last_segment == KML_INTERFACE:
            set_accept = LMFormat.KML.get_mime_type()
        elif last_segment == GEOTIFF_INTERFACE:
            set_accept = LMFormat.GTIFF.get_mime_type()
        elif last_segment == SHAPEFILE_INTERFACE:
            set_accept = LMFormat.SHAPE.get_mime_type()
        elif last_segment == EML_INTERFACE:
            set_accept = LMFormat.EML.get_mime_type()
        elif last_segment == PACKAGE_INTERFACE:
            set_accept = LMFormat.ZIP.get_mime_type()
        elif last_segment == NEXUS_INTERFACE:
            set_accept = LMFormat.NEXUS.get_mime_type()
        elif last_segment == NEWICK_INTERFACE:
            set_accept = LMFormat.NEWICK.get_mime_type()
        elif last_segment == PROGRESS_INTERFACE:
            set_accept = LMFormat.PROGRESS.get_mime_type()
            # Last one is stripped away but needed for formatting
            path_info_pieces.append(PROGRESS_INTERFACE)

        if set_accept is not None:
            cherrypy.request.headers['Accept'] = set_accept

            new_path_info = '/'.join(path_info_pieces[:-1])

        return MethodDispatcher.__call__(self, new_path_info)
