"""This tool provides output formatting for service calls based on headers

This module provides a tool for formatting outputs of service calls based on
the accept headers of the request
"""
from LmCommon.common.lmconstants import (
    CSV_INTERFACE, HTTPStatus, JSON_INTERFACE, LMFormat, SHAPEFILE_INTERFACE)
from LmServer.common.lmconstants import SnippetOperations
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.snippet import SnippetShooter
from LmWebServer.formatters.eml_formatter import eml_object_formatter
from LmWebServer.formatters.fileFormatter import (
    csv_object_formatter, file_formatter, gtiff_object_formatter,
    shapefile_object_formatter)
from LmWebServer.formatters.geo_json_formatter import geo_json_object_formatter
from LmWebServer.formatters.json_formatter import json_object_formatter
from LmWebServer.formatters.kml_formatter import kml_object_formatter
from LmWebServer.formatters.package_formatter import gridset_package_formatter
from LmWebServer.formatters.progress_formatter import progress_object_formatter
import cherrypy


# .............................................................................
def lm_formatter(f):
    """Wrapper method for formatting service objects

    Use this as a decorator for methods that return objects that should be sent
    through formatting before being returned
    """
    def wrapper(*args, **kwargs):
        """Wrapper function
        """
        # Call the handler and get the object result
        try:
            handler_result = f(*args, **kwargs)
        except TypeError:
            raise cherrypy.HTTPError(HTTPStatus.BAD_REQUEST, 'Bad request')

        accept_headers = cherrypy.request.headers.get('Accept')

        try:
            raw_headers = accept_headers.split(',')
            valued_accepts = []
            for hdr in raw_headers:
                if len(hdr.split(';')) > 1:
                    mime, val = hdr.split(';')
                    valued_accepts.append(
                        (mime.strip(), float(val.strip('q='))))
                else:
                    valued_accepts.append((h.strip(), 1.0))
        except Exception:
            valued_accepts = [('*/*', 1.0)]

        sorted_accepts = sorted(
            valued_accepts, key=lambda x: x[1], reverse=True)

        for accept_hdr, _ in sorted_accepts:
            try:
                if accept_hdr == LMFormat.GEO_JSON.getMimeType():
                    return geo_json_object_formatter(handler_result)
                # If JSON or default
                if accept_hdr in [LMFormat.JSON.getMimeType(), '*/*']:
                    shoot_snippets(
                        handler_result, SnippetOperations.VIEWED,
                        JSON_INTERFACE)
                    return json_object_formatter(handler_result)
                if accept_hdr == LMFormat.EML.getMimeType():
                    return eml_object_formatter(handler_result)
                if accept_hdr == LMFormat.KML.getMimeType():
                    return kml_object_formatter(handler_result)
                if accept_hdr == LMFormat.GTIFF.getMimeType():
                    return gtiff_object_formatter(handler_result)
                if accept_hdr == LMFormat.SHAPE.getMimeType():
                    shoot_snippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        SHAPEFILE_INTERFACE)
                    return shapefile_object_formatter(handler_result)
                if accept_hdr == LMFormat.CSV.getMimeType():
                    shoot_snippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        CSV_INTERFACE)
                    return csv_object_formatter(handler_result)
                if accept_hdr == LMFormat.NEWICK.getMimeType():
                    raise cherrypy.HTTPError(
                        HTTPStatus.BAD_REQUEST,
                        'Newick response not enabled yet')
                    # TODO: Use dendropy to convert nexus to newick
                    # return file_formatter(handler_result.getDLocation())
                if accept_hdr == LMFormat.NEXUS.getMimeType():
                    return file_formatter(handler_result.getDLocation())
                if accept_hdr == LMFormat.ZIP.getMimeType():
                    csvs = True
                    sdms = True
                    return gridset_package_formatter(
                        handler_result, includeCSV=csvs, includeSDM=sdms)
                if accept_hdr == LMFormat.PROGRESS.getMimeType():
                    obj_type, obj_id, detail = handler_result
                    return progress_object_formatter(
                        obj_type, obj_id, detail=detail)
            except Exception as e:
                # Ignore and try next accept header
                raise cherrypy.HTTPError(
                    HTTPStatus.NOT_ACCEPTABLE,
                    'Failed: {}'.format(str(e)))
        # If we cannot find an acceptable formatter, raise HTTP error
        raise cherrypy.HTTPError(
            HTTPStatus.NOT_ACCEPTABLE, 'Could not find an acceptable format')

        # return json_object_formatter(handler_result)

    return wrapper


# .............................................................................
def shoot_snippets(obj, operation, format_string):
    """Attempt to shoot snippets for downloads / viewings / etc
    """
    # Only shoot public data snippets
    try:
        if obj.getUserId() == PUBLIC_USER:
            shooter = SnippetShooter()
            shooter.addSnippets(
                obj, operation, url='{}/{}'.format(
                    obj.metadataUrl, format_string),
                who='user', agent='webService', why='request')
            shooter.shootSnippets()
    except Exception:
        # TODO: Log exceptions for snippets
        pass
