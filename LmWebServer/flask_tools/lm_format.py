"""This tool provides output formatting for service calls based on headers

This module provides a tool for formatting outputs of service calls based on
the accept headers of the request
"""
from flask import request
import werkzeug.exceptions as WEXC

from LmCommon.common.lmconstants import (CSV_INTERFACE, ENCODING, JSON_INTERFACE, LMFormat, SHAPEFILE_INTERFACE)

from LmServer.common.lmconstants import SnippetOperations
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.snippet import SnippetShooter

from LmWebServer.formatters.eml_formatter import eml_object_formatter
from LmWebServer.formatters.file_formatter import (
    csv_object_formatter, file_formatter, gtiff_object_formatter, shapefile_object_formatter)
from LmWebServer.formatters.geo_json_formatter import geo_json_object_formatter
from LmWebServer.formatters.json_formatter import json_object_formatter
from LmWebServer.formatters.kml_formatter import kml_object_formatter
from LmWebServer.formatters.package_formatter import gridset_package_formatter
from LmWebServer.formatters.progress_formatter import progress_object_formatter


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
            raise WEXC.BadRequest

        accept_headers = request.headers['Accept']

        try:
            raw_headers = accept_headers.split(',')
            valued_accepts = []
            for hdr in raw_headers:
                if len(hdr.split(';')) > 1:
                    mime, val = hdr.split(';')
                    valued_accepts.append(
                        (mime.strip(), float(val.strip('q='))))
                else:
                    valued_accepts.append((hdr.strip(), 1.0))
        except Exception:
            valued_accepts = [('*/*', 1.0)]

        sorted_accepts = sorted(
            valued_accepts, key=lambda x: x[1], reverse=True)

        for accept_hdr, _ in sorted_accepts:
            try:
                if accept_hdr == LMFormat.GEO_JSON.get_mime_type():
                    return geo_json_object_formatter(
                        handler_result).encode(ENCODING)
                # If JSON or default
                if accept_hdr in [LMFormat.JSON.get_mime_type(), '*/*']:
                    shoot_snippets(
                        handler_result, SnippetOperations.VIEWED,
                        JSON_INTERFACE)
                    return json_object_formatter(
                        handler_result).encode(ENCODING)
                if accept_hdr == LMFormat.EML.get_mime_type():
                    return eml_object_formatter(
                        handler_result)
                if accept_hdr == LMFormat.KML.get_mime_type():
                    return kml_object_formatter(
                        handler_result)
                if accept_hdr == LMFormat.GTIFF.get_mime_type():
                    return gtiff_object_formatter(
                        handler_result)
                if accept_hdr == LMFormat.SHAPE.get_mime_type():
                    shoot_snippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        SHAPEFILE_INTERFACE)
                    return shapefile_object_formatter(
                        handler_result)
                if accept_hdr == LMFormat.CSV.get_mime_type():
                    shoot_snippets(
                        handler_result, SnippetOperations.DOWNLOADED,
                        CSV_INTERFACE)
                    return csv_object_formatter(
                        handler_result).encode(ENCODING)
                if accept_hdr == LMFormat.NEWICK.get_mime_type():
                    raise WEXC.BadRequest('Newick response not enabled yet')
                    # TODO: Use dendropy to convert nexus to newick
                    # return file_formatter(handler_result.get_dlocation())
                if accept_hdr == LMFormat.NEXUS.get_mime_type():
                    return file_formatter(
                        handler_result.get_dlocation()).encode(ENCODING)
                if accept_hdr == LMFormat.ZIP.get_mime_type():
                    csvs = True
                    sdms = True
                    return gridset_package_formatter(
                        handler_result, include_csv=csvs, include_sdm=sdms
                        )
                if accept_hdr == LMFormat.PROGRESS.get_mime_type():
                    obj_type, obj_id, detail = handler_result
                    return progress_object_formatter(
                        obj_type, obj_id, detail=detail).encode(ENCODING)
            except Exception as e:
                # Ignore and try next accept header
                raise WEXC.NotAcceptable('Failed: {}'.format(str(e)))
        # If we cannot find an acceptable formatter, raise HTTP error
        raise WEXC.NotAcceptable('Could not find an acceptable format')

        # return json_object_formatter(handler_result)

    return wrapper


# .............................................................................
def shoot_snippets(obj, operation, format_string):
    """Attempt to shoot snippets for downloads / viewings / etc
    """
    # Only shoot public data snippets
    try:
        if obj.get_user_id() == PUBLIC_USER:
            shooter = SnippetShooter()
            shooter.add_snippets(
                obj, operation, url='{}/{}'.format(
                    obj.metadata_url, format_string),
                who='user', agent='webService', why='request')
            shooter.shoot_snippets()
    except Exception:
        # TODO: Log exceptions for snippets
        pass
